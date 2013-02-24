import sys
import urlparse
import datetime
import time
from util import *
from urlFrontier import urlFrontier
import re
import pycurl
import cStringIO
import threading


# global constants
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17"
CURLOPT_TIMEOUT = 60
NODE_NUMBER = 0
NUMBER_OF_NODES = 1


# basic cleaning fn for incoming html string
def basic_html_clean(html_string):
  return re.sub(r'[\x7f-\xff]', '', html_string)


# link extractor subfunction
def extract_links(html, ref_url):
  urls = []
  
  # look for all a tags
  link_tags = re.findall(r'<a(\s[^>]+)?>', html)
  for link_tag in link_tags:
    link = re.search(r'href="([^"]+)"', link_tag)
    if link is not None:
      url = link.group(1)

      # following RFC 1808 <scheme>://<net_loc>/<path>;<params>?<query>#<fragment>

      # first look for '//'- if present urlparse will handle properly
      if re.search(r'//', url) is not None:
        urls.append(url)

      # look for relative path '/'
      elif re.search(r'^/', url) is not None:
        rup = urlparse.urlsplit(ref_url)
        urls.append(rup.scheme + '://' + rup.netloc + url)

      # look for clear netloc form- 'xxx.xxx.xxx'
      elif re.search(r'^\w+\.\w+.\w+', url) is not None:
        urls.append('//' + url)

      # look for possible netloc form + path - 'xxx.xxx/yyy'
      elif re.search(r'^\w+\.\w+/', url) is not None:
        urls.append('//' + url)

      # look for known throw-away forms
      elif re.search(r'^mailto:', url) is not None:
        pass
      
      # NOTE: TO-DO --> run testing, try to think of further catches
      else:
        
        # NOTE: TO-DO --> log to some sort of error file
        rup = urlparse.urlsplit(ref_url)
        urls.append(rup.scheme + '://' + rup.netloc + '/' + url)
  return urls


# basic routine for crawling a single page from url Frontier, extracting links, logging/adding
# back to frontier
def crawl_page(uf, thread_name='Thread-?'):
  
  # get page from urlFrontier
  addr, url, next_pull_time = uf.get()

  print '\n%s got %s from queue' % (thread_name, url)

  # construct full addr-based url
  url_parts = list(urlparse.urlsplit(url))
  root_url = url_parts[1]
  url_parts[1] = addr
  url_addr = urlparse.urlunsplit(url_parts)

  # pull page with pyCurl
  buf = cStringIO.StringIO()
  c = pycurl.Curl()

  # set pycurl opts
  c.setopt(c.USERAGENT, USER_AGENT)
  c.setopt(c.URL, url_addr)
  c.setopt(c.HTTPHEADER, ["Host: " + root_url])
  c.setopt(c.FOLLOWLOCATION, 1)
  c.setopt(c.MAXREDIRS, 5)
  c.setopt(c.TIMEOUT, CURLOPT_TIMEOUT)
  c.setopt(c.WRITEFUNCTION, buf.write)
  
  # delay until >= next_pull_time
  wait_time = next_pull_time - datetime.datetime.now()
  time.sleep(max(0, wait_time.total_seconds()))

  print '\n%s pulling page %s at %s' % (thread_name, url, datetime.datetime.now())

  # pull page from web and record pull time
  with Timer() as t:
    c.perform()
  
  # Check for page transfer success (not connection/transfer timeouts are handled by opts)
  if c.getinfo(c.HTTP_CODE) < 400:

    # log page pull as succesful
    uf.log(addr, True, t.duration)

    # parse page for links
    html = basic_html_clean(buf.getvalue())
    for link_url in extract_links(html, url):
      uf.add(link_url)
      print '%s adding extracted link %s' % (thread_name, link_url)

    # store page for payload transfer
    #NOTE: TO-DO!

  else:
    uf.log(addr, False)


# crawl thread class
class CrawlThread(threading.Thread):
  def __init__(self, uf):
    threading.Thread.__init__(self)
    self.uf = uf

  def run(self):
    while True:
      try:
        crawl_page(self.uf, self.getName())
      except Exception as e:
        print e

      # if crawl_page gives an exception, still report task done to Queue so join can release
      self.uf.backq_heap.task_done()
        


# main multi-thread crawl routine
def multithread_crawl(n_threads, initial_url_list):
  
  # instantiate one urlFontier object- containing a Queue- for all threads
  uf = urlFrontier(NODE_NUMBER, NUMBER_OF_NODES, n_threads)

  # initialize the urlFrontier
  for url in initial_url_list:
    uf.add(url)

  # spawn a pool of daemon threads
  for i in range(n_threads):
    t = CrawlThread(uf)
    t.setDaemon(True)
    t.start()

  # wait on the main queue until everything processed
  uf.backq_heap.join()


#
# --> UNIT TESTS
#


TEST_INITIAL_URLS = ['http://www.crecomparex.com', 'http://www.timberintelligence.com', 'http://www.ndacomptool.net', 'http://www.crecomprex.com', 'http://www.crecomparex.com/blah.html']

#
# --> Command line functionality
#
if __name__ == '__main__':
  if sys.argv[1] == 'test' and len(sys.argv) == 2:
    print '\nTesting on %s NODE(S) with 2 THREADS:\n' % (NUMBER_OF_NODES)
    multithread_crawl(2, TEST_INITIAL_URLS)
  else:
    print 'Usage: python crawlNode.py ...'
    print '(1) test'
