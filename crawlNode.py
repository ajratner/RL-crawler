import sys
import urlparse
import datetime
import time
from util import *
from urlFontier import urlFrontier
import re
import pycurl
import cStringIO
import threading



# NOTE NOTE --> Overall to-do list
# - initial seeding function
# - payload transfer (to db first then in batch to central dispatch node?)
# - testing function that takes sample of all thread page crawls to form picture of time / 
#   efficiency, without requiring every single one to print out



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
      
      # NOTE: TO-DO --> run testing, try to think of further catches
      else:
        
        # NOTE: TO-DO --> log to some sort of error file
        rup = urlparse.urlsplit(ref_url)
        urls.append(rup.scheme + '://' + rup.netloc + '/' + url)
  return urls


# basic routine for crawling a single page from url Frontier, extracting links, logging/adding
# back to frontier
def crawl_page(uf):
  
  # get page from urlFrontier
  addr, url, next_pull_time = uf.get()

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
  time.sleep(wait_time.seconds + 1)

  # pull page from web and record pull time
  with Timer() as t:
    c.perform()
  
  # Check for page transfer success (not connection/transfer timeouts are handled by opts)
  if c.getinfo(c.HTTP_CODE) < 400:

    # parse page for links
    html = basic_html_clean(buf.getvalue())
    for link_url in extract_links(html, url):
      uf.add(link_url)

    # log page pull as succesful
    # NOTE: log AFTER adds; this should prevent join() from finishing before links added 
    uf.log(addr, True, t.duration)

    # store page for payload transfer
    #NOTE: TO-DO!

  else:
    uf.log(addr, False)


# crawl thread class
class CrawlThread(threading.Thread):
  def __init__(self, uf)
    threading.Thread.__init__(self)
    self.uf = uf

  def run(self):
    while True:
      crawl_page(self.uf)


# main multi-thread crawl routine
def multithread_crawl(n_threads, initial_url_list):
  
  # instantiate one urlFontier object- containing a Queue- for all threads
  uf = urlFrontier(NODE_NUMBER, NUMBER_OF_NODES)

  # spawn a pool of daemon threads
  for i in range(n_threads):
    t = CrawlThread(uf)
    t.setDaemon(True)
    t.start()

  # initialize the urlFrontier
  for url in initial_url_list:
    uf.add(url)

  # wait on the main queue until everything processed
  uf.backq_heap.join()


