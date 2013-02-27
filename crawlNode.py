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
from pageAnalyze import get_page_features
from pageAnalyze import analyze_page


# global constants
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17"
CURLOPT_TIMEOUT = 60
NODE_NUMBER = 0
NUMBER_OF_NODES = 1


# basic cleaning fn for incoming html string
def basic_html_clean(html_string):
  return re.sub(r'[\x7f-\xff]', '', html_string)


# extracted link resolution
def resolve_extracted_link(link, ref_url):

  # following RFC 1808 <scheme>://<net_loc>/<path>;<params>?<query>#<fragment>

  # first look for '//'- if present urlparse will handle properly
  if re.search(r'//', link) is not None:
    return link

  # look for relative path '/'
  elif re.search(r'^/', link) is not None:
    rup = urlparse.urlsplit(ref_url)
    return rup.scheme + '://' + rup.netloc + link

  # look for clear netloc form- 'xxx.xxx.xxx'
  elif re.search(r'^\w+\.\w+.\w+', link) is not None:
    return '//' + link

  # look for possible netloc form + path - 'xxx.xxx/yyy'
  elif re.search(r'^\w+\.\w+/', link) is not None:
    return '//' + link

  # look for known throw-away forms
  elif re.search(r'^mailto:', link) is not None:
    return None
  
  # NOTE: TO-DO --> run testing, try to think of further catches
  else:
    
    # NOTE: TO-DO --> log to some sort of error file
    rup = urlparse.urlsplit(ref_url)
    return rup.scheme + '://' + rup.netloc + '/' + link


# link extractor subfunction
def extract_link_data(html, ref_url):
  urls = []
  url_data = []
  
  # look for all a tags
  link_tags = re.findall(r'<a(\s[^>]+)?>(.*?)</a>', html)
  for link_tag in link_tags:
    link = re.search(r'<a [^>]*href="([^"]+)"[^>]*>(.*?)</a>', link_tag)
    if link is not None:

      # try to resolve link
      link_url = resolve_extracted_link(link.group(1), ref_url)
      if link_url is not None:
        urls.append(link_url)
        url_data.append((link.group(2)))
  
  return urls, url_data


# basic routine for crawling a single page from url Frontier, extracting links, logging/adding
# back to frontier
def crawl_page(uf, aq, thread_name='Thread-?'):
  
  # get page from urlFrontier
  next_pull_time, host_addr, url, ref_page_stats = uf.get_crawl_task()

  # FOR TESTING
  print '\n%s got %s from queue' % (thread_name, url)

  # construct full addr-based url
  url_parts = list(urlparse.urlsplit(url))
  root_url = url_parts[1]
  url_parts[1] = host_addr
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

    # parse page for links & associated data
    html = basic_html_clean(buf.getvalue())
    extracted_urls, extracted_url_data = extract_link_data(html, url)

    # NOTE: to-do: extract minimal set of page features needed to package with child links

    # NOTE: to-do: MINIMAL FIRST-LAYER THRESHOLD CALC/DECISION?

    # NOTE: to-do: drop payload to out Q, to go to db and then to analyse node
    
    # extract page features & analyze for keep / discard decision
    #page_stats, page_features = get_page_features(html, ref_page_stats)
    #extracted_url_packages = zip(extracted_urls, [page_stats + x for x in extracted_url_data])
    
    # add task to analysis queue
    #aq.put((html, page_features))

    # log page pull as successful & submit extracted urls to url frontier
    uf.log_and_add_extracted(host_addr, True, t.duration, extracted_url_pkgs)

  else:
    uf.log(addr, False)


# crawl thread class
class CrawlThread(threading.Thread):
  def __init__(self, uf, aq):
    threading.Thread.__init__(self)
    self.uf = uf
    self.aq = aq

  def run(self):
    while True:
      try:
        crawl_page(self.uf, self.aq, self.getName())

      # in case of exception, log task done to crawl task & active count queues to un-block
      except Exception as e:
        print e
        self.uf.Q_crawl_tasks.task_done()
        self.uf.Q_active_count.task_done()


# maintenance thread class
class MaintenanceThread(threading.Thread):
  def __init__(self, uf):
    threading.Thread.__init__(self)
    self.uf = uf

  def run(self):
    while True:
      try:
        self.uf.clean_and_fill()

      # in case of exception, log tasks done to overflow & cleanup queues to un-block
      except Exception as e:
        print e
        self.uf.Q_overflow_urls.task_done()
        self.uf.Q_hq_cleanup.task_done()


# analysis thread class
#class AnalysisThread(threading.Thread):
#  def __init__(self, aq, w_0):
#    threading.Thread.__init__(self)
#    self.aq = aq
#    self.w = w0
#
#  def run(self):
#    while True:
#      html, page_features = self.aq.get()
#      confidence = analyze_page(html, page_features, self.w)
#
#      # NOTE: TO-DO: DECIDE WHETHER OR NOT TO DROP PAYLOAD
#
#      # NOTE: TO-DO: RECEIVE FEEDBACK & TRAIN MODEL (???)
#
#      # NOTE: TO-DO: log task done!



# main multi-thread crawl routine
def multithread_crawl(n_threads, n_mthreads, initial_url_list):
  
  # instantiate one urlFontier object for all threads
  uf = urlFrontier(NODE_NUMBER, NUMBER_OF_NODES, n_threads)

  # initialize the urlFrontier
  uf.initialize(initial_url_list)

  # instantiate an analysis Queue for collecting payload analysis jobs
  #aq = Queue.Queue()

  # spawn a pool of daemon CrawlThread threads
  for i in range(n_threads):
    t = CrawlThread(uf, aq)
    t.setDaemon(True)
    t.start()

  # spawn a pool of daemon MaintenanceThread threads
  for i in range(n_mthreads):
    t = MaintenanceThread(uf)
    t.setDaemon(True)
    t.start()

  # wait on the active count queue until every extracted or provided url is crawled
  uf.Q_active_count.join()


#
# --> UNIT TESTS
#


TEST_INITIAL_URLS = ['http://www.crecomparex.com', 'http://www.timberintelligence.com', 'http://www.ndacomptool.net', 'http://www.crecomprex.com', 'http://www.crecomparex.com/blah.html']

#
# --> Command line functionality
#
if __name__ == '__main__':
  if sys.argv[1] == 'test' and len(sys.argv) == 2:
    print '\nTesting on %s NODE(S) with 2 C THREADS & 2 M THREADS:\n' % (NUMBER_OF_NODES)
    multithread_crawl(2, 2, TEST_INITIAL_URLS)
  else:
    print 'Usage: python crawlNode.py ...'
    print '(1) test'
