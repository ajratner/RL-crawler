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
from pageAnalyze import *


# global constants
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17"
CURLOPT_TIMEOUT = 60
NODE_NUMBER = 0
NUMBER_OF_NODES = 1
DB_VARS = ('localhost', 'root', 'penguin25', 'crawler_test')
DB_PAYLOAD_TABLE = 'payload_table'
LOG_REL_PATH = 'logs/log'
DEBUG_MODE = True


# basic routine for crawling a single page from url Frontier, extracting links, logging/adding
# back to frontier
def crawl_page(uf, Q_payload, Q_logs, thread_name='Thread-?'):
  
  # get page from urlFrontier
  next_pull_time, host_addr, url, parent_page_stats = uf.get_crawl_task()

  if DEBUG_MODE:
    Q_logs.put('%s: got %s from queue' % (thread_name, url))

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

  if DEBUG_MODE:
    Q_logs.put('%s: pulling page %s at %s' % (thread_name, url, datetime.datetime.now()))

  # pull page from web and record pull time
  with Timer() as t:
    c.perform()
  
  # Check for page transfer success (not connection/transfer timeouts are handled by opts)
  if c.getinfo(c.HTTP_CODE) < 400:

    # parse page for links & associated data
    html = basic_html_clean(buf.getvalue())
    extracted_urls, link_stats = extract_link_data(html, url, Q_logs)
    
    # parse page for (A) stats that need to be passed on with child links, (B) page features
    page_stats, page_features = analyze_page(html, parent_page_stats, Q_logs)

    # NOTE: to-do: MINIMAL FIRST-LAYER THRESHOLD CALC/DECISION?

    # add page, url + features list to queue out (-> database / analysis nodes)
    row_dict = {
      'url': url,
      'features': flist_to_string(page_features),
      'html': html
    }
    Q_payload.Q_out.put(row_dict)

    # package all data that needs to be passed on with child links
    extracted_url_pkgs = zip(extracted_urls, [page_stats + ls for ls in link_stats])

    # log page pull as successful & submit extracted urls + data to url frontier
    uf.log_and_add_extracted(host_addr, True, t.duration, extracted_url_pkgs)

  else:
    uf.log_and_add_extracted(host_addr, False)
    uf.Q_active_count.task_done()
    Q_logs.put('%s: Got HTTP code %s from %s at %s', (thread_name, c.HTTP_CODE, url, datetime.datetime.now()))



# crawl thread class
class CrawlThread(threading.Thread):
  def __init__(self, uf, Q_payload, Q_logs):
    threading.Thread.__init__(self)
    self.uf = uf
    self.Q_payload = Q_payload
    self.Q_logs = Q_logs

  def run(self):
    while True:
      crawl_page(self.uf, self.Q_payload, self.Q_logs, self.getName())


# maintenance thread class
class MaintenanceThread(threading.Thread):
  def __init__(self, uf):
    threading.Thread.__init__(self)
    self.uf = uf

  def run(self):
    while True:
      self.uf.clean_and_fill()


# main multi-thread crawl routine
def multithread_crawl(n_threads, n_mthreads, initial_url_list):
  
  # instantiate a queue-out-to-logs handler
  Q_logs = Q_out_to_file(LOG_REL_PATH)
  Q_logs.put("\n\nSession Start at %s" % (datetime.datetime.now(),))

  # instantiate one urlFontier object for all threads
  uf = urlFrontier(NODE_NUMBER, NUMBER_OF_NODES, n_threads, Q_logs)

  # initialize the urlFrontier
  uf.initialize(initial_url_list)

  # instantiate a queue-out-to-db handler
  Q_payload = Q_out_to_db(DB_VARS, DB_PAYLOAD_TABLE, uf.Q_active_counti, Q_logs)

  # spawn a pool of daemon CrawlThread threads
  for i in range(n_threads):
    t = CrawlThread(uf, Q_payload, Q_logs)
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
