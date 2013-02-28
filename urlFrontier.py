import sys
import urlparse
import socket
import heapq
import hashlib
import datetime
import random
from util import *
import Queue
import re
from pybloomfilter import BloomFilter


# global time constants
DNS_REFRESH_TIME = 21600  # Refresh DNS every 6 hours
BASE_PULL_DELAY = 60  # Base time constant to wait for pulling from domain = 60 secs


# Bloom filter (for seen url lookup) constants
BF_CAPACITY = 10000000
BF_ERROR_RATE = 0.001
BF_FILENAME = 'seen.bloom'


# backq maintenance constants
HQ_TO_THREAD_RATIO = 3
MAX_QUEUE_SIZE = 10000

DEBUG_MODE = True


# url frontier object at a node #[nodeN] of [numNodes]
#
# Primary external routines:
#
# - For CrawlThread:
#   *  get_crawl_task()
#   *  log_and_add_extracted(host_addr, success, time_taken, urls)
#
# - For MaintenanceThread:
#   *  clean_and_fill()
#
# - For initialization (sole) thread:
#   *  initialize(urls)

class urlFrontier:
  
  def __init__(self, node_n, num_nodes, num_threads, Q_logs):
    self.node_n = node_n
    self.num_nodes = num_nodes
    self.num_threads = num_threads
    self.Q_logs = Q_logs
    
    # crawl task Queue
    # Priority Queue ~ [ (next_pull_time, host_addr, url, ref_page_stats) ]
    self.Q_crawl_tasks = Queue.PriorityQueue()

    # host queue dict
    # { host_addr: [(url, ref_page_stats), ...] }
    self.hqs = {}
    
    # seen url check
    # Bloom Filter ~ [ url ]
    self.seen = BloomFilter(BF_CAPACITY, BF_ERROR_RATE, BF_FILENAME)

    # DNS Cache
    # { netloc: (host_addr, time_last_checked) }
    self.DNScache = {}

    # overflow url Queue
    # Queue ~ [ (host_addr, url, ref_page_stats) ]
    self.Q_overflow_urls = Queue.Queue()

    # host queue cleanup Queue
    # Priority Queue ~ [ (time_to_delete, host_addr) ]
    self.Q_hq_cleanup = Queue.PriorityQueue()

    # active url count queue
    # Queue ~ [ True ]
    self.Q_active_count = Queue.Queue()
  

  # primary routine for getting a crawl task from queue
  def get_crawl_task(self):
    return self.Q_crawl_tasks.get()
  

  # primary routine to log crawl task done & submit extracted urls
  def log_and_add_extracted(self, host_addr, success, time_taken=0, url_pkgs=[]):

    # add urls to either hq of host_addr or else overflow queue
    for url_pkg in url_pkgs:
      self._add_extracted_url(host_addr, url_pkg)

    # handle failure of page pull
    # NOTE: TO-DO!
    if not success:
      pass

    # calculate time delay based on success
    now = datetime.datetime.now()
    r = random.random()
    td = 10*time_to_pull + r*BASE_PULL_DELAY if success else (1 + r)*BASE_PULL_DELAY
    next_time = now + datetime.timedelta(0, td)

    # if the hq of host_addr is not empty, enter new task in crawl task queue
    if len(self.hqs[host_addr]) > 0:

      # add task to crawl task queue
      url, ref_page_stats = self.hqs[host_addr].pop()
      self.Q_crawl_tasks.put((next_time, host_addr, url, ref_page_stats))

    # else if empty, add task to cleanup queue
    else:
      self.Q_hq_cleanup.put((next_time, host_addr))
    
    # report crawl task done to queue, HOWEVER do not submit as done till payload dropped
    self.Q_crawl_tasks.task_done()


  # subroutine to add a url extracted from a host_addr
  def _add_extracted_url(self, ref_host_addr, url_pkg):
    url_in, ref_page_stats = url_pkg
  
    # basic cleaning operations on url
    # NOTE: it is the responsibility of the crawlNode.py extract_links fn to server proper url
    url = re.sub(r'/$', '', url_in)

    # check if url already seen
    if url not in self.seen:

      # get host IP address of url
      url_parts = urlparse.urlsplit(url)
      host_addr = self._get_and_log_addr(url_parts.netloc)

      # if this is an internal link, send directly to the serving hq
      # NOTE: need to check that equality operator is sufficient here!
      if host_addr == ref_host_addr:
        self.hqs[host_addr].append((url, ref_page_stats))

        # !log as seen & add to active count
        self.seen.add(url)
        self.Q_active_count.put(True)
        
        if DEBUG_MODE:
          self.Q_logs.put("Active count: %s" % self.Q_active_count.qsize())
      
      else:
        
        # check if this address belongs to this node
        url_node = hash(host_addr) % self.num_nodes
        if url_node == self.node_n:

          # add to overflow queue
          self.Q_overflow_urls.put((host_addr, url, ref_page_stats))

          # !log as seen & add to active count
          self.seen.add(url)
          self.Q_active_count.put(True)

          if DEBUG_MODE:
            self.Q_logs.put("Active count: %s" % self.Q_active_count.qsize())
        
        # else pass along to appropriate node
        # NOTE: TO-DO!
        else:
          pass


  # subfunction for getting IP address either from DNS cache or web
  def _get_and_log_addr(self, hostname):
    
    # try looking up hostname in DNScache
    now = datetime.datetime.now()
    if self.DNScache.has_key(hostname):

      # check time for DNS refresh
      addr, created = self.DNScache[hostname]
      age = now - created
      if age.seconds > DNS_REFRESH_TIME:
        addr = self._get_addr(hostname)
        if addr is not None:
          self.DNScache[hostname] = (addr, now)
        else:
          del self.DNScache[hostname]
    else:
      addr = self._get_addr(hostname)
      if addr is not None:
        self.DNScache[hostname] = (addr, now)
    return addr
  

  # sub-subfunction for getting IP address from socket
  def _get_addr(self, hostname):
    try:
      addr_info = socket.getaddrinfo(hostname, None)
    except Exception as e:
      self.Q_logs.put('DNS Error accessing ' + hostname)
      return None

    # ensure result is non-null
    if len(addr_info) > 0:
      return addr_info[0][4][0]
    else:
      self.Q_logs.put('DNS Error, null returned for ' + hostname)
      return None


  # primary maintenance routine- clear one old queue and replace with a new one from overflow
  # NOTE: this assumes constant number of existing queues is always present
  def clean_and_fill(self):
    
    # get queue to delete & time to delete at
    time_to_delete, host_addr = self.Q_hq_cleanup.get()

    # wait to delete
    wait_time = time_to_delete - datetime.datetime.now()
    time.sleep(max(0, wait_time.total_seconds()))

    # delete queue and add new one
    del self.hqs[host_addr]
    added = False
    while not added:
      added = self._overflow_to_new_hq()

    # log task done to both queues
    self.Q_hq_cleanup.task_done()
    self.Q_overflow_urls.task_done()


  # subroutine for transferring urls from overflow queue to new hq
  def _overflow_to_new_hq(self):
    host_addr, url, ref_page_stats = self.Q_overflow_urls.get()
    
    # if hq already exists, recycle- insertion not thread safe
    # NOTE: better way to do this while ensuring thread safety here?
    if self.hqs.has_key(host_addr):
      self.Q_overflow_urls.task_done()
      self.Q_overflow_urls.put((host_addr, url, ref_page_stats))
      return False
    else:
      
      # create new empty hq and send seed url to crawl task queue
      self.hqs[host_addr] = []
      self.Q_crawl_tasks.put((datetime.datetime.now(), host_addr, url, ref_page_stats))
      return True
  

  # primary routine for initialization of url frontier / hqs
  # NOTE: !!! Assumed that this is sole thread running when executed, prior to crawl start
  def initialize(self, urls=[]):
    now = datetime.datetime.now()
    
    # initialize all hqs as either full & tasked or empty & to be deleted
    i = 0
    while len(self.hqs) < HQ_TO_THREAD_RATIO*self.num_threads:
      i += 1
      
      # expend all given urls
      if len(urls) > 0:
        self._init_add_url(urls.pop())

      # else add empty queues and mark to be cleared & replaced
      else:
        self.hqs[i] = []
        self.Q_hq_cleanup.put((now, i))

    # if there are urls left over, add to appropriate queues
    for url in urls:
      self._init_add_url(url)

  
  # subroutine for adding url to hq, assuming only one thread running (initialization)
  def _init_add_url(self, url_in):

    # basic cleaning operations on url
    url = re.sub(r'/$', '', url_in)

    # check if seen
    if url not in self.seen:

      # get host IP address of url
      url_parts = urlparse.urlsplit(url)
      host_addr = self._get_and_log_addr(url_parts.netloc)

      # check if this address belongs to this node
      url_node = hash(host_addr) % self.num_nodes
      if url_node == self.node_n:

        # !log as seen & add to active count
        self.seen.add(url)
        self.Q_active_count.put(True)

        if DEBUG_MODE:
          self.Q_logs.put("Active count: %s" % self.Q_active_count.qsize())

        # add to an existing hq, or create new one & log new crawl task
        if self.hqs.has_key(host_addr):
          self.hqs[host_addr].append((url, None))
        else:
          self.hqs[host_addr] = []
          self.Q_crawl_tasks.put((datetime.datetime.now(), host_addr, url, None))

      # else pass along to appropriate node
      # NOTE: TO-DO!
      else:
        pass
        

#
# --> Command line functionality
#
#if __name__ == '__main__':
#  if sys.argv[1] == 'test' and len(sys.argv) == 2:
#    full_test()
#  else:
#    print 'Usage: python urlFrontier.py ...'
#    print '(1) test'
