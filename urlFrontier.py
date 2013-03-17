#!/usr/bin/env python

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
from node_globals import *
from node_locals import *


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
  
  def __init__(self, node_n, seen_persist, Q_message_sender=None, Q_logs=None):
    self.node_n = node_n
    self.Q_message_sender = Q_message_sender
    self.Q_logs = Q_logs
    self.total_crawled = 0
    self.payloads_dropped = 0
    
    # crawl task Queue
    # Priority Queue ~ [ (next_pull_time, host_addr, url, parent_page_stats, seed_dist, parent_url) ]
    self.Q_crawl_tasks = Queue.PriorityQueue()

    # host queue dict
    # { host_addr: [(url, ref_page_stats, seed_dist, parent_url), ...] }
    self.hqs = {}
    
    # seen url check
    # Bloom Filter ~ [ url ]
    if seen_persist:
      try:
        self.seen = BloomFilter.open(BF_FILENAME)
      except:
        self.Q_logs.put('Error opening bloom filter, creating new one')
        self.seen = BloomFilter(BF_CAPACITY, BF_ERROR_RATE, BF_FILENAME)
    else:
      self.seen = BloomFilter(BF_CAPACITY, BF_ERROR_RATE, BF_FILENAME)

    # DNS Cache
    # { netloc: (host_addr, time_last_checked) }
    self.DNScache = {}

    # overflow url Queue
    # Queue ~ [ (host_addr, url, ref_page_stats, seen_dist, parent_url) ]
    self.Q_overflow_urls = Queue.Queue()

    # host queue cleanup Queue
    # Priority Queue ~ [ (time_to_delete, host_addr) ]
    self.Q_hq_cleanup = Queue.PriorityQueue()

    # active url count queue- for counting/tracking active
    # Queue ~ [ True ]
    self.Q_active_count = Queue.Queue()

    # thread active url dict- a dict of active urls by thread using, for restart dump
    # { thread_name: active_url }
    # NOTE: note that there are problems with this methodology, but that errors will only lead
    # to data redundancy (as opposed to omission)...
    self.thread_active = {}


  # primary routine for getting a crawl task from queue
  def get_crawl_task(self):
    return self.Q_crawl_tasks.get()
  

  # primary routine to log crawl task done & submit extracted urls
  def log_and_add_extracted(self, host_addr, host_seed_dist, success, time_taken=0,url_pkgs=[]):

    # handle failure of page pull
    # NOTE: TO-DO!
    if not success:
      pass

    # add urls to either hq of host_addr or else overflow queue
    for url_pkg in url_pkgs:
      self._add_extracted_url(host_addr, host_seed_dist, url_pkg)

    # calculate time delay based on success
    now = datetime.datetime.now()
    r = random.random()
    td = 10*time_taken + r*BASE_PULL_DELAY if success else (0.5 + r)*BASE_PULL_DELAY
    next_time = now + datetime.timedelta(0, td)

    # if the hq of host_addr is not empty, enter new task in crawl task queue
    if len(self.hqs[host_addr]) > 0:

      # add task to crawl task queue
      r = self.hqs[host_addr].pop()
      self.Q_crawl_tasks.put((next_time, host_addr) + r)

    # else if empty, add task to cleanup queue
    else:
      self.Q_hq_cleanup.put((next_time, host_addr))
    
    # report crawl task done to queue, HOWEVER do not submit as done till payload dropped
    self.Q_crawl_tasks.task_done()


  # subroutine to add a url extracted from a host_addr
  def _add_extracted_url(self, ref_host_addr, ref_seed_dist, url_pkg, from_other_node=False):
    url_in, ref_page_stats, parent_url = url_pkg

    # basic cleaning operations on url
    # NOTE: it is the responsibility of the crawlNode.py extract_links fn to server proper url
    url = re.sub(r'/$', '', url_in)

    # if url already seen do not proceed, else log as seen
    if url in self.seen:
      return False
    else:
      self.seen.add(url)

    # get host IP address of url
    url_parts = urlparse.urlsplit(url)
    host_addr = self._get_and_log_addr(url_parts.netloc)

    # if the page is not of a safe type log and do not proceed
    if re.search(SAFE_PATH_RGX, url_parts.path) is None:
      self.Q_logs.put("*UN-SAFE PAGE TYPE SKIPPED: %s" % (url,))
      return False
    
    # if DNS was resolved error already reported, do not proceed any further
    if host_addr is None:
      return False

    # calculate url's seed distance
    if not from_other_node:
      seed_dist = ref_seed_dist if host_addr == ref_host_addr else ref_seed_dist + 1
    else:
      seed_dist = ref_seed_dist

    # check for being past max seed distance
    if seed_dist > MAX_SEED_DIST and MAX_SEED_DIST > -1:
      return False

    # if the page belongs to another node, pass to message sending service
    if not from_other_node:
      if DISTR_ON_FULL_URL:
        url_node = hash(url) % NUMBER_OF_NODES
      else:
        url_node = hash(host_addr) % NUMBER_OF_NODES
      if url_node != self.node_n:
        self.Q_message_sender.send((url_node, url, ref_page_stats, seed_dist, parent_url))
        return False

    # if this is an internal link, and not from other node, send directly to the serving hq
    if seed_dist == ref_seed_dist and not from_other_node:
      self.hqs[host_addr].append((url, ref_page_stats, seed_dist, parent_url))

      # add to active count & update total count
      self.Q_active_count.put(True)
      self.total_crawled += 1
      if DEBUG_MODE:
        self.Q_logs.put("Active count: %s" % self.Q_active_count.qsize())
    
    # else send to overflow_urls to stay cautiously thread safe
    else:
      
      # add to overflow queue
      self.Q_overflow_urls.put((host_addr, url, ref_page_stats, seed_dist, parent_url))

      # add to active count
      self.Q_active_count.put(True)
      self.total_crawled += 1
      if DEBUG_MODE:
        self.Q_logs.put("Active count: %s" % self.Q_active_count.qsize())


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
      self.Q_logs.put('DNS ERROR: skipping ' + hostname)
      return None

    # ensure result is non-null
    if len(addr_info) > 0:
      return addr_info[0][4][0]
    else:
      self.Q_logs.put('DNS ERROR: skipping ' + hostname)
      return None

  
  # primary routine WITH INTERNAL LOOP for maintenance threads
  # routine is: get cleanup task --> delete old hq after wait --> fill from overflow
  # routine is looped so as not to get stuck in an impasse situation
  def clean_and_fill_loop(self):
    hqs_to_make = 0
    
    # primary loop- must loop so as not to get stuck in impasse situation
    while True

      # get queue to delete & time to delete at; if no hqs to make then block
      get_block = (hqs_to_make == 0)
      try:
        time_to_delete, host_addr = self.Q_hq_cleanup.get(get_block)

        # wait till safe to delete, then delete
        wait_time = time_to_delete - datetime.datetime.now()
        time.sleep(max(0, wait_time.total_seconds()))
        del self.hqs[host_addr]
        hqs_to_make += 1

      # if there are still hqs to make, then don't block on getting more cleanup tasks
      except Queue.Empty:
        pass

      # try a bounded number of times to find a url in overflow that doesn't already have an hq
      for i in range(min(OVERFLOW_TRY_MAX, self.Q_overflow_urls.qsize())):

        # get an overflow url tuple
        r = list(self.Q_overflow_urls.get())
        host_addr = r[0]

        # if hq already exists for this host_addr then recycle and continue
        if self.hqs.has_key(host_addr):
          self.Q_overflow_urls.task_done()
          self.Q_overflow_urls.put(tuple(r))
          continue

        # else create a new hq
        else:
          self.hqs[host_addr] = []

          # if OVERFLOW_MULTI enabled, try to fill the new hq with multiple consecutive
          cn = 0
          while cn < OVERFLOW_MULTI_TRY_L:
            try:
              s = list(self.Q_overflow_urls.get(False))
            
            # don't block on attempt to fill additional urls from overflow here...
            except Queue.Empty:
              break

            # check if the pulled url belongs in the hq, if not recycle
            if s[0] == host_addr:
              self.hqs[host_addr].append(tuple(s[1:]))
            else:
              self.Q_overflow_urls.put(tuple(s))
              cn += 1
            self.Q_overflow_urls.task_done()
          
          # add the original url from overflow to crawl tasks
          r.insert(0, datetime.datetime.now())
          self.Q_crawl_tasks.put(tuple(r))
          hqs_to_make -= 1
          self.Q_overflow_urls.task_done()
          self.Q_hq_cleanup.task_done()


  # primary routine for initialization of url frontier / hqs
  # NOTE: !!! Assumed that this is sole thread running when executed, prior to crawl start
  def initialize(self, urls=[]):
    now = datetime.datetime.now()
    
    # initialize all hqs as either full & tasked or empty & to be deleted
    i = 0
    while len(self.hqs) < HQ_TO_THREAD_RATIO*NUMBER_OF_CTHREADS:
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

    # assume unseen and input to seen list, add to active count
    self.seen.add(url)

    # get host IP address of url
    url_parts = urlparse.urlsplit(url)
    host_addr = self._get_and_log_addr(url_parts.netloc)

    # if the page is not of a safe type log and do not proceed
    if re.search(SAFE_PATH_RGX, url_parts.path) is None:
      self.Q_logs.put("*UN-SAFE PAGE TYPE SKIPPED: %s" % (url,))
      return False

    # if DNS was resolved error already reported, do not proceed any further
    if host_addr is None:
      return False

    # if the page belongs to another node, pass to message sending service
    if DISTR_ON_FULL_URL:
      url_node = hash(url) % NUMBER_OF_NODES
    else:
      url_node = hash(host_addr) % NUMBER_OF_NODES
    if url_node != self.node_n:
      self.Q_message_sender.send((url_node, url, None, 0, None))
      return False

    # add to an existing hq, or create new one & log new crawl task, or add to overflow
    self.Q_active_count.put(True)
    self.total_crawled += 1
    if DEBUG_MODE:
      self.Q_logs.put("Active count: %s" % self.Q_active_count.qsize())
    if self.hqs.has_key(host_addr):
      self.hqs[host_addr].append((url, None, 0, None))
    elif len(self.hqs) < HQ_TO_THREAD_RATIO*NUMBER_OF_CTHREADS:
      self.hqs[host_addr] = []
      self.Q_crawl_tasks.put((datetime.datetime.now(), host_addr, url, None, 0, None))
    else:
      self.Q_overflow_urls.put((host_addr, url, None, 0, None))


  # routine called on abort (by user interrupt or by MAX_CRAWLED count being reached) to
  # save current contents of all queues to disk & seen filter flushed for restart
  def dump_for_restart(self):
    
    # get all urls in Q_crawl_tasks, hqs, or Q_overflow_urls
    # only get urls as these will be re-injected through the initialize method of uf
    with open(RESTART_DUMP, 'w') as f:
      for thead_name, url in self.thread_active.iteritems():
        f.write(url + '\n')

      while self.Q_crawl_tasks.full():
        try:
          r = self.Q_crawl_tasks.get(False)
          f.write(r[2] + '\n')
        except:
          break

      for host_addr, paths in self.hqs.iteritems():
        for path in paths:
          f.write(path[0] + '\n')

      while self.Q_overflow_urls.full():
        try:
          r = self.Q_overflow_urls.get(False)
          f.write(r[1] + '\n')
        except:
          break

    # ensure seen filter file is synced
    self.seen.sync()

#
# --> Command line functionality
#
#if __name__ == '__main__':
#  if sys.argv[1] == 'test' and len(sys.argv) == 2:
#    full_test()
#  else:
#    print 'Usage: python urlFrontier.py ...'
#    print '(1) test'
