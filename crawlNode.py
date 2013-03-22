#!/usr/bin/env python

import sys
import traceback
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
from node_globals import *
from node_locals import *
import numpy as np


# basic routine for crawling a single page from url Frontier, extracting links, logging/adding
# back to frontier
def crawl_page(uf, Q_payload, Q_logs, thread_name='Thread-?'):
  
  # get page from urlFrontier
  next_pull_time,host_addr,url,parent_page_stats,host_seed_dist,parent_url = uf.get_crawl_task()

  # report active url
  # NOTE: note that there are problems with this methodology, but that errors will only lead
  # to data redundancy (as opposed to omission)...
  uf.thread_active[thread_name] = url

  if DEBUG_MODE:
    Q_logs.put('%s: got %s from queue' % (thread_name, url))

  # construct full addr-based url
  url_parts = list(urlparse.urlsplit(url))
  root_url = url_parts[1]
  url_parts[1] = host_addr
  url_addr = urlparse.urlunsplit(url_parts)

  # IF PAGE IS A DOC TYPE (e.g. pdf, doc, ...) DO NOT PULL HERE --> STRAIGHT TO DB W MARKER
  if re.search(DOC_PATH_RGX, url_parts[2]) is not None:
    row_dict = {
      'url': url,
      'html': "[%s]" % (re.search(DOC_PATH_RGX, url_parts[2]).group(1),),
      'node': NODE_ID
    }
    if parent_page_stats is not None:
      row_dict['parent_stats'] = flist_to_string(parent_page_stats)
    if parent_url is not None:
      row_dict['parent_url'] = parent_url
    if uf.active:
      Q_payload.Q_out.put(row_dict)
    return True

  # pull page with pyCurl
  buf = cStringIO.StringIO()
  c = pycurl.Curl()

  # set pycurl opts
  c.setopt(c.USERAGENT, USER_AGENT)
  c.setopt(c.URL, url_addr)
  c.setopt(c.HTTPHEADER, ["Host: " + root_url])
  if parent_url is not None:
    c.setopt(c.REFERER, parent_url)
  c.setopt(c.FOLLOWLOCATION, 1)
  c.setopt(c.MAXREDIRS, 5)
  c.setopt(c.TIMEOUT, CURLOPT_TIMEOUT)
  c.setopt(c.WRITEFUNCTION, buf.write)
  
  # delay until >= next_pull_time
  wait_time = next_pull_time - datetime.datetime.now()
  time.sleep(max(0, wait_time.total_seconds()))

  # if uf went inactive since crawl task was pulled, stop now
  if not uf.active:
    return False

  if DEBUG_MODE:
    Q_logs.put('%s: pulling page %s at %s' % (thread_name, url, datetime.datetime.now()))

  # pull page from web and record pull time
  with Timer() as t:
    try:
      c.perform()
      pulled = True
    except Exception as e:
      Q_logs.put('%s: CONNECTION ERROR: %s from parent url %s at %s: %s' % (thread_name, url, parent_url, datetime.datetime.now(), e[1]))
      uf.log_and_add_extracted(host_addr, host_seed_dist, False)
      task = uf.Q_active_count.get()
      uf.Q_active_count.task_done()
      pulled = False
      uf.thread_active[thread_name] = None
  
  if pulled:

    # Check for page transfer success (not connection/transfer timeouts are handled by opts)
    if c.getinfo(c.HTTP_CODE) < 400:

      # parse page for links & associated data
      html = basic_html_clean(buf.getvalue())
      extracted_urls, link_stats = extract_link_data(html, url, Q_logs)
      
      # parse page only for stats that need to be passed on with child links
      page_stats = extract_passed_stats(html)

      # add page, url + features list to queue out (-> database / analysis nodes)
      row_dict = {
        'url': url,
        'html': html,
        'node': NODE_ID
      }
      if parent_page_stats is not None:
        row_dict['parent_stats'] = flist_to_string(parent_page_stats)
      if parent_url is not None:
        row_dict['parent_url'] = parent_url
      if uf.active:
        Q_payload.Q_out.put(row_dict)

      # package all data that needs to be passed on with child links
      # the data format of extracted link packages will be:
      #
      # url_pkg = ( url, parent_page_stats, parent_url )
      #
      # with:
      #
      # parent_page_stats = (
      #                       #: page_text_len,
      #                       #: num_links,
      #                       [t]: title_tokens,
      #                       [t]: link_title_tokens
      #                     )
      extracted_url_pkgs = zip(extracted_urls, [tuple(page_stats) + tuple(ls) for ls in link_stats], [url for x in extracted_urls])

      # log page pull as successful & submit extracted urls + data to url frontier
      if uf.active:
        uf.log_and_add_extracted(host_addr,host_seed_dist, True, t.duration, extracted_url_pkgs)

      # clear thread active here
      # NOTE: there still is a problem if node restart dump occurs AFTER this but before
      #       payload actually sent to disk!!!
      uf.thread_active[thread_name] = None

    else:
      Q_logs.put('%s: CONNECTION ERROR: HTTP code %s from %s, from parent url %s, at %s' % (thread_name, int(c.getinfo(c.HTTP_CODE)), url, parent_url, datetime.datetime.now()))
      uf.log_and_add_extracted(host_addr, host_seed_dist, False)
      task = uf.Q_active_count.get()
      uf.Q_active_count.task_done()
      uf.thread_active[thread_name] = None


# crawl thread class
class CrawlThread(threading.Thread):
  def __init__(self, uf, Q_payload, Q_logs):
    threading.Thread.__init__(self)
    self.uf = uf
    self.Q_payload = Q_payload
    self.Q_logs = Q_logs

  def run(self):
    try:
      while True:
        crawl_page(self.uf, self.Q_payload, self.Q_logs, self.getName())
    except:
      handle_thread_exception(self.getName(), 'crawl-thread', self.uf, self.Q_logs)


# maintenance thread class
class MaintenanceThread(threading.Thread):
  def __init__(self, uf, Q_logs):
    threading.Thread.__init__(self)
    self.uf = uf
    self.Q_logs = Q_logs

  def run(self):
    try:
      self.uf.clean_and_fill_loop()
    except:
      handle_thread_exception(self.getName(), 'maint-thread', self.uf, self.Q_logs)


# main multi-thread crawl routine
def multithread_crawl(node_n, initial_url_list, seen_persist=False):

  # initialize activity monitor row- need to esp clear stop flags from previous run!
  with DB_connection(DB_VARS) as handle:
    row_dict = {'active_count':0, 'rcount':0, 'scount':0, 'failure':0, 'stop_order':0, 'init':0}
    insert_or_update(handle, DB_NODE_ACTIVITY_TABLE, (node_n+1), row_dict)
  
  # instantiate a queue-out-to-logs handler
  Q_logs = Q_out_to_file(LOG_REL_PATH)
  Q_logs.put("\n\nSession Start at %s" % (datetime.datetime.now(),))

  # instantiate one urlFontier object for all threads
  uf = urlFrontier(node_n, seen_persist, Q_logs)

  # instantiate a queue-out-to-db handler
  Q_payload = Q_out_to_db(DB_VARS, DB_PAYLOAD_TABLE, uf, Q_logs)

  # instantiate a node message sender
  Q_ms = Q_message_sender(uf, Q_logs)

  # instantiate a node message receiver now that urlFrontier is initialized with seed list
  Q_mr = Q_message_receiver(uf, Q_logs)

  # wait an optional start delay time while still receiving messages to active uf
  time.sleep(NODE_START_DELAY)

  # initialize the urlFrontier
  uf.initialize(initial_url_list)

  # spawn a pool of daemon CrawlThread threads
  for i in range(NUMBER_OF_CTHREADS):
    t = CrawlThread(uf, Q_payload, Q_logs)
    t.setDaemon(True)
    t.start()

  # spawn a pool of daemon MaintenanceThread threads
  for i in range(NUMBER_OF_MTHREADS):
    t = MaintenanceThread(uf, Q_logs)
    t.setDaemon(True)
    t.start()

  print 'crawl started (NODE %s of %s, %s + %s threads); Ctrl-C to abort' % ((node_n+1), NUMBER_OF_NODES, NUMBER_OF_CTHREADS, NUMBER_OF_MTHREADS)

  # main loop- waits for node active count queues to be empty & all inter-node messaging done
  check_count = 0
  try: 
    while True:
      time.sleep(ACTIVITY_CHECK_P)

      # send updated info to activity monitor table of central db
      with DB_connection(DB_VARS) as handle:
        row_dict = {
          'active_count': uf.Q_active_count.qsize(), 
          'rcount': Q_mr.rcount(),
          'scount': Q_ms.scount(),
          'init': 1 }
        insert_or_update(handle, DB_NODE_ACTIVITY_TABLE, (node_n + 1), row_dict)
        if DEBUG_MODE:
          Q_logs.put("Submitted node activity status (a: %s, s: %s, r: %s)" % (uf.Q_active_count.qsize(), Q_ms.scount(), Q_mr.rcount()))
          Q_logs.put("uf status: (pd: %s, ct: %s, hqs: %s, ou: %s, hqc: %s)" % (uf.payloads_dropped, uf.Q_crawl_tasks.qsize(), sum([len(v) for k,v in uf.hqs.iteritems()]), uf.Q_overflow_urls.qsize(), uf.Q_hq_cleanup.qsize()))

        time.sleep(ACTIVITY_CHECK_P/10.0)

        # check activity monitor db for global stop conditions
        node_rows = get_rows(handle, DB_NODE_ACTIVITY_TABLE, NUMBER_OF_NODES)
        nr_sums = np.sum(np.array(node_rows), 0)

        # [A] Crawl completed if active counts all == 0 & sent == received & all have been init
        if nr_sums[1] == 0 and nr_sums[2] == nr_sums[3] and nr_sums[6] == NUMBER_OF_NODES:
          Q_logs.put("crawl completed at %s" % (datetime.datetime.now(),))
          print 'crawl completed!'
          sys.exit(0)

        # [B] If a thread exception was handled on this node, shut down
        elif node_rows[NODE_ID][4] > 0:
          sys.exit(1)
        
        # [C] If a thread exception was handled on another node, shut down
        elif nr_sums[4] > 0:
          Q_logs.put("FAILURE IN OTHER NODE-- dumping for restart & shutting down")
          uf.dump_for_restart()
          sys.exit(2)
        
        # [D] If a manual stop was ordered via the activity monitor tablem shut down
        elif nr_sums[5] > 0:
          Q_logs.put("MANUAL STOP ORDERED-- dumping for restart & shutting down")
          uf.dump_for_restart()
          sys.exit(3)
          
      check_count += 1

  # In case of Ctrl-C, or any other failure, abort gracefully
  except (Exception, KeyboardInterrupt):
    handle_thread_exception('main', 'main', uf, Q_logs, True)


#
# --> Command line functionality
#
if __name__ == '__main__':
  if sys.argv[1] == 'run' and len(sys.argv) == 2:
    multithread_crawl(NODE_ID, SEED_LIST)
  elif sys.argv[1] == 'restart' and len(sys.argv) == 2:
    with open(RESTART_DUMP, 'r') as f:
      restart_seeds = [re.sub(r'\n', '', l) for l in f.readlines()]
    multithread_crawl(NODE_ID, restart_seeds, True)
  else:
    print 'Usage: python crawlNode.py ...'
    print '(1) run'
    print '(2) restart'
