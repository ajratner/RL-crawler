#!/usr/bin/env python

import sys
import traceback
import time
import MySQLdb as mdb
import threading
import Queue
import os
from node_globals import *
from node_locals import *
import re
import pickle
import urlparse
import socket
import datetime


# timing using "with"
class Timer:
  def __enter__(self):
    self.start = time.clock()
    return self

  
  def __exit__(self, *args):
    self.end = time.clock()
    self.duration = self.end - self.start


# simple multi-thread safe data funnel class:
# instantiates Queue.Queue() which collects items from threads, and then transfers them out
# will also report task done to global queue if one is passed in


# subfunction for killing off a queue to free from a join on it
def kill_join(Q):
  while True:
    record = Q.get()
    Q.task_done()


# EXCEPTION HANDLING SUBFUNCTIONS -->

# thread exception handler function
def handle_thread_exception(thread_name, thread_type, uf, Q_logs=None, sys_exit=False):
  
  # deactivate url frontier
  uf.active = False
  
  # log full exception traceback
  exc_type, exc_value, exc_tb = sys.exc_info()
  if Q_logs is not None:
    Q_logs.put('\n***************\nTHREAD EXCEPTION in %s (%s) at %s:\n%s' % (thread_name, thread_type, datetime.datetime.now(), ''.join(traceback.format_exception(exc_type, exc_value, exc_tb))))
  
  # log uf detailed state if possible
  if Q_logs is not None:
    Q_logs.put("uf status: (pd: %s, ct: %s, hqs: %s, ou: %s, hqc: %s, ton: %s)" % (uf.payloads_dropped, uf.Q_crawl_tasks.qsize(), sum([len(v) for k,v in uf.hqs.iteritems()]), uf.Q_overflow_urls.qsize(), uf.Q_hq_cleanup.qsize(), uf.Q_to_other_nodes.qsize()))

  # dump for restart if possible
  uf.dump_for_restart()
  if Q_logs is not None:
    Q_logs.put("Restart file dumped successfully")
  else:
    Q_logs.put("Restart dump not possible- use last periodic restart dump from normal routine")

  # report failure to activity monitor
  with DB_connection(DB_VARS) as handle:
    insert_or_update(handle, DB_NODE_ACTIVITY_TABLE, (NODE_ID+1), {'failure': 1})
    if Q_logs is not None:
      Q_logs.put("Sent failure notice to activity table")

  # shut down entire node
  # NOTE: could have less sensitive reaction down the road...?
  if sys_exit:
    if Q_logs is not None:
      Q_logs.put("Shutting down node %s" % (NODE_ID,))
    sys.exit(0)
  else:
    if Q_logs is not None:
      Q_logs.put("Shutting down node %s at next node activity check" % (NODE_ID,))


# FOR MESSAGING/TRANSFER BETWEEN NODES using simple socket datagram --
class MsgReceiver(threading.Thread):
  def __init__(self, Q_rcount, uf=None, Q_logs=None):
    threading.Thread.__init__(self)
    self.uf = uf
    self.Q_logs = Q_logs
    self.Q_rcount = Q_rcount

  def run(self):    
    try:
     
      # bind a blocking socket for receiving messages
      s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      s.setblocking(1)
      s.bind(("", DEFAULT_IN_PORT))

      # bind a socket for sending confirmation signal
      c = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      c.bind(("", CONFIRM_OUT_PORT))

      # receive messages and send confirm signal once added to uf
      while True and self.uf.active:

        # BLOCK until received data and place into urlFrontier via Q_overflow_urls
        data, addr = s.recvfrom(MSG_BUF_SIZE)
        data_tuple = list(pickle.loads(data))
        
        # data_tuple should be of form (url, ref_page_stats, seed_dist, parent_url)
        seed_dist = int(data_tuple[2])
        if self.Q_logs is not None and DEBUG_MODE:
          self.Q_logs.put("Received %s from node at %s" % (data_tuple[0], addr))
        
        # pipe into uf via _add_extracted_url
        url_pkg = (data_tuple[0], data_tuple[1], data_tuple[3])
        self.uf._add_extracted_url(None, seed_dist, url_pkg, True)

        # once data has been processed into url frontier, send confirmation
        # NOTE: could be faster -> less cautious here...
        self.Q_rcount.put(True)
        c.sendto("success", (addr[0], CONFIRM_IN_PORT))
        if self.Q_logs is not None and DEBUG_MODE:
          self.Q_logs.put("Sent confirmation of reception of %s to node at %s" % (data_tuple[0], addr))

    except:
      handle_thread_exception(self.getName(), 'receive-thread', self.uf, self.Q_logs)


class Q_message_receiver:
  def __init__(self, uf=None, Q_logs=None):
    self.uf = uf
    self.Q_logs = Q_logs
    self.Q_rcount = Queue.Queue()

    # start a receiver thread
    tr = MsgReceiver(self.Q_rcount, self.uf, self.Q_logs)
    tr.setDaemon(True)
    tr.start()

  def rcount(self):
    return self.Q_rcount.qsize()


class MsgSender(threading.Thread):
  def __init__(self, Q_scount, uf, Q_logs=None):
    threading.Thread.__init__(self)
    self.uf = uf
    self.Q_logs = Q_logs
    self.Q_scount = Q_scount

  def run(self):
    try:

      # bind a socket for sending messages
      s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      s.bind(("", DEFAULT_OUT_PORT))

      # bind a timeout-bounded blocking socket for receiving confirmation signals
      c = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      c.settimeout(CONFIRM_WAIT_TIME)
      c.bind(("", CONFIRM_IN_PORT))

      while True and self.uf.active:

        # get a message to be sent from the queue
        # of the form: (node_num_to, url, seed_dist, parent_page_stats)
        data_tuple = self.uf.Q_to_other_nodes.get()
        node_num_to = int(data_tuple[0])
        host_to = NODE_ADDRESSES[node_num_to]
        data = pickle.dumps(data_tuple[1:])
      
        # send message
        s.sendto(data, (host_to, DEFAULT_IN_PORT))
        if DEBUG_MODE and self.Q_logs is not None:
          self.Q_logs.put("%s sent to node %s" % (data_tuple[1], node_num_to))

        # wait for confirmation; if no confirm, recycle, log error
        try:
          data, addr = c.recvfrom(MSG_BUF_SIZE)
          if data == "success":

            # on success - update sent count, uf active count, log optionally
            self.Q_scount.put(True)
            task = self.uf.Q_active_count.get()
            self.uf.Q_active_count.task_done()
            if self.Q_logs is not None and DEBUG_MODE:
              self.Q_logs.put("Confirmation on %s received by %s" % (data_tuple[1], addr))
          
          # handle improper confirm message (shouldn't occur...)
          else:
            self.uf.Q_to_other_nodes.put(data_tuple)
            if self.Q_logs is not None:
              self.Q_logs.put("Confirmation message error: %s, placing message back in out queue..." % (data,))

        # handle confirmation receipt timeout- recycle message back to out queue
        except:
          self.uf.Q_to_other_nodes.put(data_tuple)
          if self.Q_logs is not None:
            self.Q_logs.put("CONFIRMATION TIMEOUT FROM NODE %s on receipt of %s, placing back in out queue..." % (node_num_to, data_tuple[1]))

    except:
      handle_thread_exception(self.getName(), 'send-thread', self.uf, self.Q_logs)


class Q_message_sender:
  def __init__(self, uf, Q_logs=None):
    self.Q_logs = Q_logs
    self.Q_scount = Queue.Queue()
    
    # Queue of messages to be sent to other nodes
    # Queue ~ [ (node_num_to, url, seed_dist, parent_page_stats) ]
    self.uf = uf

    # start a sender thread
    ts = MsgSender(self.Q_scount, self.uf, self.Q_logs)
    ts.setDaemon(True)
    ts.start()

  def send(self, pkg):
    self.Q_out.put(pkg)

  def scount(self):
    return self.Q_scount.qsize()


# FOR TRANSFER TO DB --
class PostmanThreadDB(threading.Thread):
  def __init__(self, Q_out, db_vars, db_table_name, uf=None, Q_logs=None):
    threading.Thread.__init__(self)
    self.Q_out = Q_out
    self.db_vars = db_vars
    self.db_table_name = db_table_name
    self.uf = uf
    self.Q_logs = Q_logs
    self.count_mailed = 0

  
  def run(self):
    try:
      with DB_connection(self.db_vars) as handle:
        while True:
          
          # get item from queue, item must be row_dict
          mail_dict = self.Q_out.get()

          # if max pages crawled has been reached, quit here; note Q_out will be drained
          if not self.uf.active:
            continue

          # insert row into db
          if insert_row_dict(handle, self.db_table_name, mail_dict):
          
            # if success, then log if applicable
            self.count_mailed += 1
            self.uf.payloads_dropped += 1
            if self.Q_logs is not None and DEBUG_MODE:
              self.Q_logs.put("Postman: %s html and features payload dropped!\nTotal payloads dropped = %s" % (mail_dict['url'], self.count_mailed))

          # else log as error if applicable, then pass over
          else:
            if self.Q_logs is not None:
              self.Q_logs.put("DB ERROR: PAYLOAD DROP FOR "+mail_dict['url']+" FAILED!")

          # either way report task done to master joining queue if applicable
          if self.uf is not None:

            # if max pages crawled has been reached, terminate the crawl
            if self.count_mailed == MAX_CRAWLED:

              # log if possible
              if self.Q_logs is not None:
                self.Q_logs.put("CRAWL REACHED MAX. TERMINATING...")

              # deactivate node, dump for restart and empty active count
              self.uf.active = False
              self.uf.dump_for_restart()

              # drain active count to 0 and block here
              kill_join(self.uf.Q_active_count)

            # pull a task record & record done to handle loop & join type blocking
            task = self.uf.Q_active_count.get()
            self.uf.Q_active_count.task_done()
            if self.Q_logs is not None and DEBUG_MODE:
              self.Q_logs.put("Active count: " + str(self.uf.Q_active_count.qsize()))

    except:
      handle_thread_exception(self.getName(), 'db-thread', self.uf, self.Q_logs)
    

class Q_out_to_db:
  def __init__(self, db_vars, db_table_name, uf=None, Q_logs=None):
    self.db_vars = db_vars
    self.db_table_name = db_table_name
    self.uf = uf
    self.Q_logs = Q_logs

    # the queue of packages to be sent out
    self.Q_out = Queue.Queue()

    # start the 'postman' worker thread
    t = PostmanThreadDB(self.Q_out, self.db_vars, self.db_table_name, self.uf, self.Q_logs)
    t.setDaemon(True)
    t.start()

  def put(self, row_dict):
    self.Q_out.put(row_dict)


# FOR TRANSFER TO FILE --
class PostmanThreadFile(threading.Thread):
  def __init__(self, Q_out, fpath, Q_task_done=None):
    threading.Thread.__init__(self)
    self.Q_out = Q_out
    self.fpath = fpath
    self.Q_task_done = Q_task_done

  
  def run(self):
    while True:
      with open(self.fpath, 'a') as f:

        # get item from queue, item should be string
        mail_string = str(self.Q_out.get()) + '\n'

        # write line to file
        f.write(mail_string)

        # report item out success to master joining queue if applicable
        if self.Q_task_done is not None:
          self.Q_task_done.task_done()
    

class Q_out_to_file:
  def __init__(self, fpath_rel, Q_task_done=None):
    self.fpath = os.path.join(os.path.dirname(__file__), fpath_rel)
    self.Q_task_done = Q_task_done

    # the queue of packages to be sent out
    self.Q_out = Queue.Queue()

    # start the 'postman' worker thread
    t = PostmanThreadFile(self.Q_out, self.fpath, self.Q_task_done)
    t.setDaemon(True)
    t.start()

  def put(self, string):
    self.Q_out.put(string)


# conversion from list of features- numbers or token-lists- to a string e.g. 
#     [1, 3.4, ('apple', 'bob'), 5] --> "1;3.4;apple,bob;5"
def flist_to_string(flist):
  string_out = ""
  for f in flist:
    if type(f) == int or type(f) == float:
      string_out += str(f)
    elif type(f) == tuple or type(f) == list:
      string_out += ','.join(f)
    string_out += ';'
  return string_out

def string_to_flist(string):
  if string is not None:
    flist = []
    for f in re.split(r';', string)[:-1]:
      if re.search(r'\d', f) is not None:
        flist.append(float(f))
      else:
        flist.append([t for t in re.split(r',', f) if t != ''])
    return flist
  else:
    return None


# --> SIMPLE MYSQL/DB INTERFACE

# example simple table:
#
# mysql -u root -p
# USE crawler_test;
# CREATE TABLE interface_test (
#     id INT NOT NULL AUTO_INCREMENT,
#     col1 VARCHAR(200),
#     PRIMARY KEY (id)
# )
# ...
# DROP TABLE interface_test;

# simple mysql connection class to be used in "with" clause
# returns a 'handle' on the database = (conn, cur)
class DB_connection:
  def __init__(self, db_vars, db_type='MYSQL'):

    # db_vars = (DB_HOST, DB_USER, DB_PWD, DB_NAME)
    self.db_vars = db_vars
    self.db_type = db_type

  
  # open connection & cursor
  def __enter__(self):
    self.conn = mdb.connect(*self.db_vars)
    self.cur = self.conn.cursor()
    return (self.conn, self.cur)

  
  # close connection & cursor
  def __exit__(self, *args):
    self.cur.close()
    self.conn.close()


# insert row from dict of values
def insert_row_dict(handle, table_name, row_dict):
  q = "INSERT INTO " + table_name + " (" + ', '.join(row_dict.keys()) + ") VALUES (" + ', '.join(["%s" for i in range(len(row_dict))]) + ")"
  try:
    handle[1].execute(q, tuple(row_dict.values()))
    handle[0].commit()
    return True
  except mdb.Error, e:
    handle[0].rollback()
    return False


# pop a row
def pop_row(handle, table_name, delete=True, row_id=None, blocking=True):
  row = None

  # if blocking is True, loop until row pulled
  while row is None:
    q = "SELECT * FROM " + table_name
    
    # optional: pop specific row
    if row_id is not None:
      q += (" WHERE id=%s" % (row_id))
    else:
      q += " LIMIT 1"

    # execute & get row
    handle[1].execute(q)
    row = handle[1].fetchone()
    if not blocking:
      break
    else:
      time.sleep(1)
  
  # delete pulled row if applicable
  if delete and row is not None:
    q = "DELETE FROM " + table_name + " WHERE id = %s"
    handle[1].execute(q, (int(row[0]),))
    handle[0].commit()
  return row


# insert or update row by id with dict
def insert_or_update(handle, table_name, row_id, row_dict):
  q = "INSERT INTO " + table_name + " (id, " + ', '.join(row_dict.keys()) + ") VALUES (" + ', '.join(["%s" for i in range(len(row_dict) + 1)]) + ") ON DUPLICATE KEY UPDATE " + ', '.join(["%s = %s" % (k, v) for k,v in row_dict.iteritems()])
  q_vals = row_dict.values()
  q_vals.insert(0, row_id)
  try:
    handle[1].execute(q, tuple(q_vals))
    handle[0].commit()
    return True
  except mdb.Error, e:
    print e
    handle[0].rollback()
    return False


# get n (or ALL if n is None) rows
def get_rows(handle, table_name, n=None):
  q = "SELECT * FROM " + table_name
  if n is not None:
    q += (" LIMIT %s" % (int(n),))
  handle[1].execute(q)
  return handle[1].fetchall()


# delete all rows
def clear_table(handle, table_name):
  q = "DELETE FROM " + table_name
  try:
    handle[1].execute(q)
    handle[0].commit()
    return True
  except mdb.Error, e:
    print e
    handle[0].rollback()
    return False

