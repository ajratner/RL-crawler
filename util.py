#!/usr/bin/env python

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


# FOR MESSAGING/TRANSFER BETWEEN NODES using simple socket datagram --
class MsgReceiver(threading.Thread):
  def __init__(self, Q_rcount, uf=None, Q_logs=None):
    threading.Thread.__init__(self)
    self.uf = uf
    self.Q_logs = Q_logs
    self.Q_rcount = Q_rcount

  def run(self):
    
    # bind a socket to the default port
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("", DEFAULT_IN_PORT))
    while True:

      # receive data and place into urlFrontier via Q_overflow_urls
      data, addr = s.recvfrom(MSG_BUF_SIZE)
      data_tuple = list(pickle.loads(data))
      
      # data_tuple should be of form (url, ref_page_stats, seed_dist, parent_url)
      seed_dist = int(data_tuple[2])
      self.Q_rcount.put(True)
      
      if self.uf is not None:

        # pipe into uf via _add_extracted_url
        url_pkg = (data_tuple[0], data_tuple[1], data_tuple[3])
        self.uf._add_extracted_url(None, seed_dist, url_pkg, True)

      # FOR TESTING:
      else:
        print url


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
  def __init__(self, Q_scount, Q_out, Q_logs=None):
    threading.Thread.__init__(self)
    self.Q_out = Q_out
    self.Q_logs = Q_logs
    self.Q_scount = Q_scount

  def run(self):
    while True:

      # get a message to be sent from the queue
      # of the form: (node_num_to, url, seed_dist, parent_page_stats)
      data_tuple = self.Q_out.get()
      node_num_to = int(data_tuple[0])
      host_to = NODE_ADDRESSES[node_num_to]
      data = pickle.dumps(data_tuple[1:])
    
      # bind a socket to the default port
      s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
      s.bind(("", DEFAULT_OUT_PORT))

      # send message
      s.sendto(data, (host_to, DEFAULT_IN_PORT))
      if DEBUG_MODE and self.Q_logs is not None:
        self.Q_logs.put("%s sent to node %s" % (data_tuple[1], node_num_to))
      self.Q_scount.put(True)


class Q_message_sender:
  def __init__(self, Q_logs=None):
    self.Q_logs = Q_logs
    self.Q_scount = Queue.Queue()
    
    # Queue of messages to be sent to other nodes
    # Queue ~ [ (node_num_to, url, seed_dist, parent_page_stats) ]
    self.Q_out = Queue.Queue()

    # start a sender thread
    ts = MsgSender(self.Q_scount, self.Q_out, self.Q_logs)
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
    with DB_connection(self.db_vars) as handle:
      while True:
        
        # get item from queue, item must be row_dict
        mail_dict = self.Q_out.get()

        # insert row into db
        if insert_row_dict(handle, self.db_table_name, mail_dict):
        
          # if success, then log if applicable
          if self.Q_logs is not None and DEBUG_MODE:
            self.count_mailed += 1
            self.Q_logs.put("Postman: %s html and features payload dropped!\nTotal payloads dropped = %s" % (mail_dict['url'], self.count_mailed))

        # else log as error if applicable, then pass over
        else:
          if self.Q_logs is not None:
            self.Q_logs.put("DB ERROR: PAYLOAD DROP FOR "+mail_dict['url']+" FAILED!")

        # either way report task done to master joining queue if applicable
        if self.uf is not None:

          # if max pages crawled has been reached, terminate the crawl
          if self.count_mailed == MAX_CRAWLED:
            if self.Q_logs is not None:
              self.Q_logs.put("CRAWL REACHED MAX. TERMINATING...")

            # also dump queues first, as they may not be emptied, to prep for restart
            self.uf.dump_for_restart()
            kill_join(self.uf.Q_active_count)

          # pull a task record & record done to handle loop & join type blocking
          task = self.uf.Q_active_count.get()
          self.uf.Q_active_count.task_done()
          if self.Q_logs is not None and DEBUG_MODE:
            self.Q_logs.put("Active count: " + str(self.uf.Q_active_count.qsize()))

    

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
