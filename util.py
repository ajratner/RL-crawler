import time
import MySQLdb as mdb
import threading
import Queue


# FOR TESTING OF DB INTERFACE CLASS
DB_TEST_VARS = ('localhost', 'root', 'penguin25', 'crawler_test')


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
class PostmanThread(threading.Thread):
  def __init__(self, Q_out, db_vars, db_table_name, Q_task_done=None):
    threading.Thread.__init__(self)
    self.Q_out = Q_out
    self.db_vars = db_vars
    self.db_table_name = db_table_name
    self.Q_task_done = Q_task_done

  
  def run(self):
    with DB_connection(self.db_vars) as handle:
      while True:
        
        # get item from queue, item must be row_dict
        mail_dict = self.Q_out.get()

        # insert row into db
        insert_row_dict(handle, self.db_table_name, mail_dict)

        # report item out success to master joining queue if applicable
        if self.Q_task_done is not None:
          self.Q_task_done.task_done()
    

class Q_out_to_db:
  def __init__(self, db_vars, db_table_name, Q_task_done=None):
    self.db_vars = db_vars
    self.db_table_name = db_table_name
    self.Q_task_done = Q_task_done

    # the queue of packages to be sent out
    self.Q_out = Queue.Queue()

    # start the 'postman' worker thread
    t = PostmanThread(self.Q_out, self.db_vars, self.db_table_name, self.Q_task_done)
    t.setDaemon(True)
    t.start()


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
def pop_row(handle, table_name):
  q = "SELECT * FROM " + table_name + " LIMIT 1"
  handle[1].execute(q)
  row = handle[1].fetchone()
  if row is not None:
    q = "DELETE FROM " + table_name + " WHERE id = %s"
    handle[1].execute(q, (int(row[0]),))
    handle[0].commit()
  return row
  
