#!/usr/bin/env python

from util import *
from node_globals import *
from node_locals import *
from pageAnalyze import *
import classifier
import sys
import os
import csv
import re


# copy all records from payload table to test table in proper format
# NOTE: labelling the data with page's true class must be done manually
def populate_test_table():
  with DB_connection(DB_VARS) as handle:
    
    # get starting id in payload table
    row1 = pop_row(handle, DB_PAYLOAD_TABLE, False, None, False)
    i = int(row1[0])

    while True:
      row_p = pop_row(handle, DB_PAYLOAD_TABLE, False, i, False)
      if row_p is None:
        break
      row_t_dict = {'url': row_p[1], 'parent_stats': row_p[2], 'html': row_p[3], 'tc': -1}
      insert_row_dict(handle, DB_BATCH_TEST_TABLE, row_t_dict)
      i += 1


# runs through test batch, outputs paramter evolution as csv file
def batch_test(filepath_out):
  
  # get proper absolute filepath
  if re.search(r'(/|^)[A-Za-z0-9_]+\.csv', filepath_out) is None:
    sys.exit("Improper csv file path")
  fpath = os.path.join(os.path.dirname(__file__), filepath_out)
  
  with Timer() as t:
    # perceptron (or other algorithm) object
    c = classifier.OLClassifier()

    # loop serially through all the rows of the batch test table, writing results to csv
    with open(fpath, 'wb') as out_file:
      out = csv.writer(out_file)
      data = []
      with DB_connection(DB_VARS) as handle:
        
        # get starting index
        row1 = pop_row(handle, DB_BATCH_TEST_TABLE, False, None, False)
        r = int(row1[0])
        n_pages = 0

        # loop through all rows
        while True:

          # get row- rows should be of form (id, url, parent_stats, html, true_class)
          row = pop_row(handle, DB_BATCH_TEST_TABLE, False, r, False)
          if row is None:
            break

          # extract features and calculate prediction, then updated parameters given true class
          features = extract_features(row[3], string_to_flist(row[2]))
          score = c.classify(features)
          
          # decide whether or not to skip user feedback input
          tc = int(row[4])
          if FEEDBACK_THRESH:
            if abs(score) > 0.5:

              # skip submitting feedback
              c.skip_feedback()
              loss = max(0.0, 1 - tc*score)
            else:
              loss = c.feedback(tc)
          else:
            loss = c.feedback(tc)
          data.append([int(row[0]), row[1], score, tc, loss, c.W])

          r += 1
          n_pages += 1

      # assemple header row and write to file
      header = ["db_id", "url", "score", "tc", "LOSS"]
      for i in range(len(features)):
        if c.token_maps.has_key(i):
          header += [k for v,k in sorted([(v,k) for k,v in c.token_maps[i].iteritems()])]
        else:
          header.append("NUM")
      out.writerow(header)

      # extend all input features to full vector length and insert as rows
      for d in data:
        row = [d[0], d[1], d[2], d[3], d[4]]
        W = d[5]
        for i in range(len(W)):
          if c.token_maps.has_key(i):
            row += W[i] + [0 for j in range(len(c.token_maps[i]) - len(W[i]))]
          else:
            row.append(W[i])
        out.writerow(row)
  
  print 'Classified %s pages in %s seconds' % (n_pages, t.duration)


def test_html_calcs(filepath_out):

  # get proper absolute filepath
  if re.search(r'(/|^)[A-Za-z0-9_]+\.csv', filepath_out) is None:
    sys.exit("Improper csv file path")
  fpath = os.path.join(os.path.dirname(__file__), filepath_out)
  
  with Timer() as t:

    # loop serially through all the rows of the batch test table, writing results to csv
    with open(fpath, 'wb') as out_file:
      out = csv.writer(out_file)
      out.writerow(('url', 'ptl', 'rptl', 'rptl_norm', 'nl', 'rnl', 'rnl_norm', 'lts', 'lts_norm'))
      with DB_connection(DB_VARS) as handle:
        
        # get starting index
        row1 = pop_row(handle, DB_BATCH_TEST_TABLE, False, None, False)
        r = int(row1[0])
        n_pages = 0

        # loop through all rows
        while True:

          # get row- rows should be of form (id, url, parent_stats, html, true_class)
          row = pop_row(handle, DB_BATCH_TEST_TABLE, False, r, False)
          if row is None:
            break

          # extract all html-parsing features, also in absolute (non-norm.) form, + url & html
          pt = get_page_text(row[3])
          ptl = float(len(pt))
          nl = float(len(re.findall(r'<a\s.*?>', row[3])))
          lts = calc_LTS(row[3])
          lts_norm = sl_normalize(lts/1000.0)
          parent_page_stats = string_to_flist(row[2])
          if parent_page_stats is not None:
            p_ptl, p_nl, p_tt, p_ltt = parent_page_stats
            rptl = ptl / p_ptl
            rptl_norm = sl_normalize(rptl)
            rnl = nl / p_nl
            rnl_norm = sl_normalize(rnl)
          else:
            rptl = 0.0
            rptl_norm = 0.0
            rnl = 0.0
            rnl_norm = 0.0
          out.writerow((row[1], ptl, rptl, rptl_norm, nl, rnl, rnl_norm, lts, lts_norm))

          r += 1
          n_pages += 1


# command line functionality
if __name__ == '__main__':
  if len(sys.argv) == 2 and sys.argv[1] == 'populate':
    populate_test_table()
  elif len(sys.argv) == 3 and sys.argv[1] == 'run':
    batch_test(sys.argv[2])
  elif len(sys.argv) == 3 and sys.argv[1] == 'test_html_calcs':
    test_html_calcs(sys.argv[2])
  else:
    print 'USAGE: python batchTest.py ...'
    print '  (1) run <rel_filepath_output>'
    print '  (2) populate'
    print '  (3) test_html_calcs <rel_filepath_output>'
