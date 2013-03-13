#!/usr/bin/env python

from node_globals import *
from node_locals import *
from pageAnalyze import *
from util import *
from flask import Flask, render_template, request
import classifier


# instantiate Flask app object
app = Flask(__name__)


# instantiate perceptron classifier object
c = classifier.OLClassifier()


@app.route('/', methods=['GET', 'POST'])
def get_feedback():

  if request.method == 'POST':

    # process feedback on last and add to perceptron object queue WITH UNIQUE ID
    tc = 1 if request.form['feedback'] == 'Positive' else -1
    loss = c.feedback(tc)

  # pop one page from the database (with blocking by default enabled)
  with DB_connection(DB_VARS) as handle:
    
    # if applicable, get row from last iteration, delete or transfer depending on feedback
    if request.method == 'POST':
      row = pop_row(handle, DB_PAYLOAD_TABLE, True, request.form["docid"])
      if tc == 1:
        row_dict = {'url': row[1], 'html': row[3], 'parent_url': row[4]}
        insert_row_dict(handle, DB_POSITIVES_TABLE, row_dict)
      
      # use feedback to populate a batch testing table for later classifier testing
      if FILL_BATCH_TEST:
        row_dict = {'url': row[1], 'parent_stats': row[2], 'html': row[3], 'tc': tc}
        insert_row_dict(handle, DB_BATCH_TEST_TABLE, row_dict)


    # get new datum for feedback
    # row should be of form 
    # row = [
    #         id, 
    #         url,
    #         parent_stats,
    #         html
    #         parent_url]; 
    #
    # do not delete at this step
    row = pop_row(handle, DB_PAYLOAD_TABLE, False)

  # extract body html for display
  body_html = re.sub(r'^.*?<body[^>]*>|</body>.*?$', '', row[3], 0, re.DOTALL)

  # get features and run through perceptron to get score- note that this is blocking
  features = extract_features(row[3], string_to_flist(row[2]))
  score = c.classify(features)

  # get weights for testing display feedback
  x, w = c.readable_weights(features)
  
  # return rendered template
  return render_template('analysis.html', docid = int(row[0]), url = row[1], features = row[2], x = x, w = w, score = "%.2f" % (100*score), content = body_html)


if __name__ == '__main__':
  app.run(debug=True)
