from node_globals import *
from util import *
from flask import Flask, render_template, request
from perceptron import Perceptron


# instantiate Flask app object
app = Flask(__name__)


# instantiate perceptron classifier object
p = Perceptron()


@app.route('/', methods=['GET', 'POST'])
def get_feedback():

  if request.method == 'POST':

    # process feedback on last and add to perceptron object queue WITH UNIQUE ID
    tc = 1 if request.form['feedback'] == 'Positive' else -1
    p.feedback(tc)

  # pop one page from the database (with blocking by default enabled)
  with DB_connection(DB_VARS) as handle:
    
    # get row from last iteration, delete or transfer depending on feedback
    row = pop_row(handle, DB_PAYLOAD_TABLE, True, request.form["docid"])
    if tc == 1:
      row_dict = {url: row[1], html: row[3]}
      insert_row_dict(handle, DB_POSITIVES_TABLE, row_dict)

    # get new datum for feedback
    # row should be of form [id, url, features_string, html]; do not delete at this step
    row = pop_row(handle, DB_PAYLOAD_TABLE, False)

  # extract body html for display
  body_html = re.sub(r'^.*?<body[^>]*>|</body>.*?$', '', row[3])

  # run through perceptron to get score- note that this is blocking
  score = p.classify(string_to_flist(row[2]))
  
  # return rendered template
  return render_template('analysis.html', docid = int(row[0]), url = row[1], features = row[2], score = "%.2f" % (100*score), content = body_html)


if __name__ == '__main__':
  app.run(debug=True)


# FEATURE PARSING SHOULD BE LOCAL (ON ANALYSIS NODE)??
# NOTE: TO-DO: consider this...
