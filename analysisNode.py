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
    if request.form['feedback'] == 'Positive':
      tc = 1
    else:
      tc = -1
    p.feedback(tc)

  # pop one page from the database (with blocking by default enabled)
  with DB_connection(DB_VARS) as handle:

    # row should be of form [id, url, features_string, html]
    row = pop_row(handle, DB_PAYLOAD_TABLE)

  # extract body html for display
  body_html = re.sub(r'^.*?<body>|</body>.*?$', '', row[3])

  # run through perceptron to get score- note that this is blocking
  score = p.classify(string_to_flist(row[2]))
  
  # return rendered template
  return render_template('analysis.html', url=row[1], features=row[2], score= "%.2f" % (100*score), content=body_html)


if __name__ == '__main__':
  app.run(debug=True)


# FEATURE PARSING SHOULD BE LOCAL (ON ANALYSIS NODE)??
# NOTE: TO-DO: consider this...
