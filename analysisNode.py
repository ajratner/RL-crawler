from node_globals import *
from util import *
from flask import Flask, render_template


# instantiate Flask app object
app = Flask(__name__)


# instantiate perceptron classifier object
# NOTE: TO-DO


@app.route('/')
def get_feedback():

  # process feedback on last and add to perceptron object queue WITH UNIQUE ID
  # NOTE: TO-DO

  # pop one page from the database (with blocking by default enabled)
  with DB_connection(DB_VARS) as handle:

    # row should be of form [id, url, features_string, html]
    row = pop_row(handle, DB_PAYLOAD_TABLE, True, False)  # delete disabled for testing...

  # extract body html for display
  body_html = re.sub(r'^.*?<body>|</body>.*?$', '', row[3])

  # run through perceptron to get score
  # NOTE: TO-DO
  
  # return rendered template
  return render_template('analysis.html', url=row[1], features=row[2], score="0", content=body_html)


if __name__ == '__main__':
  app.run(debug=True)
