import sys
import re
import MySQLdb as mdb
import pycurl
import cStringIO
import bs4
from bs4 import BeautifulSoup
from collections import Counter
from nltk.stem.porter import PorterStemmer
from util import *


# re subfunction for returning page text only
def get_page_text(html):
  return re.sub(r'\s', '', re.sub(r'\n(\s*\n)+', r'\n', re.sub(r'&\w{2,4};', '', re.sub(r'<.*?>', '', re.sub(r'<(a|script|style).*?>.*?</\1>', '', html, 0, re.DOTALL)))))

# re subfunction for getting page tokens
def tokens(string):
  return re.findall(r'[A-Za-z]+', string)

# Return the most frequent, non-common words
# NOTE: should words be stemmed?  too expensive probably...
STOP_TOKENS = ["this", "that", "shall", "under", "with", "other", "within", "from", "such", "which", "means", "each", "have", "including", "upon", "after", "these", "been", "include", "otherwise", "against", "least", "through", "than", "unless", "does", "either", "whether", "without", "only", "between", "described", "percent", "their", "then", "those", "when", "except", "into", "during", "iii", "where", "would", "they", "itself", "last", "there", "also", "below", "here", "includes", "more", "neither", "being", "both", "cannot", "about", "above", "were", "first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eigth", "ninth", "tenth", "three", "four", "five", "seven", "eight", "nine"]

def mf_words(pg_txt, n):
  c = Counter([t.lower() for t in tokens(pg_txt) if len(t) > 3])
  for sw in STOP_TOKENS:
    del c[sw]
  return [w[0] for w in c.most_common(n)]

# Return the average / longest text string
# NOTE: to-do: handle divs?  I.e. if at lvl div, text should count... but should end with </div>
# NOTE: to-do: handle <h\n>?  Look through w3 list of tags and handle all...
TEXT_TAGS = ["p", "b", "i", "u", "ul", "li", "table", "th", "tr", "td", "tbody", "thead", "tfoot"]
SKIP_TAGS = ["br", "hr", "img", "link", "meta", "META", "if", "endif"]
def calc_LTS(html):
  ts = 0
  lts = 0

  # split up into [<...>] [...] [</...>]
  split_html = re.findall(r'<[^>]+>|[^><]+', html)

  # count text sequences
  lvl = ['top'];
  for t in split_html:
    if t[0] == '<' and t[1] == '/':
      del lvl[-1]
    elif t[0] == '<':
      try:
        tag = re.match('<\W*(\w+)', t).group(1)
        if tag not in SKIP_TAGS:
          lvl.append(tag)
          ts = ts if lvl[-1] in TEXT_TAGS else 0
      except AttributeError:
        print 'Error extracting tag from ' + t
    elif lvl[-1] in TEXT_TAGS:
      ts += len(re.sub(r'\s', '', t))
      lts = ts if ts > lts else lts
  return lts


def get_page_features(html, ref_stats):
  
  # ref stats are passed in from referring page
  # ref link anchor text, ref link context, ref title, ref percent text, ref num of links
  # NOTE: there is some variability here dependent on which page referred first...
  ref_t, ref_ptl, ref_nl, ref_lt = ref_stats
  html_textonly = get_page_test(html)

  # calculate & package stats / features
  try:
    title = re.match(r'<title[^>]*>(.*?)</title>', html).group(1)
  except:
    title = None
  pt = get_page_text(html)
  ptl = float(len(pt))
  rpt = ptl / ref_pt
  nl = float(len(re.findall(r'<a\s.*?>', html)))
  rnl = nl / ref_nl
  lts = calc_LTS(html)
  mf_words = mf_words(pt, 20)


  # have a periodically updated vocab list- take the intersection of top words,
  # and then record any unseen ones for addition to set in next it
  # ONLINE LEARNING?


  # NOTE: need to think of way to update / add to vocabulary...



  
# analyze page payload- simple perceptron for now
def analyze_page(page_html, page_features, w):

