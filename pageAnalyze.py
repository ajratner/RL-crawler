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
import urlparse
from node_globals import *


# re subfunction for returning page text only
def get_page_text(html):
  return re.sub(r'\n(\s*\n)+', r'\n', re.sub(r'&\w{2,4};', '', re.sub(r'<.*?>', '', re.sub(r'<(a|script|style).*?>.*?</\1>', '', html, 0, re.DOTALL))))


# re subfunction for getting page tokens
def tokens(string):
  if string is not None:
    return [t.lower() for t in re.findall(r'[A-Za-z]+', string)]
  else:
    return []


# Return the most frequent, non-common words
# NOTE: should words be stemmed?  too expensive probably...
STOP_TOKENS = ["this", "that", "shall", "under", "with", "other", "within", "from", "such", "which", "means", "each", "have", "including", "upon", "after", "these", "been", "include", "otherwise", "against", "least", "through", "than", "unless", "does", "either", "whether", "without", "only", "between", "described", "percent", "their", "then", "those", "when", "except", "into", "during", "iii", "where", "would", "they", "itself", "last", "there", "also", "below", "here", "includes", "more", "neither", "being", "both", "cannot", "about", "above", "were", "first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eigth", "ninth", "tenth", "three", "four", "five", "seven", "eight", "nine"]


# return most frequent non-stop/small words as list
def mf_words(pg_txt, n):
  c = Counter([t.lower() for t in tokens(pg_txt) if len(t) > 3])
  for sw in STOP_TOKENS:
    del c[sw]
  return [w[0] for w in c.most_common(n)]


# basic cleaning fn for incoming html string
def basic_html_clean(html_string):
  return re.sub(r'[\x7f-\xff]', '', html_string)


# extracted link resolution
def resolve_extracted_link(link, ref_url, Q_logs):

  # following RFC 1808 <scheme>://<net_loc>/<path>;<params>?<query>#<fragment>

  # first look for '//'- if present urlparse will handle properly
  if re.search(r'//', link) is not None:
    return link

  # look for relative path '/'
  elif re.search(r'^/', link) is not None:
    rup = urlparse.urlsplit(ref_url)
    return rup.scheme + '://' + rup.netloc + link

  # look for clear netloc form- 'xxx.xxx.xxx'
  elif re.search(r'^\w+\.\w+.\w+', link) is not None:
    return '//' + link

  # look for possible netloc form + path - 'xxx.xxx/yyy'
  elif re.search(r'^\w+\.\w+/', link) is not None:
    return '//' + link

  # look for known throw-away forms
  elif re.search(r'^mailto:', link) is not None:
    return None
  
  # NOTE: TO-DO --> run testing, try to think of further catches
  else:

    # log for improvement purposes
    if Q_logs is not None:
      Q_logs.put("LINK PARSE EXCEPTION: %s", (link,))
    
    rup = urlparse.urlsplit(ref_url)
    return rup.scheme + '://' + rup.netloc + '/' + link


# link extractor subfunction
def extract_link_data(html, ref_url, Q_logs=None):
  urls = []
  url_data = []
  
  # look for all a tags
  link_tags = re.findall(r'<a(?:\s[^>]+)?>.*?</a>', html)
  for link_tag in link_tags:
    link = re.search(r'<a [^>]*href="([^"]+)"[^>]*>(.*?)</a>', link_tag)
    if link is not None:

      # try to resolve link
      link_url = resolve_extracted_link(link.group(1), ref_url, Q_logs)
      if link_url is not None:
        urls.append(link_url)

        # url data: (link_text_tokens)
        url_data.append((tokens(link.group(2)),))
  
  return urls, url_data


# Return the average / longest text string
# NOTE: to-do: handle divs specially?  I.e. if at lvl div, text should count... but should end with </div>
# NOTE: to-do: handle <h\n>?  Look through w3 list of tags and handle all...
TEXT_TAGS = ["div", "p", "b", "i", "u", "ul", "li", "table", "th", "tr", "td", "tbody", "thead", "tfoot"]
SKIP_TAGS = ["br", "hr", "img", "link", "meta", "META", "if", "endif"]
def calc_LTS(html, Q_logs=None, debug=False):
  ts = 0
  lts = 0

  # split up into [<...>] [...] [</...>]
  split_html = re.findall(r'<[^>]+>|[^><]+', html)

  # count text sequences
  try:
    lvl = ['top'];
    for t in split_html:
      
      if debug:
        print t
        print lvl
        print ts
      
      if t[0] == '<' and t[1] == '/':
        del lvl[-1]
      elif t[0] == '<':
        try:
          tag = re.match('<\W*(\w+)', t).group(1)
          if tag not in SKIP_TAGS:
            lvl.append(tag)
            ts = ts if lvl[-1] in TEXT_TAGS else 0
        except AttributeError:
          if Q_logs is not None:
            Q_logs.put('HTML PARSE EXCEPTION: Error extracting tag from ' + t)
      elif lvl[-1] in TEXT_TAGS:
        ts += len(re.sub(r'\s', '', t))
        lts = ts if ts > lts else lts
  except:
    if Q_logs is not None:
      Q_logs.put('LTS CALC FATAL ERROR: lts = ' + lts + ', parsing html:\n ' + html)
  return lts


# function for extracting stats that need to be passed on with child links
#
# USED IN: CRAWL NODE
#
# takes: html
#
# outputs:
#   - page_stats = (
#                   #: page_text_len,
#                   #: num_links,
#                   [t]: title_tokens
#                  )

def extract_passed_stats(html, Q_logs=None):
  pt = get_page_text(html)
  try:
    tt = tokens(re.search(r'<title[^>]*>(.*?)</title>', html).group(1))
  except:
    tt = []
  ptl = float(len(pt))
  nl = float(len(re.findall(r'<a\s.*?>', html)))
  return (ptl, nl, tt)
  

# function for extracting page features & appropriately normalizing numerical ones
#
# USED IN: ANALYSIS NODE
#
# takes: 
#   - html
#   - parent_page_stats = (page_text_len, num_links, title_tokens, link_title_tokens)
#
# outputs:
#   - page_features = (
#                       #,[0,1]: rel_page_text_len,
#                       #,[0,1]: rel_num_links,
#                       #,[0,1]: longest_text_sequence,
#                       [t]: most_frequent_tokens,
#                       [t]: title_tokens,
#                       [t]: parent_link_text_tokens,
#                       [t]: parent_title_tokens 
#                     )

def extract_features(html, parent_page_stats, Q_logs=None):
  pt = get_page_text(html)
  
  # first calculate stats/features that depend on html only
  try:
    tt = tokens(re.search(r'<title[^>]*>(.*?)</title>', html).group(1))
  except:
    tt = []
  ptl = float(len(pt))
  nl = float(len(re.findall(r'<a\s.*?>', html)))
  lts = calc_LTS(html, Q_logs)
  mft = mf_words(pt, 20)
  
  # we normalize lts assuming a rough avg of 1000 chars / page in a contract...
  #lts = 

  # next handle those dependent on parent page data (for relative measures)
  # NOTE: assume that if this var is none, dealing with seed pages at beginning of crawl
  if parent_page_stats is not None:
    ref_ptl, ref_nl, ref_tt, ref_ltt = parent_page_stats

    # calculate & package stats / features
    rpt = ptl / ref_ptl
    rnl = nl / ref_nl
  
  # else use default neutral vals- beginning of crawl so doesn't matter much
  else:
    rpt = 1.0
    rnl = 1.0
    ref_ltt = []
    ref_tt = []

  return (rpt, rnl, lts, mft, tt, ref_ltt, ref_tt)
