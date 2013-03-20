#!/usr/bin/env python

# node globals

# CRAWL: BOUNDS, CAPS, OVERALL CONTROL
MAX_CRAWLED = 100000  # NOTE: per node!
MAX_SEED_DIST = 0  # -1 for no max seed distance
RESTART_DUMP = 'restart_seed_list'


# PAGE TYPE PARAMS
# http://www.fileinfo.com/filetypes/web
# note certain document types are included, to be handled specially by crawl_page
SAFE_PATH_RGX = r'\.((x|p|r|s)?htm?l?|php\d?|asp|cfml?|pdf|docx?|rtf|txt)/?$|^[^\.]*$'
DOC_PATH_RGX = r'\.(pdf|docx?|rtf|txt)/?$'


# CONNECTION / pycurl
CURLOPT_TIMEOUT = 60


# THREADS / NODES
NUMBER_OF_CTHREADS = 2
NUMBER_OF_MTHREADS = 1
NUMBER_OF_NODES = 3
NODE_ADDRESSES = ['54.225.229.185', '54.225.201.124', '54.225.201.136']
# NOTE: ordered!
DISTR_ON_FULL_URL = True


# INTER-NODE MESSAGING DETAILED PARAMS
DEFAULT_IN_PORT = 8081
DEFAULT_OUT_PORT = 0
MSG_BUF_SIZE = 1024


# NODE CONTROL PARAMS
DB_NODE_ACTIVITY_TABLE = 'activity_monitor'
ACTIVITY_CHECK_P = 60


# PAYLOAD DB
DB_PAYLOAD_TABLE = 'payload_table'
DB_POSITIVES_TABLE = 'positives_table'


# LOGGING MODULE
LOG_REL_PATH = 'logs/log'
DEBUG_MODE = False


# DNS CACHE
DNS_REFRESH_TIME = 21600  # Refresh DNS every 6 hours


# POLITENESS
BASE_PULL_DELAY = 60  # Base time constant to wait for pulling from domain = 60 secs


# SEEN (BLOOM) FILTER
BF_CAPACITY = 10000000
BF_ERROR_RATE = 0.001
BF_FILENAME = 'seen.bloom'


# CRAWL NODE QUEUE CONSTANTS
HQ_TO_THREAD_RATIO = 3
MAX_QUEUE_SIZE = 10000


# CRAWL NODE Q FLOW
OVERFLOW_TRY_MAX = 10
OVERFLOW_MULTI = True  # mthreads try to insert multiple from overflow to new hq
                       # (applicable esp. if number of hosts is low)
OVERFLOW_MULTI_TRY_L = 1


# ANALYSIS NODE INTERFACE & BATCH TESTING SCRIPT
DB_BATCH_TEST_TABLE = 'batch_test'
FILL_BATCH_TEST = False


# BINARY RELEVANCE CLASSIFIER
AGGRESSIVE_PARAM = 1
FEEDBACK_THRESH = True
