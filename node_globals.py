# node globals

# CONNECTION / pycurl
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17"
CURLOPT_TIMEOUT = 60


# NODE ID / NETWORK
NODE_NUMBER = 0
NUMBER_OF_NODES = 1


# PAYLOAD DB
DB_VARS = ('localhost', 'root', 'penguin25', 'crawler_test')
DB_PAYLOAD_TABLE = 'payload_table'


# LOGGING MODULE
LOG_REL_PATH = 'logs/log'
DEBUG_MODE = True


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
