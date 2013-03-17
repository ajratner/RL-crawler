#!/usr/bin/env python

# individual node-specific params

NODE_ID = 0

#SEED_LIST = []
SEED_LIST = ['http://www.metallicontrax.com', 'http://www.shippingcontractcomp.com', 'http://www.crecomparex.com']

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.57 Safari/537.17"

#DB_VARS = ('54.225.229.185', 'cnode', 'penguin25', 'crawl_node')
DB_VARS = ('localhost', 'cnode', 'penguin25', 'crawl_node') # 'localhost' for node w/ the db

NODE_START_DELAY = 10  # in seconds; allows node to receive messages but does not start actual crawl
