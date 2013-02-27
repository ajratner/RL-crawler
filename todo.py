# NOTE NOTE --> OVERALL TO-DO LIST
# - initial seeding function
# - payload transfer (to db first then in batch to central dispatch node?)
# - testing function that takes sample of all thread page crawls to form picture of time / 
#   efficiency, without requiring every single one to print out
# - crawl for a domain x killed if a bad link in x?  correct this...
# - make some sort of CHECKS system that checks that all pages expected to be crawled were
# - some sort of function to handle transferring e.g. DNS cache data to disk when/if too large
# - way to send extracted urls that do not belong to this node to other node in periodic packet
#   to avoid index muddling/confusion
# - find out what the server footprint of socket is...
# - unified error system... read up on pythonic ways of doing this
# - robots.txt reader / policy system (ask Matt about this...)
# - what happens if a crawl thread calls 'get' but fails before calling 'log'?
# - handle logging/possible re-try of pages that failed to pull... ALSO: detecting whether
#   entire server might be down, putting url back and putting a long wait time in backq_heap
# - implement fingerprinting for deduplication?
# - try using http://publicsuffix.org/list/?
# - ***handle other doc types e.g. pdfs
#  -***How to preserve domain link structure for relative parent/child calcs?
# - DUAL LAYER PERCEPTRON THRESHOLD: have one serially-updating one on analyze node, have
#   another simpler, low-thresh, less-frequently-updated one doing basic screen on crawl node
#   e.g. 'Is there anything here at all??'
# _ ***how to restart/add new features without ruining work of classifier so far
# - --> GENERAL RESTART MODULE
# - ERROR MOD:: need to make sure active coutn Q is handled for all errors...


# NOTE NOTE --> EFFIENCY GAIN TO-DO/CHECK
# - datetime objects: more efficent way to store this (time.time?)
# - clean up empty backq objects (queue & associated table entry)?
# - try a Trie structure instead of a Bloom filter for seen lookup?  Or try just a Bloom of
#   the hostname, followed by a simple list/dict lookup of the relative path? --> NO to this
#   second idea, might as well just use a dict lookup then...
# - compare performance of Bloom filter versus a python Set
# - ***upgrade DNS cache (currently a dict)?
# - ***CHECK OUT EVENT-BASED I/O PROGRAMMING i.e. http://docs.celeryproject.org/en/latest/userguide/concurrency/eventlet.html or http://www.gevent.org/
# - !!!Make sure mysqldb connection on multiple threads is working ok...?


# NOTE --> useful links
# * python global dict / thread safety: http://stackoverflow.com/questions/1312331/using-a-global-dictionary-with-threads-in-python
# http://moo.nac.uci.edu/~hjm/HOWTO_move_data.html

