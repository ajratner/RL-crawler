#!/usr/bin/env python

import numpy as np
from Queue import Queue
import time
from node_globals import *
from node_locals import *

# a simple online learning perceptron object:
#
# - binary classification of input datum:
#   * input consists of a vector of either numeric or token array features
#   * individual token-array-to-vector conversions handled for feature
#
# - a dynamic margin for deciding whether to solicit user feedback
#
# (- use of 'passive-aggressive' algorithm scheme?)
#
# * not necessarily thread safe


class OLClassifier:

  def __init__(self):
    self.W = []
    self.token_maps = {}
    self.in_count = 0
    self.true_count = 0
    self.last_x = []

  
  # for handling feature vectors of form e.g. [1.3, 2, ('a', 'b', 'e'), 4]
  def _handle_features_in(self, mixed_features):
    x = []
    for i, f in enumerate(mixed_features):
      if type(f) == float or type(f) == int or type(f) == np.float64:
        x.append(f)

        # handle initial conditions
        if len(self.W) <= i:
          self.W.append(0.0)
      
      elif type(f) == list or type(f) == tuple:
        
        # map the tokens to vector form via the token maps
        if not self.token_maps.has_key(i):
          self.token_maps[i] = {}
        x.append([0.0 for j in self.token_maps[i]])
        for token in f:
          if self.token_maps[i].has_key(token):
            x[i][self.token_maps[i][token]] = 1.0
          else:
            x[i].append(1.0)
            self.token_maps[i][token] = len(self.token_maps[i])

        # append whole new row to W if initial conditions
        if len(self.W) <= i:
          self.W.append([0.0 for j in range(len(self.token_maps[i]))])

        # else append existing row accordingly
        else:
          self.W[i] += [0.0 for j in range(len(self.token_maps[i]) - len(self.W[i]))]

    # return x np array, flattened and normalized
    x = self._flatten(x)
    return x/np.linalg.norm(x)

  
  # subfunction for flattening mixed array
  def _flatten(self, mixed_array):
    array_out = []
    for x in mixed_array:
      if type(x) == list:
        array_out += x
      elif type(x) == tuple:
        array_out += list(x)
      else:
        array_out.append(x)
    return np.array(array_out)


  # subfuntion for un-flattening mixed array according to prototype
  def _unflatten(self, flat_array, proto_array):
    array_out = []
    i = 0
    for r in proto_array:
      if type(r) == list or type(r) == tuple:
        array_out.append([flat_array[i+j] for j in range(len(r))])
        i += len(r)
      else:
        array_out.append(flat_array[i])
        i += 1
    return array_out

  
  # a function for displaying an entered features list in flat readbale format with weights
  def readable_weights(self, mixed_features):
    w_relevant = [x for x in self._flatten(self.W) if x > 0]
    x_readable = []
    for i, f in enumerate(mixed_features):
      if type(f) == float or type(f) == int:
        x_readable.append(f)
      elif type(f) == list or type(f) == tuple:
        x_readable += [x[1] for x in sorted([(self.token_maps[i][t], t) for t in f])]
    return x_readable, w_relevant


# NOTE: TO-DO: !!! --> switch it to [0,1] unary rather than [-1,1], i.e. tally up based on
# positives only, don't get flooded with any negative indicators

# NOTE: TO-DO: count up the occurences of OOVs (new vocab words)- this should effect the
#       reporting thresh

# NOTE: TO-DO: use dynamic threshold to classify & decide whether to wait for input


# NOTE: IDEA --> some sort of constant for each individual parameter that captures its informational value...

  # primary routine for adding a datum and getting predicted classification back
  def classify(self, mixed_features):
    
    # block until all params updated from last data point
    while self.in_count != 0:
      time.sleep(1)
    
    # log that an input datum was entered
    self.in_count += 1

    # calculate the input doc's score
    x = self._flatten(self._handle_features_in(mixed_features))
    w = self._flatten(self.W)
    score = np.dot(x, w)
    self.last_x = x

    return score


  # use passive-aggressive algorithm-I from Crammer et. al. '06
  def feedback(self, true_class):
    tc = int(true_class)

    # update params according to feedback using simple perceptron update
    x = self.last_x
    w = self._flatten(self.W)
    loss = max(0.0, 1 - tc*np.dot(w, x))
    w += min(AGGRESSIVE_PARAM, loss/sum(x**2))*tc*x
    self.W = self._unflatten(w, self.W)

    # log that an input datum was completed with feedback returned
    self.in_count -= 1

    return loss


  def skip_feedback(self):

    # log that an input datum was completed with feedback returned
    self.in_count -= 1

    
    
