import numpy as np

# a simple online learning perceptron object:
#
# - binary classification of input datum:
#   * input consists of a vector of either numeric or token array features
#   * individual token-array-to-vector conversions handled for feature
#
# - a dynamic margin for deciding whether to solicit user feedback
#
# (- use of 'passive-aggressive' algorithm scheme?)

class Perceptron:

  def __init__(self):
    self.W = []
    self.token_maps = {}
    self.in_count = 0
    self.true_count = 0
  
  # for handling feature vectors of form e.g. [1.3, 2, ('a', 'b', 'e'), 4]
  def _handle_features_in(self, mixed_features):
    x = []
    for i, f in enumerate(mixed_features):
      if type(f) == float or type(f) == int:
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
          self.W.append([0.0 for j in f])

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


  # primary routine for adding a datum and getting predicted classification back
  def add_and_classify(self, mixed_features):
    
    # log that an input datum was entered
    self.in_count += 1

    # calculate the input doc's score
    x = self._flatten(self._handle_features_in(mixed_features))
    w = self._flatten(self.W)
    score = np.dot(x, w)

    # NOTE: !!! --> switch it to [0,1] unary rather than [-1,1], i.e. tally up based on
    # positives only, don't get flooded with any negative indicators

    # NOTE: count up the occurences of OOVs (new vocab words)- this should effect the
    #       reporting thresh

    # use dynamic threshold to classify & decide whether to wait for input

    
    
