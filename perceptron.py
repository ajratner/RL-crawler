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
    self.W = np.array([])
    self.W_map = []
    self.in_count = 0
    self.true_count = 0
    self.token_maps = {}
  
  # for handling feature vectors of form e.g. [1.3, 2, ('a', 'b', 'e'), 4]
  def _handle_features_in(self, mixed_features):
    
    # initialization conditions
    if len(self.W) == 0:
      self.W = np.array([0.0 for f in mixed_features])
      self.W_map = range(len(mixed_features))

    # map features in & w to new forms
    w = []
    x = []
    j = 0
    for i, f in enumerate(mixed_features):
      
      # handle int / float features
      if type(f) == float or type(f) == int:
        x.append(f)
        w.append(self.W[self.W_map[i]])

        # update mapping for i
        self.W_map[i] = j
        j += 1
      
      # handle token array features
      elif type(f) == tuple or type(f) == tuple:
        
        # convert tokens to vector form & handle new tokens
        if not self.token_maps.has_key(i):
          self.token_maps[i] = {}
        else:
          x += [0.0 for y in range(len(self.token_maps[i]))]
          w += [self.W[self.W_map[i]+k] for k in range(len(self.token_maps[i]))]
        
        # add the tokens in the feature, seen or unseen as of t
        for token in f:
          if self.token_maps[i].has_key(token):
            x[j+self.token_maps[i][token]] = 1.0
          else:
            x.append(1.0)
            w.append(0.0)
            self.token_maps[i][token] = len(self.token_maps[i])

        # update mapping for i
        self.W_map[i] = j
        j += len(self.token_maps[i])

    # return x, update W
    self.W = np.array(w)
    return np.array(x)

  # primary add routine
  def add(self, mixed_features):
    x = self._handle_features_in(mixed_features)
          


