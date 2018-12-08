# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
Created on Mon Apr 20 14:12:41 2015
@author: giang nguyen
"""
import math
import sklearn.metrics as sm 
import sys
sys.path.append('/home/giang/work/Magnetic/proficiency-metric-master/python/')
from predeval import *
    
class MetricQuality():
    def __init__(self, y_true, y_score):
        self.y_true  = zero_one(y_true,  0)             # (-1 or 1) to (0 or 1)                        
        # self.y_score = sigmoid_vector(y_score)        # prob in range (0,1)
        # save_vector_to_file(self.y_score, '/home/giang/python/data/vw/predictions.score')        
        self.y_score = y_score                          # y_score = prob(0,1)
        self.y_pred  = zero_one(self.y_score, 0.5)      # prob(0,1) to (0 or 1)        
                       
    def print_metrics(self):       
        print 'y_true =', len(self.y_true),  sum(self.y_true)  #, self.y_true
        print 'y_score=', len(self.y_score), sum(self.y_score) #, self.y_score
        print 'y_pred =', len(self.y_pred),  sum(self.y_pred)  #, self.y_pred
        
        print '\nconfusion matrix:\n', sm.confusion_matrix(self.y_true, self.y_pred )        
        print 'accuracy, precision, recall, F1:\n\t', \
            sm.accuracy_score(  self.y_true, self.y_pred ), \
            sm.precision_score( self.y_true, self.y_pred ), \
            sm.recall_score(    self.y_true, self.y_pred ), \
            sm.f1_score(        self.y_true, self.y_pred )               
        print 'MCC: \t',            sm.matthews_corrcoef( self.y_true, self.y_pred )
        print 'log_loss: \t',       sm.log_loss(          self.y_true, self.y_pred )               
        print 'RMSE: \t', math.sqrt(sm.mean_squared_error(self.y_true, self.y_pred ))
        
        print '\noverpredict=', sum(self.y_score) / sum(self.y_true)            
        print 'ROC_AUC:  \t',  sm.roc_auc_score( self.y_true, self.y_score )
        print 'tvd, tvd_n:\t', tvd( self.y_true, self.y_score ), tvd( self.y_true, self.y_score, True )
        print 'lift: \t', lift_gain(self.y_true, self.y_score )
        
        lq = LiftQuality("vw_wrapper")
        for a, s  in zip(self.y_true, self.y_score):
            lq.add(a, s)
        print '\n', lq, '\n', lq.bestLift()

                    
def lift_gain(y_true, y_score, bucket_size=1000):  
    v = [ t for (s, t) in sorted(zip(y_score, y_true), reverse=True) ]    
    targets = []
    sum_v = 0
    count = 0
    for item in v:
        count += 1
        if item > 0:
            sum_v += item
        if count % bucket_size == 0:
            targets.append([count, sum_v])       
    targets.append([count, sum_v])
    
    total   = sum_v
    base    = (1.0 * total) / count
    optimal = (1.0 - base)  / 2.0
    cph     =  0.5 * total              # Cumulative Percent Hits
    for pair in targets:
        cph += pair[1]                  # sum_v
    cph /= len(targets) * total
    print '\tcount=', count, 'total=', total, 'len(targets)=', len(targets), 'cph=', cph, '\tlift=', 
    
    lift = (cph - 0.5) / optimal
    return lift

# Total Variation Distance of probability measures 
# tvd_normalize: 0=perfect, 1=bad
def tvd(v1, v2, normalize=False):
    x   = normalize_vector(v1)
    y   = normalize_vector(v2)
    tvd = l1_norm(x, y) / 2.0
    if normalize:
        p = sum(v2) / len(v2)
        q = sum(v1) / len(v1)
        r = 1 - ((p + q - abs(p-q)) / 2 )
        tvd /= r
    return tvd

def normalize_vector(v):
    s = sum(v)        
    return [ item / s for item in v ]   
       
# raw score to probability in range (0,1)
def sigmoid_vector(v, normalize=False):
    v_prob = [ 1 / (1 + math.exp(-1.0 * item) ) for item in v ]
    if normalize:
        return normalize_vector(v_prob)
    else:
        return v_prob
        
# Manhattan distance
def l1_norm(v1, v2, normalize=False):         
    v = [ abs(x-y) for x, y in zip(v1, v2) ]
    d = sum(v)
    if normalize:
        p = sum(v2) / len(v2)
        q = sum(v1) / len(v1)
        r = p + q - 2*p*q
        d /= r
    return d

# Euclidean distance
# scipy.spatial.distance.euclidean(v1, v2)
def l2_norm(v1, v2):         
    d = 0
    for i in range(len(v1)):
        d += (v1[i] - v2[i])**2
    return math.sqrt(d)

# cosine_similarity: angle between two vectors, 0:no-similarity; 1:match
# 1 - scipy.spatial.distance.cosine(v1, v2)
def cosine_similarity(v1, v2):                  
    norm_x, dot_xy, norm_y = 0, 0, 0
    for i in range(len(v1)):
        x = v1[i]
        y = v2[i]
        norm_x += x*x    
        norm_y += y*y
        dot_xy += x*y
    return dot_xy / math.sqrt(norm_x * norm_y) 

# Matthews correlation coefficient (MCC) = Pearson Correlation = phi coefficient = Mean Square Contingency Coefficient
# scipy.stats.pearsonr(v1, v2)
# sklearn.metrics.matthews_corrcoef(y_true, y_pred)

def zero_one(v, threshold):
    v_format = [1.0] * len(v)
    for i, item in enumerate(v):
        if item < threshold:
            v_format[i] = 0
    return v_format

def save_vector_to_file(v, filename):
    with open(filename, 'w') as f:
        for item in v:
            f.write(str(item) + '\n') 
            
