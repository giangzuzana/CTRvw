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
Created on Fri Apr 17 12:11:38 2015
@author: giang nguyen
"""

dir_data_train = 'data/vw/train/'            # dir_data_calib = dir_data_train
dir_data_test  = 'data/vw/test/'
dir_tmp = 'data/vw/tmp/'

# Variable names 
train_range = ['2015_02_01_23', '2015_02_01_23']      
test_range  = ['2015_02_11_23', '2015_02_11_23']

# Data windows for train and test data
# shifting = [shifting number, step in hours] e.g. [30moves , 24h] = [30, 24]
shifting = [1, 24]

# Undersampling rate = [start, end, step]
sampling = [10, 10, 1]

# VW options:
# vw_args = ['-b', '-l', '--passes', '--l1', '--l2', '--nn']
vw_args = ['-b', '-l', '--passes']
vw_args_values = [                  # value = [start, end, step]
    [24, 24, 1],                    # [18, 32, 1] ] 
    [0.5, 0.5, 0.1],                # [0.01, 0.6, 0.05],
    [10, 10, 1]                     # [10, 200, 10],
    # [1e-8, 1e-7, 1e-8]              # small values for --l1 2e-8
    # [1e-7, 1e-6, 1e-7]              # small values for --l2 2e-7    
    # [1, 30, 1]                      # [1, 100, 1]
] 
                                  
# VW add one feature from list, ignore the rest
# here is always one cycle (more) without "--add" to compare            
# vw_namespaces = ['p', 's', 'o', 'm', 'w', 'k', 'b', 'c', 'z', 'a', 'e', 'f', 'y', 'r']
# vw_namespaces = ['d', 'v', '1', '2', '3', 't', 'h']
vw_namespaces = []                  # empty=turn off add/keep/ignore
                    
# VW --keep option:                 # take one --keep namespace per one time
# here is always one cycle (more) without any --keep to compare            
vw_keep_num = 0                     # 0=turn off keep; -1=consider whole but one per time 

# VW --ignore option:               # take one --ignore namespace per one time
# here is always one cycle (more) without any --ignore to compare            
vw_ignore_num = 0                   # 0=turn off ignore; -1=consider whole but one per time 

# VW quadratic option: take one quadratic combination per one time
# here is always one cycle (more) without any quadratic to compare
# 1. approach: Cartesian combination
# vw_namespaces_quadratic =  ['b', 'p', 's']
vw_namespaces_quadratic = []        # empty=turn off 1. approach

# if 1. approach is off, btm.py will consider 2. and 3. approach
# 2. approach: ordering list, one per time
# 3. approach: incremental combination from ordering list
# vw_quadratic  = ['bp', 'ps', 'cp', 'ms', 'bm', 'dy', 'cm', 'pr', 'mp', 'pt' ]
vw_quadratic  = []                  # empty=turn off 2. and 3. approach
vw_quadratic_num = 0                # 0=turn off 2. approach + run 3. approach; 
                                    # -1=consider whole but one per time for 2. approach
# Auxiliary
ask_to_continue = False             # wait and ask to continue after generation all combination
