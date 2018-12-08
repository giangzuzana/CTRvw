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
Created on Thu Apr 16 15:52:46 2015
@author: giang nguyen
"""

import shlex
import subprocess
import sys
sys.path.append('/home/giang/python/')
from gz_utils import *
from gz_stats import *
from vw_lib   import *
    
def vw_train(log_fn, data_train):
    print_log('\n***** vw train *****', log_fn)
    rm_dir(dir_tmp)
    os.makedirs(dir_tmp)    

    cmd  = ' vw --cache --kill_cache --loss_function logistic --link logistic' 
    cmd += ' --bit_precision 24 --learning_rate 0.5 --passes 10 '     
    cmd += ' --data ' + data_train
    cmd += ' --final_regressor ' + dir_tmp  + 'regressor.final'    
    print_log('\n'+ cmd, log_fn)    
        
    start = time.strftime("%Y%m%d-%H%M%S")
    args  = shlex.split(cmd)
    return_code = subprocess.call(args)
    print_log('\nvw_train return=' + str(return_code) + ' after ' + str(run_time(start)) + '\n', log_fn)
    return return_code

def vw_test(log_fn, data_test):
    print_log('\n***** vw test *****', log_fn)
    
    cmd  = ' vw '
    cmd += ' --data ' + data_test
    cmd += ' --testonly --initial_regressor ' + dir_tmp + 'regressor.final'		 
    cmd += ' --predictions '                  + dir_tmp + 'predictions.txt'
    print_log('\n'+ cmd, log_fn)    
    
    start = time.strftime("%Y%m%d-%H%M%S")
    args  = shlex.split(cmd)    
    return_code = subprocess.call(args)
    print_log('\nvw_test return=' + str(return_code) + ' after ' + str(run_time(start)) + '\n', log_fn)   
    return return_code

def vw_eval(data_test, data_pred):
    y_true  = np.loadtxt( data_test, delimiter=' ', usecols=[0] )
    y_score = np.loadtxt( data_pred )     
    lq = MetricQuality(y_true, y_score)
    lq.print_metrics()
    return

def main(argv):
    start  = time.strftime("%Y%m%d-%H%M%S")      # timestamp
    log_fn = argv.logFilename + '_' + start      # log file
    
    data_train = 'data/vw/train/names.train'
    data_test  = 'data/vw/test/names.test'
    dir_tmp = 'data/vw/tmp/'

    if vw_train(log_fn, data_train) == 0:                                
        if vw_test(log_fn, data_test) == 0:
            vw_eval(data_test, dir_tmp + 'predictions.txt')
                            
    print_log('\nRuntime: ' + str(run_time(start)) + '\n', log_fn)            
    return
    
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='VW parametric study. ' +
                    'Run vw_wrapper.py after configuring input parameters in vw_cfg.py file', epilog='---')
    parser.add_argument("-o", "--output",
                        default='out/vw_tt.stats',
                        dest="reportFilename", help="report output filename", metavar="FILENAME")
    parser.add_argument("-l", "--log",
                        default='out/vw_tt.log',
                        dest="logFilename", help="log filename", metavar="FILENAME")
    args = parser.parse_args()
    main(args)
