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
from gz_utils import *
from gz_stats import *
from vw_cfg import *
from vw_lib import *
      
def vw_train(log_fn, data_range, sample=0, 
             opt_arg='', opt_add='', opt_keep='', opt_ignore='', opt_quad=''):
    start  = time.strftime("%Y%m%d-%H%M%S")    
    r_code = []   
    rm_dir( dir_tmp )
    os.makedirs( dir_tmp )       
    ok, missing = get_real_filenames(log_fn, dir_data_train, data_range)
    for i, fn in enumerate(ok):  
        data_train = undersampling(fn, dir_tmp + 'data_train.vw', sample)
        
        cmd  = ' vw --cache --kill_cache --loss_function logistic' 
        if i==0:
            cmd += ' --link logistic'
        if os.path.isfile(dir_tmp + 'regressor.final'):
            os.rename(dir_tmp + 'regressor.final', dir_tmp + 'regressor.initial')
            cmd += ' --initial_regressor ' + dir_tmp + 'regressor.initial'
        cmd += ' --final_regressor ' + dir_tmp + 'regressor.final'    
        cmd += opt_arg  +' '+ opt_add[1] +' '+ opt_keep +' '+ opt_ignore +' '+ opt_quad
        cmd += ' --data ' + data_train
        print_log('\n***** vw train *****\n' + cmd + '\n', log_fn)           

        r_code.append( subprocess.call( shlex.split(cmd) ))
        
    print_log('\nvw_train return=' + str(r_code) + ' after ' + str(run_time(start)) + 's\n', log_fn)
    return sum(r_code)

def vw_test(log_fn, data_range):
    start = time.strftime("%Y%m%d-%H%M%S")   
    r_code  = []
    y_true  = []
    y_score = []
    ok, missing = get_real_filenames(log_fn, dir_data_test, data_range, 0)    
    for fn in ok:       
        data_test = strip_file(fn, dir_tmp + 'data_test.vw')        

        cmd  = ' vw '
        cmd += ' --testonly --initial_regressor ' + dir_tmp + 'regressor.final'		 
        cmd += ' --predictions '                  + dir_tmp + 'predictions.txt'
        cmd += ' --data ' + data_test
        print_log('\n***** vw test *****\n' + cmd + '\n', log_fn)          
        
        r_code.append( subprocess.call( shlex.split(cmd) ))       
        y_true  = np.concatenate( (y_true,  np.loadtxt( data_test, delimiter=' ', usecols=[0]   )) )
        y_score = np.concatenate( (y_score, np.loadtxt( dir_tmp + 'predictions.txt'             )) )
        
    vw_mq = MetricQuality(y_true, y_score)
    vw_mq.print_metrics()    
    
    print_log('\nvw_test return=' + str(r_code) + ' after ' + str(run_time(start)) + 's\n', log_fn)     
    return sum(r_code)

def main(argv):
    start = time.strftime("%Y%m%d-%H%M%S")          # timestamp
    log_fn    = argv.logFilename    + '_' + start   # log file
    report_fn = argv.reportFilename + '_' + start   # result file

    # Generate data windows for sampled, calibration and complete data
    train = list(generate_windows(dir_data_train, train_range, shifting, log_fn))
    test  = list(generate_windows(dir_data_test,  test_range,  shifting, log_fn))
    if not train or not test:
        return       
    
    print_log("\nChecking data windows: ", log_fn)
    win_num = 0
    for i in range(len(train)):
        if train[i] and test[i]:
            win_num +=1
            msg = '\n\tData windows ' + str(win_num) + ': '
            msg += 'train=[' + ' '.join(train[i]) + '] test=[' + ' '.join(test[i]) + ']'
            print_log(msg, log_fn)                
    if win_num == 0:
        print_log('\n\tNo such data windows for train and test data !!! \n', log_fn)
        return

    # Generate undersampling rates
    sample_list = list(generate_range(sampling[0], sampling[1], sampling[2]))
    msg = '\nSampling: ' + str(len(sample_list)) + ':\t' + ' '.join(str(item) for item in sample_list)
    print_log('\n' + msg + '\n', log_fn)
      
    # Generate VW options
    opt_arg_list = list(generate_combinations(vw_args, vw_args_values, '', 0))
    msg = '\nVW arguments: ' + str(len(opt_arg_list)) + '\n\t' + '\n\t'.join(opt_arg_list)
    print_log(msg, log_fn)
        
    add_ns_list = list(generate_add_ns(vw_namespaces))
    msg = '\nVW add namespaces: ' + str(len(add_ns_list))
    for item in add_ns_list:
        msg += '\n\t' + item[0] + '\t' + item[1]
    print_log(msg, log_fn)
    
    keep_ns_list = list(generate_keep_ns(vw_namespaces, vw_keep_num))
    msg = '\nVW keep namespaces: ' + str(len(keep_ns_list)) + '\n\t' + '\n\t'.join(keep_ns_list)
    print_log(msg, log_fn)
    
    ignore_ns_list = list(generate_ignore_ns(vw_namespaces, vw_ignore_num))
    msg = '\nVW ignore namespaces: ' + str(len(ignore_ns_list)) + '\n\t' + '\n\t'.join(ignore_ns_list)
    print_log(msg, log_fn)
    
    # Generate VW quadratic combinations (3 approaches)  
    if vw_namespaces_quadratic:
        quadratic_comb = list(generate_quadratic_cartesian(vw_namespaces_quadratic))
    elif vw_quadratic_num != 0:
        quadratic_comb = list(generate_quadratic_order(vw_quadratic, vw_quadratic_num))
    else:
        quadratic_comb = list(generate_quadratic_incremental(vw_quadratic))
        
    msg = '\nVW quadratic combinations: ' + str(len(quadratic_comb)) + '\n\t' + '\n\t'.join(quadratic_comb)
    print_log(msg, log_fn)    
    
    # Number of combinations
    n = win_num * len(sample_list) * len(opt_arg_list) * \
        len(add_ns_list) * len(keep_ns_list) * len(ignore_ns_list) * len(quadratic_comb)
    print_log('\n' + str(n) + ' combinations are generated!!! \n', log_fn)
    if n <= 0:
        return
    
    if ask_to_continue:
        if not query_yes_no("Do you want to continue?"):
            print_log("\nExit without parametric study " + run_time(start) + "\tsince " + start + "\n\n", log_fn)
            return
    
    for i in range(len(train)):
        if not train[i] or not test[i]:
            continue
        for sample in sample_list:
            for opt_arg in opt_arg_list:
                for opt_add in add_ns_list:
                    for opt_keep in keep_ns_list:
                        for opt_ignore in ignore_ns_list:
                            for opt_quad in quadratic_comb:
                                
                                train_code = vw_train(log_fn, train[i], sample, 
                                                      opt_arg, opt_add, opt_keep, opt_ignore, opt_quad)
                                if train_code == 0:                                
                                    test_code = vw_test(log_fn, test[i])

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
