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
Created on Thu Apr  9 09:21:26 2015
@author: giang nguyen
"""

import bz2
import logging
import numpy as np
import os
import random
import shutil
import sys
import time
from datetime import datetime, timedelta
#from scipy.stats import nanmean

def query_yes_no(question, default="yes"):
    valid = {"yes": True, "y": True, "no": False, "n": False}
    while True:
        sys.stdout.write(question + ' [Y/n] ')
        choice = raw_input().lower()
        if choice in valid:
            return valid[choice]
        else:
            print "PY: Please respond with 'yes' or 'no' or 'y' or 'n' \n"
    return

def print_log(msg, log_fn):
    print msg,
    with open(log_fn, 'a') as f:
        f.write(msg)
  
def run_time(start, format_str='%Y%m%d-%H%M%S'):
    diff = time.mktime(datetime.strptime(time.strftime(format_str), format_str).timetuple()) - \
           time.mktime(datetime.strptime(start, format_str).timetuple())
    return timedelta(seconds=diff)


def file_open_bz2(fn):
    if fn.endswith('.bz2'):
        fd = bz2.BZ2File(fn)
    else:
        fd = open(fn)
    return fd    

def rm_dir(dir_name):
    if os.path.isdir(dir_name):
        shutil.rmtree(dir_name)

def rm_file(filename):
    try:
        os.remove(filename)
    except OSError:
        pass 

def strip_file(fn_in, fn_out):
    with open(fn_in) as fi, open(fn_out, 'w') as fo:
        for line in fi:
            line = line.strip()
            if len(line) > 0:
                fo.write(line + '\n')
    return fn_out    
    
def shuf_file(fin, fout):
    lines = open(fin).readlines()
    random.shuffle(lines)            
    open(fout, 'w').writelines(lines)

def strip_shuf_file(fn_in, fn_out, shuffling=True):
    lines = open(fn_in).readlines()
    if shuffling:    
        random.shuffle(lines)        
    with open(fn_out, 'w') as fo:
        for line in lines:
            line = line.strip()
            if len(line) > 0:
                fo.write(line + '\n')
    return fn_out    
  
def add_averages(result_fn):
    if not os.path.isfile(result_fn):
        return
    num_lines = sum(1 for line in open(result_fn))
    if num_lines > 2:
        data = np.genfromtxt(result_fn, skip_header=1, delimiter='\t')
        with open(result_fn, 'a') as fw:        
            fw.write('\t'.join(map(str, data.mean(0))) + '\n')
    return


class GzLog():    
    def __init__(self, name='', filename='out/gz_log.txt', log_level=logging.DEBUG):
        if len(name) == 0:
            self.logger = None
        else:
            self.logger = logging.getLogger(name)
            self.logger.setLevel(log_level)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            
            if len(filename) != 0:  
                try:
                    os.remove(filename)
                except OSError:
                    pass                    
                fh = logging.FileHandler(filename)
                fh.setLevel(log_level)
                fh.setFormatter(formatter)
                self.logger.addHandler(fh)
            
            ch = logging.StreamHandler()
            ch.setLevel(log_level)
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)
        
    def setLevel(self, level=logging.INFO):
        self.logger.setLevel(level)

    def msg(self, s, msg_type='info'):
        if isinstance(self.logger,logging.Logger):
            if msg_type == 'warn':
                self.logger.warn(s)
            elif msg_type == 'debug':
                self.logger.debug(s)
            elif msg_type == 'error':
                self.logger.error(s)
            elif msg_type == 'critical':
                self.logger.critical(s)
            else:
                self.logger.info(s)
        else:
            print 'INFO ' + s               
'''        
# application code
l1 = GzLog('log1')
l1.msg('info message', 'warn')      # info | warn | error | critical
l2 = GzLog()
l2.msg('message')
'''
