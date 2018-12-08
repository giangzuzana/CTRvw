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
Common library
2015.03
@author: giang nguyen
"""

import sys
from dateutil import rrule
sys.path.append('/home/giang/python/')
from gz_utils import *

def undersampling(fn_in, fn_out, rate=10, shuffling=True):
    if rate > 0:    
        ll = []
        negative = rate        
        with open(fn_in) as fi:
            for line in fi:
                splits = line.strip().split('\t')
                if (len(splits) > 1):
                    line = splits[1]
                if len(line) == 0:
                    continue
                
                if line[0] == '1':
                    negative += rate
                    ll.append(line)
                elif negative > 0:
                    negative -= 1
                    ll.append(line)
        if shuffling: 
            random.shuffle(ll)
        open(fn_out, 'w').writelines(ll)
    else:
        strip_shuf_file(fn_in, fn_out, shuffling)
    return fn_out
   
# BTM arguments combinations
def generate_combinations(list_args, list_args_values, s, i):
    if i == len(list_args):
        yield s
    else:
        s_opt = s + ' ' + list_args[i]
        for value in generate_range(list_args_values[i][0], list_args_values[i][1], list_args_values[i][2]):
            s = s_opt + ' ' + str(value)
            for perm in generate_combinations(list_args, list_args_values, s, i+1):
                yield perm
        i += 1
    return

# BTM quadratic features: Cartesian product of all namespaces, test one per time
# list_chars =  ['p', 's', 'o', 'm', 'w', 'k', 'b', 'c', 'z', 'a', 'e', 'y', 'f', 'r']
def generate_quadratic_cartesian(list_chars):
    yield ' '                               # always run one without any -q to compare
    if not list_chars:
        return
    for i, ci in enumerate(list_chars):         
        for j, cj in enumerate(list_chars):     
            if j > i:
                yield '-q ' + ci + cj
    return

# BTM quadratic features in ordering e.g. output from igr.py, test one per time
# list_quad =  ['yw', 'zw', 'yz']
def generate_quadratic_order(list_quad, num):
    yield ' '                               # always run one without any -q to compare
    if not list_quad:
        return
    if num > len(list_quad) or num < 0:
        num = len(list_quad)
    if num == 0:
        return
    for quad in list_quad[:num]:
        yield '-q ' + quad
    return
    
# BTM quadratic features: incremental combinations
# list_quad =  ['bp', 'ps', 'cp', 'ms', 'bm']
def generate_quadratic_incremental(list_quad):
    yield ' '                               # always run one without any -q to compare
    if not list_quad:
        return
    comb = ''
    for quad in list_quad:
        comb += ' -q ' + quad
        yield comb[1:]
    return

# BTM --ignore one namespace per time
# list_ns =  ['p', 's', 'o', 'm', 'w', 'k', 'b', 'c', 'z', 'a', 'e', 'f', 'y', 'r']
def generate_ignore_ns(list_ns, num):
    yield ' '                               # always run one without --ignore to compare
    if not list_ns:
        return        
    if num > len(list_ns) or num < 0:
        num = len(list_ns)
    if num == 0:
        return
    for ns in list_ns[:num]:
        yield '--ignore ' + ns
    return   

# BTM --keep one namespace per time
# list_ns =  ['p', 's', 'o', 'm', 'w', 'k', 'b', 'c', 'z', 'a', 'e', 'f', 'y', 'r']
def generate_keep_ns(list_ns, num):
    yield ' '                               # always run one without --keep to compare
    if not list_ns:
        return            
    if num > len(list_ns) or num < 0:
        num = len(list_ns)
    if num == 0:
        return
    for ns in list_ns[:num]:
        yield '--keep ' + ns
    return   


# BTM add new namespace, one per tine
# list_ns =  ['d', 'v', '1', '2', '3', 't']
def generate_add_ns(list_ns):
    if len(list_ns) == 0:                   # always run one without "--add" to compare
        yield ' ', ' '
        return
    else:
        s = ''
        for ns in list_ns:
            s += '--ignore ' + ns + ' '
        yield ' ', s[:-1]
        for i in range(len(list_ns)):
            s = ''
            for j, ns in enumerate(list_ns):
                if i <>j:
                    s += '--ignore ' + ns + ' '
            yield '--add ' + list_ns[i], s[:-1]          
    return   


# BTM, HRM, IGR: data windows and calendar functions
def delta_time(d_start, d_end, format_str='%Y_%m_%d_%H'):
    ts1 = time.mktime(datetime.strptime(d_start, format_str).timetuple())
    ts2 = time.mktime(datetime.strptime(d_end, format_str).timetuple())
    return ts2-ts1

def generate_calendar_list(data_range, add_hours=0, format_str='%Y_%m_%d_%H'):
    d_start = datetime.strptime(data_range[0], format_str) + timedelta(hours=add_hours)
    d_end   = datetime.strptime(data_range[1], format_str) + timedelta(hours=add_hours)
    for d in rrule.rrule(rrule.HOURLY, dtstart=d_start, until=d_end):
        yield d.strftime(format_str) 
    return
    
def get_real_filenames(log_fn, data_dir, data_range, add_hours=0, format_str='%Y_%m_%d_%H'):
    dr = data_range
    if add_hours > 0:
        dr[0] = (datetime.strptime(data_range[0], format_str) + timedelta(hours=add_hours)).strftime(format_str)
        dr[1] = (datetime.strptime(data_range[1], format_str) + timedelta(hours=add_hours)).strftime(format_str)

    print_log("\nChecking data for [" + dr[0] + " " + dr[1] + "] in " + data_dir, log_fn)
    ok = []
    missing = []
    for item in list(generate_calendar_list(dr)):
        found = False
        for fn in list(list_dir(data_dir)):
            if item in fn:
                found = True;
                ok.append(data_dir + fn)
        if not found:
            missing.append(item)

    if len(missing) > 0:
        msg = ""
        for item in missing:
            msg += "\n\t" + item
        print_log("\n\tMissing files:" + msg, log_fn)

    if ok:
        print_log("\n\tOK, some data is here", log_fn)        
    else:
        print_log("\n\tNo such input data", log_fn)

    # print '\n data_range=', dr,   "\nok=", ok,    "\nmissing files=", missing
    return ok, missing

def check_data_range(data_range, log_fn):
    try:
        dateStart = datetime.strptime(data_range[0], '%Y_%m_%d_%H')
    except ValueError:
        print_log('\nError: Invalid start date format yyyy_mm_dd_hh', log_fn)
        return False

    try:
        dateEnd = datetime.strptime(data_range[1], '%Y_%m_%d_%H')
    except ValueError:
        print_log('\nError: Invalid end date format yyyy_mm_dd_hh', log_fn)
        return False

    if delta_time(data_range[0], data_range[1]) < 0:
        print_log('\nError: End date must be greater or equal start date', log_fn)
        return False
    else:
        return True

def generate_windows(data_dir, data_range, shifting, log_fn):
    if not check_data_range(data_range, log_fn):
        return
    for i in range(shifting[0]):
        h = i * shifting[1]
        cal_list = list(generate_calendar_list(data_range, h))

        ok, missing = get_real_filenames(log_fn, data_dir, data_range, h)
        if len(ok) > 0:
            yield [cal_list[0], cal_list[-1]]
        else:
            yield []
    return

# GENERAL purpose functions
def generate_range(start, end, step):
    while start <= end:
        yield start
        start += step
    return

def list_dir(dir_name, start_filter=('click_'), end_filter=('.vw', '.bz2')):
    if os.path.isdir(dir_name):
        for root, dirs, files in os.walk(dir_name):
            for name in sorted(files):
                if name.startswith(start_filter) and name.endswith(end_filter):
                    yield name
    return

# Human Readable Model (HRM), Information Gain Ratio (IGR): 
# list of "namespace^feature" in one line of impression file (.vw)
# line = user_cookie \t vw_line
def generate_nsf_from_impression(line, separate_weight=False, missing_value='_missing'):
    line = line.strip()
    splits = line.split('|')
    for s in splits[1::]:                               # except the first part 
        s = s.strip()
        if s.count(' ') > 1:                            # in the case e.g. |w IE 8 |
            item_list = s.split(' ')
            for item in item_list[1::]:                 # create items combinations i.e. [w IE, w 8]
                if not separate_weight:
                    yield item_list[0] + ' ' + item
                else:
                    if ':' in item:                     # Jonathan: need to make this work for continuous variables
                        item, weight = item.split(':')  # i.e. [(w IE, 1.0), (w 8, 1.0)]
                        weight = float(weight)
                    else:
                        weight = 1.0
                    yield (s[0] + ' ' + item, weight)
        elif s.count(' ') == 1:                         # case |w IE |                 
            if not separate_weight:
                yield s
            else:
                yield (s, 1.0)
        else:                                           # treat namespaces without features e.g. |k |m  | 
            if not separate_weight:                    
                yield s + ' ' + missing_value           # as separate class 'ns _missing'
    return

def is_1_line(line):
    vw_line = line
    splits = line.split('\t')
    if (len(splits) > 1):
        vw_line = splits[1]       # without user_cookie
    if vw_line[0] == '1':
        return True
    else:
        return False
        
# Jonathan's functions
def get_namespace_dict():
    sys.path.append('cfg/extraction/' if os.getcwd().split('/')[-1] == 'CTRPrediction' else '../cfg/extraction/')
    from udf_vw_adapt import fields
    d = {}
    for name, metadata in fields:
        d[metadata['vw_key']] = name
    
    # the following 3 are for new namespaces that Jon created
    # d['d'] = 'UPDB'
    d['x'] = 'cookie_age'
    d['q'] = 'not_UPDB'
    
    # the following 7 are for [DS-333] Evaluate new features
    d['d'] = 'bid_device_type'
    d['v'] = 'data_provider_id'
    d['1'] = 'keyword_category1'
    d['2'] = 'keyword_category2'
    d['3'] = 'keyword_category3'
    d['t'] = 'day_of_week'
    d['h'] = 'hour_of_day'
    return d

def replace_namespace(key, ndict):
    if key.count(' ') < 1:
        if key in ndict:
            key = ndict[key]
    else:
        key_ns, key_feat = key.split(' ')
        if key_ns in ndict:
            key_ns = ndict[key_ns]
        key = key_ns + ' ' + key_feat
    return key
