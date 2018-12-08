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
Query Primary Language (QPL) based on keyboard identifications
parquet data
lift values from accept_language (AL), ua_language (UAL) and referer_domain (TLD)

@author: giang nguyen
2015.04 

Usage: spark-submit --master spark://machine_name:7077 spark_qpl.py
"""

import pyspark
import codecs
import operator
import os
from pprint import pprint
from collections  import Counter
from gz_utils import *

def print_result(filename, d):
    lfd = []
    for k, v in d.iteritems():
        lfd.append([ k, v[0][0], v[0][1], v[1][0], v[1][1], v[2][0], v[2][1] ])
    # lfd.sort(key=itemgetter(1, 2), reverse=True)
    lfd.sort(key=lambda r: (r[1], -r[2]))
        
    with codecs.open(filename, 'w', 'utf-8') as f:
        f.write("AL\tlift\tUAL\tlift\tTLD\tlift\tquery\n" )
        for r in lfd:
            f.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % 
                (r[1], str(r[2]), r[3], str(r[4]), r[5], str(r[6]), r[0]) )
    return

def check_input_files(argv):
    file_list = []
    for file in os.listdir(argv.inputDir):
        if file.endswith('.parquet'):
            f = argv.inputDir + file
            file_list.append(f)
            print f
    return file_list
    
def take_TLD(domain):
    if domain is not None and domain != '' and domain != 'None':
        domain = domain.split('.')[-1]
    return domain

def get_fraction(lang, lang_list):    
    frac = 0
    for i in range(len(lang_list)):
        frac += 1/(float(i+1))
        
    n_Q_l_one = 0
    for i, l in enumerate(lang_list):
        if l==lang:
            n_Q_l_one += 1/((i+1)*frac)
            
    return n_Q_l_one   

def check_all_lang_list(row, position):
    r = tuple(row[1])
    al = tuple( elem[position] for elem in r )
    n_Q = len(al) 
    al_list = []                                    # al_list= list([lang_list, count]) = [ll]
    for ll in Counter(al).items():   
        if ll[0] is not None and ll[0] != '' and ll[0] != 'None':
            al_list.append( [ ll[0].split('|'), ll[1] ] )
        else:
            al_list.append( [ ['_none'], ll[1] ] )  # None and empty value as separate class    
    return n_Q, al_list
    
def map_AL_TLD(row, position=0, verbose=False):
    n_Q, al_list = check_all_lang_list(row, position)
    sl = [ set(ll[0]) for ll in al_list ]
    i = set.intersection(*sl) if any(isinstance(elem, set) for elem in sl) else set()    
    lift_Q_l = dict()
    if not i:
        u = set.union(*sl) if any(isinstance(elem, set) for elem in sl) else set()
        for lang in u:
            n_Q_l = sum(get_fraction(lang, ll[0])*ll[1] for ll in al_list)
            lift_Q_l[lang] = n_Q_l/n_Q
    else:
        for lang in i:            
            n_Q_l = sum(ll[0].count(lang)/float(len(ll[0]))*ll[1] for ll in al_list)
            lift_Q_l[lang] = n_Q_l/n_Q
    lang = max(lift_Q_l.iteritems(), key=operator.itemgetter(1))[0]
    if verbose: 
        print 'al_list=', al_list, '\nn_Q=', n_Q
        print 'intersection=', i                  
        print 'lift_Q_l='
        pprint(lift_Q_l)
        print 'check sum lift_Q_l=', sum([lift for lift in lift_Q_l.values()])  
    return [lang, lift_Q_l[lang]]

def map_UAL(row, position=1):
    n_Q, al_list = check_all_lang_list(row, position)
    sl = [ set(ll[0]) for ll in al_list ]
    i = set.intersection(*sl) if any(isinstance(elem, set) for elem in sl) else set()    
    lift_Q_l = dict()
    if not i:
        return ['_none', 0]
    else:
        for lang in i:
            n_Q_l = sum(ll[0].count(lang)/float(len(ll[0]))*ll[1] for ll in al_list)
            lift_Q_l[lang] = n_Q_l/n_Q
        lang = max(lift_Q_l.iteritems(), key=operator.itemgetter(1))[0]
        return [lang, lift_Q_l[lang]]

def map_QPL(row):
    return [ row[0], map_AL_TLD(row, 0), map_UAL(row, 1), map_AL_TLD(row, 2) ]

# row = ( row[0], [[lang, lift], [lang, lift], [lang, lift]] )
def filter_not_en(row):
    al_en  = ['_none', 'en']
    ual_en = ['_none', 'en-us', 'en-US', 'en-gb', 'en-GB', 'en-ca', 'en-CA', 'en-au', 'en', 'us', 'US']
    tld_en = ['_none', 'org', 'com', 'net', 'gov', 'edu', 'us', 'uk']
    not_en_query = (row[1][0][0] not in al_en)  or \
                   (row[1][1][0] not in ual_en) or \
                   (row[1][2][0] not in tld_en)
    return not_en_query
    
def filter_lang(row, lang):
    query_in_lang =(lang in row[1][0][0]) or \
                   (lang in row[1][1][0]) or \
                   (lang in row[1][2][0]) 
    return query_in_lang
    
def main(argv):
    start = time.strftime("%Y%m%d-%H%M%S")      # timestamp
    argv.outputFilename += '_' + start
    argv.logFilename += '_' + start    

    file_list = check_input_files(argv)
    if not file_list:
        return

    scfg = (pyspark.SparkConf()
            .setAppName("QPL lift")
            # .set("spark.akka.frameSize", "100")                     # big collect()?
            # .set("spark.core.connection.ack.wait.timeout", "600")   # wait longer for GC?
            # .set("spark.driver.memory", "7600m")                    # smaller? max=8g-384m  
            # .set("spark.executor.memory", "7600m")                  # smaller? max=8g-384m 
            # .set("spark.storage.memoryFraction", "1")               # better? or default=0.6
            # .set("spark.cores.max", "4")
            # .set("spark.rdd.compress", "true")
            # .set("spark.shuffle.compress", "true")
            )
    # sc = pyspark.SparkContext(conf=scfg, pyFiles=['recipe_counter.py'])
    sc = pyspark.SparkContext(conf=scfg)            
    print_log(scfg.toDebugString(), argv.logFilename)    
    sqc = pyspark.sql.SQLContext(sc)    
    
    rdd_list = []
    for fn in file_list:
        print_log('\ninput data: ' + fn, argv.logFilename)       
        rdd_parquet = sqc.parquetFile(fn)
        rdd_parquet.registerTempTable("rdd_parquet")
        rdd = (sqc.sql("SELECT raw_keyword, accept_language, ua_language, referer_domain FROM rdd_parquet")
                    .map(lambda row: (row.raw_keyword, \
                        [row.accept_language, row.ua_language, take_TLD(row.referer_domain)] ))      )
        rdd_list.append(rdd)
        
    rdd_all = (sc.union(rdd_list)
                    .groupByKey(100)
                    .map(map_QPL) 
                    .map(lambda row: (row[0], row[1:]))     )
    rdd_result = rdd_all.filter(lambda row: filter_not_en(row))
    if argv.language != 'all_none_en':
        rdd_result = rdd_all.filter(lambda row: filter_lang(row, argv.language))
    '''
    for k, v in rdd_result.take(20):
        print k.encode('utf-8', 'ignore')
        for item in v:
            print '\t', item
    rdd_result.saveAsTextFile(argv.outputFilename)    
    '''
    d = dict(rdd_result.collect())             
    sc.stop()
    print_log('\nSpark stop at: ' + run_time(start) + '\n', argv.logFilename)
    print_result(argv.outputFilename, d)
    print_log('\nresult number: ' + str(len(d)), argv.logFilename)      
    print_log('\nlanguage selection: ' + argv.language, argv.logFilename)     
    print_log('\noutput: ' + argv.outputFilename, argv.logFilename)   
    print_log('\nRuntime: ' + run_time(start) + '\n', argv.logFilename)
    return

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Query Primary Language (QPL) new definition (parquet)', epilog='---')
    parser.add_argument("-d", "--dir",
                        default='/home/giang/python/data/parquet/',
                        dest="inputDir", help="input dir", metavar="DIR")
    parser.add_argument("-l", "--language",
                        # default='all_none_en',                                          
                        default='es',                 
                        dest="language", help="language selection, default=all none EN languages", metavar="LANGUAGE")                        
    parser.add_argument("--output",
                        default='./qpl_stats',
                        dest="outputFilename", help="output filename", metavar="FILENAME")
    parser.add_argument("--log",
                        default='./qpl_log',
                        dest="logFilename", help="local log filename", metavar="FILENAME")
    args = parser.parse_args()
    if not os.path.exists(args.inputDir):
        sys.exit('Error - no such input dir')
    main(args)

    '''
    row = ['query_1', [['en', None, None], ['pt|en', None, None], ['es|pt', None, None], ['de|es', None, None], ['es|de', None, None]] ]
    # row = ['query_2', [['en', None, None], ['es|en', None, None], ['de|fr|en', None, None]]]
    # row = ['query_3', [['fr|en', None, None], ['es|fr', None, None], ['de|fr', None, None]]]
    # row = ['query_4', [[None, None, None], [None, None, None], [None, None, None]]]    
    # row = ['query_5', [['fr|en', None, None], [None, None, None], ['', None, None], ['es|fr', None, None], ['de|fr', None, None]]]
    pprint(map_AL_TLD(row, 0, True))  
    '''
