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
# giang nguyen
# 2015.04 
# Usage to see app in GUI: spark-submit --master spark://machine_name:7077 spark_demo.py
#

import pyspark
import argparse
import operator
import time
from random import random
from gz_utils import *

def f(_):
    x = random()*2 - 1
    y = random()*2 - 1
    return 1 if x**2 + y**2 < 1 else 0
        
def count_PI(sc, num_samples=1000000, partitions=4):
    n = num_samples * partitions
    count = ( sc.parallelize(xrange(1, n+1), partitions)
                .map(f)
                .reduce(operator.add)   )
    print "Pi is roughly %f" % (4.0 * count / n)
    return

def read_text(sc, filename):
    rdd = sc.textFile(filename)
    rdd_one = rdd.filter(lambda line: is_1_line(line))
    print 'lines=%d one_lines=%d' % (rdd.count(), rdd_one.count())
    return

def read_parquet(sc, filename):
    sqc = pyspark.sql.SQLContext(sc)
    rdd = (sqc.parquetFile(filename)
              .map(lambda row: (row.raw_keyword, 1))
              .reduceByKey(operator.add)    )
    # rdd.saveAsTextFile(argv.outputFilename)
    # ret = dict(rdd.collect())
    # print ret    
    for k, v in rdd.take(20):
        print v, '\t', k.encode('utf-8', 'ignore')
    return
       
def main(argv):
    start = time.strftime("%Y%m%d-%H%M%S")      
    argv.outputFilename += '_' + start
    argv.logFilename    += '_' + start
    
    scfg = (pyspark.SparkConf()
            .setAppName("GZ-spark-01")
            # .set("spark.akka.frameSize", "100")                     # big collect()?
            # .set("spark.core.connection.ack.wait.timeout", "600")   # wait longer for GC?
            # .set("spark.driver.memory", "7600m")                    # smaller? max=8g-384m  
            # .set("spark.executor.memory", "7600m")                  # smaller? max=8g-384m 
            # .set("spark.storage.memoryFraction", "1")               # better? or default=0.6
            # .set("spark.cores.max", "4")
            # .set("spark.rdd.compress", "true")
            # .set("spark.shuffle.compress", "true")
            )
    sc = pyspark.SparkContext(conf=scfg)
    # read_text(sc, '/home/giang/python/data/vw/sample_100.vw')
    read_parquet(sc, '/home/giang/python/data/parquet/part-m-00000.parquet') 
    # count_PI(sc)
    
    sc.stop()
    return

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='GZ-spark test', epilog='---')
    parser.add_argument("--output",
                        default='./gz_out',
                        dest="outputFilename", help="output filename", metavar="FILENAME")
    parser.add_argument("--log",
                        default='./gz_log',
                        dest="logFilename", help="local log filename", metavar="FILENAME")
    args = parser.parse_args()
    main(args)
