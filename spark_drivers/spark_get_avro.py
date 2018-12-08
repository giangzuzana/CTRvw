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
Created 2015
@author: giangnguyen
"""

import pyspark
import sys

if __name__ == "__main__":

    print >> sys.stderr, """
        Usage: run with example jar:
        spark-submit --jars
        /opt/cloudera/parcels/CDH/jars/avro-1.7.6-cdh5.3.1.jar,
        /opt/cloudera/parcels/CDH/jars/avro-mapred-1.7.6-cdh5.3.1-hadoop2.jar,
        /opt/cloudera/parcels/CDH/jars/spark-examples-1.2.0-cdh5.3.1-hadoop2.5.0-cdh5.3.1.jar
        spark_get_avro.py
        """

    print '\n', len(sys.argv), sys.argv, '\n'
    sc = pyspark.SparkContext()             # not necessary in an interactive session
    sqc = pyspark.sql.SQLContext(sc)
    '''
    conf = None
    if len(sys.argv) == 1:
        # path = '/user/jobs/scored/2015/02/02/02/EVENTS_SCORED/part-m-00000.avro'
    elif len(sys.argv) == 2:
        path = sys.argv[1]
    elif len(sys.argv) == 3:
        schema_rdd = sc.textFile(sys.argv[2], 1).collect()
        conf = {"avro.schema.input.key": reduce(lambda x, y: x + y, schema_rdd)}
    '''
    path = 'users.avro'
    schema_file = 'user.avsc'
    schema_rdd = sc.textFile(schema_file, 1).collect()
    conf = None
    conf = {"avro.schema.input.key": reduce(lambda x, y: x + y, schema_rdd)}
    print '\n', conf, '\n'

    avro_rdd = sc.newAPIHadoopFile(
        path,
        "org.apache.avro.mapreduce.AvroKeyInputFormat",
        "org.apache.avro.mapred.AvroKey",
        "org.apache.hadoop.io.NullWritable",
        keyConverter="org.apache.spark.examples.pythonconverters.AvroWrapperToJavaConverter",
        conf=conf)
    data = avro_rdd.map(lambda x: x[0]).collect()
    print data.count()

    '''
    lines = 0
    for k in output:
        lines += 1
        print k
        if lines >= 3:
            sys.exit()
    '''
    sc.stop()

