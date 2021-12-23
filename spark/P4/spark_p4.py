from typing import Sequence
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from math import sqrt as math_sqrt
from math import pow as power

spark = SparkSession.builder.master('local[*]').appName('words').getOrCreate()
sc = spark.sparkContext

try:
    # files
    lines_states = sc.textFile('../data/usa_states.csv')
    lines = sc.textFile('../data/epa_hap_daily_summary-small.csv')

    # file mapping
    logRows_states = lines_states.filter( lambda line : len(line) > 0)    \
                    .zipWithIndex() \
                    .filter( lambda x: x[1] > 0) \
                    .map(lambda x: x[0]) \
                    .map( lambda line: line.split(',')) \
                    .map( lambda arr : (arr[1], float(arr[2]), float(arr[3]), \
                                        float(arr[4]), float(arr[5])))
    logRows = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr: ((arr[24], arr[2], arr[1]), (float("{:.3f}".format(float(arr[5]))), float("{:.3f}".format(float(arr[6]))))))

    logRows = logRows.distinct()

    logRows = logRows.map(lambda arr: (arr[0][0],arr[1]))

    final = logRows_states.map(lambda arr: (arr[0], (((arr[1]+arr[2])/2), ((arr[3]+arr[4])/2))))\
                    .join(logRows) \
                    .map(lambda arr: (arr[0], math_sqrt(power( (arr[1][1][0]-arr[1][0][0])*111,2) +power( (arr[1][1][1]-arr[1][0][1])*111,2)))) \
                    .mapValues(lambda v: (v, 1)) \
                    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1])) \
                    .mapValues(lambda v: v[0]/v[1]) \

    
    for k,v in final.take(20):
        print(k,v)

    sc.stop()
    
except Exception as err:
    print(err)
    sc.stop()
