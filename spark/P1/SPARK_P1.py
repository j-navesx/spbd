import pyspark
from operator import add as sum

sc = pyspark.SparkContext('local[*]')
try:
    lines = sc.textFile('log.csv') # Change the name of the file to what you have it named here
    logTuples = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr: ( (arr[0], arr[1], arr[2]), arr[24]) )                    
    DistinctTuples = logTuples.distinct()
    MonitorCounts = DistinctTuples.map( lambda x: (x[1], 1) ) \
                                  .reduceByKey(sum) \
                                  .sortBy( lambda x: x[1], ascending = False)
    for row in MonitorCounts.collect():
        print(row)
    sc.stop()
except Exception as err:
    print(err)
    sc.stop()
