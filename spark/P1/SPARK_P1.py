import pyspark
from operator import add as sum

sc = pyspark.SparkContext('local[*]')
try:
    lines = sc.textFile('../data/epa_hap_daily_summary-small.csv') # Change the name of the file to what you have it named here
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
    rank = 1
    for state,number in MonitorCounts.collect():
        print(f"{rank}. {state}:{number}")
        rank += 1
    sc.stop()
except Exception as err:
    print(err)
    sc.stop()
