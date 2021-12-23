import pyspark

sc = pyspark.SparkContext('local[*]')
try:
    lines = sc.textFile('../data/epa_hap_daily_summary-small.csv') # Change the name of the file to what you have it named here
    logTuples = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr: ((arr[11][:4], arr[24]), float(arr[16])))
    stateRanks = logTuples.mapValues( lambda v: (v, 1)) \
                          .reduceByKey( lambda a,b: (a[0]+b[0], a[1]+b[1])) \
                          .mapValues( lambda v: v[0]/v[1]) \
                          .sortBy(lambda x: (x[1]), ascending=False) \
                          .sortBy(lambda x: (x[0][0]))
                          
    for year_state, value in stateRanks.take(100):
        year, state = year_state
        print(year, state, value)
    sc.stop()
except Exception as err:
    print(err)
    sc.stop()