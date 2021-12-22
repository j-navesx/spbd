import pyspark

sc = pyspark.SparkContext('local[*]')

try:
    lines = sc.textFile('../data/epa_hap_daily_summary-small.csv') # Change the name of the file to what you have it named here
    logTuples = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(','))
    
    counties_airqual = logTuples.map(lambda l: [(l[0]+l[1], l[25]), float(l[16])])
    airqual = counties_airqual.mapValues(lambda v: (v, 1))\
                    .reduceByKey(lambda a,b: (a[0]+b[0], a[1]+b[1]))\
                    .mapValues(lambda v: v[0]/v[1])


    ranking = airqual.sortBy(lambda l: l[1], ascending=False)
    
    rank = 1
    for (state, value) in ranking.collect():
        print(f"{rank}: {state[1]}, {value}")
        rank +=1

    sc.stop()
except Exception as e:
    print(e)
    sc.stop()