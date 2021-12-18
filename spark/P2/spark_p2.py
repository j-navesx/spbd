import pyspark

sc = pyspark.SparkContext('local[*]')

try:
    lines = sc.textFile('log.csv') # Change the name of the file to what you have it named here
    logTuples = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(','))
    
    counties_airqual = logTuples.map(lambda l: [(l[0]+l[1], l[25]), float(l[16])])

    sum_airqual = counties_airqual.reduceByKey(lambda a,b : a+b)

    ranking = sum_airqual.sortBy(lambda l: l[1], ascending=False)
    
    rank = 1
    for (state, value) in ranking.collect():
        print(f"{rank}: {state[1]}, {value}")
        rank +=1

    sc.stop()
except Exception as e:
    print(e)
    sc.stop()