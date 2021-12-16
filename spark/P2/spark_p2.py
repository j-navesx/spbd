import pyspark

sc = pyspark.SparkContext('local[*]')

try:
    lines = sc.read.option("header", True).csv("[insert path]")
    lines = lines.filter(lambda l: len(l) > 0).map(lambda l: [l.split(),l])

    counties_airqual = lines.map(lambda l: [(l[0]+l[1], l[25]), l[16]])

    sum_airqual = counties_airqual.reduceByKey(lambda a,b : a+b)

    ranking = sum_airqual.sortBy(lambda l: l[1], ascending=False)
    
    for (state, rank) in ranking.collect():
        print(f"{rank}: {state[1]}")

    sc.stop()
except Exception as e:
    print(e)
    sc.stop()