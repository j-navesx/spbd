from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('words').getOrCreate()
sc = spark.sparkContext

try:
    # files
    lines_states = sc.textFile('usa_states.csv')
    lines = sc.textFile('log.csv')

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
                     .map( lambda arr: (arr[24], (float(arr[5]), float(arr[6]))))

    logRows = logRows.distinct()

    final = logRows_states.map(lambda arr: (arr[0], (((arr[1]+arr[2])/2), ((arr[3]+arr[4])/2))))\
                    .join(logRows) \
                    .map(lambda arr: (arr[0], sqrt( pow((arr[1][1][0]-arr[1][0][0])*111,2) + pow((arr[1][1][1]-arr[1][0][1])*111,2) ))) \
                    .groupByKey() \
                    .map(lambda arr: (arr[0], sum(arr[1])/len(arr[1])))

    
    for k,v in final.take(20):
        print(k,v)
    

    # finalDF = logRowsStatesDF.withColumn('center_Lat', (col('minLat')+col('maxLat'))/2 ) \
    #                     .withColumn('center_Lon', (col('minLon')+col('maxLon'))/2 ) \
    #                     .drop('minLon') \
    #                     .drop('maxLon') \
    #                     .drop('minLat') \
    #                     .drop('maxLat') \
    #                     .drop('state') \
    #                     .join(logRowsDF, 'name') \
    #                     .withColumn('distance', sqrt( pow((col('Lat')-col('center_Lat'))*111,2) + pow((col('Lon')-col('center_Lon'))*111,2) ) ) \
    #                     .groupBy('name').avg('distance')


    sc.stop()
    
except Exception as err:
    print(err)
    sc.stop()