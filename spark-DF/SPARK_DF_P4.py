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
                    .map( lambda arr : Row( state = arr[0], name = arr[1], minLat = float(arr[2]), \
                                            maxLat = float(arr[3]), minLon = float(arr[4]), \
                                            maxLon = float(arr[5])))
    logRows = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr: Row( name = arr[24], Lat = float(arr[5]), Lon = float(arr[6]) ) )    
    
    # Dataframe creation
    logRowsStatesDF = spark.createDataFrame( logRows_states )
    logRowsDF = spark.createDataFrame( logRows )
    logRowsDF = logRowsDF.distinct() # Makes sure we are using different monitors

    # Necessary computations to solve the problem
    finalDF = logRowsStatesDF.withColumn('center_Lat', (col('minLat')+col('maxLat'))/2 ) \
                        .withColumn('center_Lon', (col('minLon')+col('maxLon'))/2 ) \
                        .drop('minLon') \
                        .drop('maxLon') \
                        .drop('minLat') \
                        .drop('maxLat') \
                        .drop('state') \
                        .join(logRowsDF, 'name') \
                        .withColumn('distance', sqrt( pow((col('Lat')-col('center_Lat'))*111,2) + pow((col('Lon')-col('center_Lon'))*111,2) ) ) \
                        .groupBy('name').avg('distance')
    
    finalDF.show(54)

    sc.stop()
    
except Exception as err:
    print(err)
    sc.stop()