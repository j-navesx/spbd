from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('words').getOrCreate()
sc = spark.sparkContext

try:
    # files
    lines_states = sc.textFile('../data/epa_hap_daily_summary-small.csv')
    #lines = sc.textFile('log.csv')

    # file mapping
    logRows = lines_states.filter( lambda line : len(line) > 0)    \
                    .zipWithIndex() \
                    .filter( lambda x: x[1] > 0) \
                    .map(lambda x: x[0]) \
                    .map( lambda line: line.split(',')) \
                    .map( lambda arr : Row( state = arr[24], countyCode = arr[1], site_num = arr[2]))    
    
    # Dataframe creation
    logRowsDF = spark.createDataFrame( logRows )
    #logRowsDF = spark.createDataFrame( logRows )
    #logRowsDF = logRowsDF.distinct() # Makes sure we are using different monitors
    
    logRows2DF = logRowsDF.select('state','countyCode','site_num').distinct().groupBy('state')\
                                                                         .agg(count('site_num').alias('Nr of Monitors'))\
                                                                         .sort('Nr of Monitors', ascending = False)   
    logRows2DF.show(100,truncate=50)

    sc.stop()
except Exception as e:
    print(e)
    sc.stop()
