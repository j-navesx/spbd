from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('words').getOrCreate()
sc = spark.sparkContext

try:
    lines = sc.textFile('epa_hap_daily_summary-small.csv') # Change the name of the file to what you have it named here
    logRows = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr : Row( Year = arr[11][:4], State = arr[24], Arithmetic_mean = float(arr[16])))
    
    logRowsDF = spark.createDataFrame( logRows )
    
    logRows2DF = logRowsDF.select('State','Year','Arithmetic_mean')\
                                                           .groupBy('Year','State')\
                                                           .agg((sum('Arithmetic_mean')/count('Arithmetic_mean')).alias('Avg Pollutants'))\
                                                           .sort('Avg Pollutants', ascending = False)
                                                                    
    
    logRows2DF.show(200,truncate=50)
    
    sc.stop()
except Exception as e:
    print(e)
    sc.stop()