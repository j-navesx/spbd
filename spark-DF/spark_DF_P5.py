#https://stackoverflow.com/questions/39048229/spark-equivalent-of-if-then-else

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('words').getOrCreate()
sc = spark.sparkContext

try:
    # files
    lines_states = sc.textFile('usa_states.csv')
    lines = sc.textFile('epa_hap_daily_summary-small.csv') # Change the name of the file to what you have it named here
    
    # file mapping
    logRows_states = lines_states.filter( lambda line : len(line) > 0)    \
                    .zipWithIndex() \
                    .filter( lambda x: x[1] > 0) \
                    .map(lambda x: x[0]) \
                    .map( lambda line: line.split(',')) \
                    .map( lambda arr : Row( state = arr[0], name = arr[1], centerLat = (float(arr[2]) + float(arr[3]))/2, \
                                            centerLon = (float(arr[4]) + float(arr[5]))/2))
    logRows = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr: Row( name = arr[24], countyCode = arr[1], siteNum = arr[2], lat = float(arr[5]), lon = float(arr[6]) ) )  
    
    # Creates the dataframes
    logRowsDF = spark.createDataFrame( logRows )
    
    logRows_statesDF = spark.createDataFrame( logRows_states )
    
    
    logRowsDF = logRowsDF.select('name','countyCode','siteNum','lat','lon').distinct()
    
    logRowsDF = logRowsDF.select('name','lat','lon')
    
    
    joinedDF = logRowsDF.join(logRows_statesDF,logRowsDF.name == logRows_statesDF.name,"inner" )
    
    MonitorDF = joinedDF.select((logRowsDF.name).alias('name'),\
                                (logRowsDF.lat).alias('lat'),\
                                (logRowsDF.lon).alias('lon'),\
                                (logRows_statesDF.centerLat).alias('centerLat'),\
                                (logRows_statesDF.centerLon).alias('centerLon')\
                               )
    
    
    MonitorDF = MonitorDF.withColumn("quadrant", when((col("lat") < col("centerLat")) & (col("lon") < col("centerLon")),'NW')\
                                                 .when((col("lat") < col("centerLat")) & (col("lon") > col("centerLon")),'NE')\
                                                 .when((col("lat") > col("centerLat")) & (col("lon") < col("centerLon")),'SW')\
                                                 .when((col("lat") > col("centerLat")) & (col("lon") > col("centerLon")),'SE')
                                                 .otherwise('Center or Borders')
                                    )                              
    
    FinalMonitorDF = MonitorDF.groupBy('name','quadrant').agg(count('quadrant').alias('Nr of Monitors')).sort('name', ascending = True)
    
    FinalMonitorDF.show(400)
    

    sc.stop()
except Exception as err:
    print(err)
    sc.stop()