from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('words').getOrCreate()
sc = spark.sparkContext

try:
    # files
    lines_states = sc.textFile('../data/usa_states.csv')
    lines = sc.textFile('../data/epa_hap_daily_summary-small.csv') # Change the name of the file to what you have it named here
    
    # file mapping
    logRows_states = lines_states.filter( lambda line : len(line) > 0)    \
                    .zipWithIndex() \
                    .filter( lambda x: x[1] > 0) \
                    .map(lambda x: x[0]) \
                    .map( lambda line: line.split(',')) \
                    .map( lambda arr : Row( name = arr[1], centerLat = (float(arr[2])+float(arr[3]))/2, \
                                            centerLon = (float(arr[4])+float(arr[5]))/2))
    logRows = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr: Row( stateName = arr[24], siteNum = arr[2], countyCode = arr[1], lat = float(arr[5]), lon = float(arr[6]) ) )  
    
    # Creates the dataframes and views
    logRowsDF = spark.createDataFrame( logRows )
    logRowsDF.createOrReplaceTempView("log")

    logRows_statesDF = spark.createDataFrame( logRows_states )
    logRows_statesDF.createOrReplaceTempView("log_states")

    query = "SELECT stateName as State, AVG(Dist_Monitor_Center) as Avg_Dist_Monitor_Center \
    FROM ( SELECT stateName, \
        sqrt( pow( (AVG(lat-centerLat))*111, 2) + pow( (AVG(lon-centerLon))*111, 2) ) as Dist_Monitor_Center \
        FROM log JOIN log_states ON stateName = name\
        GROUP BY stateName, countyCode, siteNum )\
    GROUP BY State"

    finalDF = spark.sql(query)

    
    finalDF.show(100)

    sc.stop()
except Exception as err:
    print(err)
    sc.stop()
