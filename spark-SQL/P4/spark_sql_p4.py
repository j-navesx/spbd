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
                    .map( lambda arr : Row( stateName = arr[1], minLat = arr[2], maxLat = arr[3], \
                                            minLon = arr[4], maxLon = arr[5]))
    logRows = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr: Row( stateName = arr[24], siteNum = arr[2], countyCode = arr[0]+arr[1], lat = float(arr[5]), lon = float(arr[6]) ) )  
    
    # Creates the dataframes and views
    logRowsDF = spark.createDataFrame( logRows )
    logRowsDF.createOrReplaceTempView("log")

    logRows_statesDF = spark.createDataFrame( logRows_states )
    logRows_statesDF.createOrReplaceTempView("log_states")
    
    query = "SELECT log.name\
        FROM (SELECT log.name,\
            \
        )"
    
    query = "SELECT stateName as State, avg(Dist_Monitor_Center) as Avg_Dist_Monitor_Center \
            FROM (SELECT stateName,\
                countyCode,\
                siteNum,\
                sqrt( pow( (lat-((minLat+maxLat)/2))*111, 2) + pow( (lon-((minLon+maxLon)/2))*111, 2) ) as Dist_Monitor_Center\
                FROM (SELECT log.stateName,\
                    log.countyCode,\
                    log.siteNum,\
                    log.lat,\
                    log.lon,\
                    log_states.minLat,\
                    log_states.maxLat,\
                    log_states.minLon,\
                    log_states.maxLon\
                    FROM log\
                    JOIN log_states ON stateName = name\
                ) table1\
                GROUP BY stateName, countyCode, siteNum, lat, lon, minLat, maxLat, minLon, maxLon\
            ) table2\
            GROUP BY stateName\
            ORDER BY Avg_Dist_Monitor_Center DESC"

    finalDF = spark.sql(query)

    
    finalDF.show(100)

    sc.stop()
except Exception as err:
    print(err)
    sc.stop()