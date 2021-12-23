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
                    .map( lambda arr : Row( state = arr[0], name = arr[1], centerLat = (float(arr[2]) + float(arr[3]))/2, \
                                            centerLon = (float(arr[4]) + float(arr[5]))/2))
    logRows = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr: Row( name = arr[24], countyCode = arr[1], stateNUM = arr[2], lat = float("{:.3f}".format(float(arr[5]))), lon = float("{:.3f}".format(float(arr[6]))) ) )  
    
    # Creates the dataframes and views
    logRowsDF = spark.createDataFrame( logRows )
    logRowsDF.createOrReplaceTempView("log")

    logRows_statesDF = spark.createDataFrame( logRows_states )
    logRows_statesDF.createOrReplaceTempView("log_states")
    
    # Atribui a cada monitor único o seu quadrante
    MonitorDF = spark.sql("SELECT log.name, log.countyCode, log.stateNum, \
     CASE \
         WHEN log.lat < log_states.centerLat AND log.lon > log_states.centerLon THEN 'NE' \
         WHEN log.lat > log_states.centerLat AND log.lon > log_states.centerLon THEN 'SE' \
         WHEN log.lat > log_states.centerLat AND log.lon < log_states.centerLon THEN 'SW' \
         WHEN log.lat < log_states.centerLat AND log.lon < log_states.centerLon THEN 'NW' \
         ELSE 'Center or Borders' \
     END AS Quadrant \
     FROM log JOIN log_states ON log.name=log_states.name GROUP BY log.name, log.countyCode, log.stateNum, Quadrant")
    MonitorDF.createOrReplaceTempView("Monitor")

    # Conta o Nr. de monitores em cada quadrante por estado
    finalDF = spark.sql("SELECT name AS State, Quadrant, count(*) AS Num_Monitors  FROM Monitor GROUP BY name, Quadrant")
    
    finalDF.show(100)

    sc.stop()
except Exception as err:
    print(err)
    sc.stop()