from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('words').getOrCreate()
sc = spark.sparkContext

try:
    lines = sc.textFile('../data/epa_hap_daily_summary-small.csv') # Change the name of the file to what you have it named here
    logRows = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr : Row( State = arr[24], County = arr[25], countyCode = arr[1], Arithmetic_mean = float(arr[16])))
    logRowsDF = spark.createDataFrame( logRows )
    logRowsDF.createOrReplaceTempView("log")

    stateRanksDF = spark.sql("SELECT State, \
        countyCode AS County_Code, \
        County, \
        AVG(Arithmetic_mean) AS Pollutant_levels \
        FROM log GROUP BY State, countyCode, County \
        ORDER BY Pollutant_levels DESC")
    stateRanksDF.show(100)

    sc.stop()
except Exception as err:
    print(err)
    sc.stop()
