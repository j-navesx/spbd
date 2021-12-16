from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('words').getOrCreate()
sc = spark.sparkContext

try:
    lines = sc.textFile('log.csv') # Change the name of the file to what you have it named here
    logRows = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr : Row( Year = arr[11][:4], State = arr[24], Arithmetic_mean = float(arr[16])))
    logRowsDF = spark.createDataFrame( logRows )
    logRowsDF.createOrReplaceTempView("log")

    stateRanksDF = spark.sql("SELECT Year, State, AVG(Arithmetic_mean) AS Pollutant_levels FROM log GROUP BY State, Year ORDER BY Year, Pollutant_levels")
    stateRanksDF.show(100)

    sc.stop()
except Exception as err:
    print(err)
    sc.stop()