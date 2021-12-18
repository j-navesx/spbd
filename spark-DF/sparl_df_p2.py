from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('words').getOrCreate()
sc = spark.sparkContext

try:
    lines = sc.textFile('epa_hap_daily_summary_small.csv') # Change the name of the file to what you have it named here
    logRows = lines.filter( lambda line: len(line) > 0) \
                     .zipWithIndex() \
                     .filter( lambda x: x[1] > 0) \
                     .map(lambda x: x[0]) \
                     .map( lambda line: line.split(',')) \
                     .map( lambda arr : Row( county_name = arr[25], state_code = arr[0], county_code = arr[1], arithmetic_mean = float(arr[16])))
    logRowsDF = spark.createDataFrame( logRows )
    logRowsDF.createOrReplaceTempView("log")
    
    # Necessary computations to solve the problem
    finalDF = logRowsDF.withColumn('county', (col('state_code')+col('county_code'))) \
                    .drop('state_code') \
                    .drop('county_code') \
                    .groupBy('county','county_name').sum('arithmetic_mean') \
                    .withColumnRenamed('sum(arithmetic_mean)', 'pollutant_levels') \
                    .orderBy(col('pollutant_levels').desc()) \
                    .drop('county') \

    finalDF.show(20)
    sc.stop()
    
except Exception as err:
    print(err)
    sc.stop()