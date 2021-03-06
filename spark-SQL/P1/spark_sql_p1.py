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
                     .map( lambda arr : Row( state_name = arr[24], site_num = arr[2], state_code = arr[0], county_code = arr[1]))
    logRowsDF = spark.createDataFrame( logRows )
    logRowsDF.createOrReplaceTempView("log")

    query = query = "SELECT state_name, COUNT(*) AS number_monitors FROM (SELECT DISTINCT * FROM log) GROUP BY state_name ORDER BY number_monitors DESC"

    final = spark.sql(query)

    final.show(30)

    sc.stop()
except Exception as err:
    print(err)
    sc.stop()
