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
                     .map( lambda arr : Row( state_name = arr[24], site_num = arr[2], state_code = arr[0], county_code = arr[1]))
    logRowsDF = spark.createDataFrame( logRows )
    logRowsDF.createOrReplaceTempView("log")

    query = "SELECT COUNT(*) as rank, state_name FROM (SELECT site_num, CONCAT(state_code,county_code) as county, state_name FROM data GROUP BY site_num, county) ORDER BY rank DESC"

    final = spark.sql(query)

    final.show(30)

    sc.stop()
except Exception as err:
    print(err)
    sc.stop()