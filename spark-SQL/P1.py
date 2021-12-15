from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master('local[*]').appName('words').getOrCreate()
sc = spark.sparkContext

try:
    # lines = sc.textFile('web.log')
    # logRows = lines.filter( lambda line : len(line) > 0)    \
    #                 .map( lambda line: line.split(' ')) \
    #                 .map( lambda arr : Row( date = arr[0][:16], ip_source = arr[1], return_value = arr[2], \
    #                                         operation = arr[3], url = arr[4], time = arr[5], \
    #                                         last_ip_number = "{0:0>5}".format(arr[1].split(".")[-1].split(":")[-1]) )) 
    # logRowsDF = spark.createDataFrame( logRows )
    # logRowsDF.createOrReplaceTempView("log")

    # finalDF = spark.sql("SELECT COUNT(*) AS NrRequests FROM log GROUP BY date ORDER BY NrRequests")

    # finalDF.show(30)

    lines = sc.read.option("header", True).csv("[insert path]")
    lines.createOrReplaceTempView("data")

    query = "SELECT COUNT(*) as rank, state_name FROM (SELECT site_num, CONCAT(state_code,county_code) as county, state_name FROM data GROUP BY site_num, county) ORDER BY rank DESC"

    sc.stop()
except Exception as err:
    print(err)
    sc.stop()