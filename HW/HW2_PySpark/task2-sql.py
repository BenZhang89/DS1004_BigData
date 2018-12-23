import sys
import string
from pyspark.sql.functions import format_string
from csv import reader

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

#lines = spark.read.format('csv').options(header='true',inferschema='true').load ('/user/ecc290/HW1data/parking-violations-header.csv')
lines = spark.read.format('csv').options(header='true',inferschema='true').load (sys.argv[1])
lines.createOrReplaceTempView("lines")
result = spark.sql("SELECT violation_code, count(violation_code) as counts FROM lines GROUP BY violation_code")
result.select(format_string('%s\t%d',result.violation_code,result.counts)).write.save("task2-sql.out",format="text")


