import sys
import string
from pyspark.sql.functions import format_string
from csv import reader

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

#lines = spark.read.format('csv').options(header='true',inferschema='true').load ('/user/ecc290/HW1data/open-violations-header.csv')
lines = spark.read.format('csv').options(header='true',inferschema='true').load (sys.argv[1])
lines.createOrReplaceTempView("lines")

result = spark.sql("SELECT license_type, SUM(amount_due) as sum, AVG(amount_due) as avg FROM lines GROUP BY license_type")
result.select(format_string('%s \t %.2f, %.2f',result.license_type, result.sum, result.avg)).write.save("task3-sql.out",format="text")
