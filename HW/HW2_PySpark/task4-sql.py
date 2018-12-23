import sys
import string
from pyspark.sql.functions import format_string
from csv import reader
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

#lines = spark.read.format('csv').options(header='true',inferschema='true').load ('/user/ecc290/HW1data/open-violations-header.csv')
lines = spark.read.format('csv').options(header='true',inferschema='true').load (sys.argv[1])
lines.createOrReplaceTempView("lines")
result = spark.sql("SELECT CASE WHEN registration_state='NY' THEN 'NY' ELSE 'Other' END AS registration_state, COUNT(*) as counts FROM lines GROUP BY CASE WHEN registration_state='NY' THEN 'NY' ELSE 'Other' END ORDER BY registration_state")
result.select(format_string('%s \t %d',result.registration_state, result.counts)).write.save("task4-sql.out",format="text")

