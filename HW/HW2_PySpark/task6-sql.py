import sys
import string
from pyspark.sql.functions import format_string
from csv import reader
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

#lines = spark.read.format('csv').options(header='true',inferschema='true').load ('/user/ecc290/HW1data/parking-violations-header.csv')
parking = spark.read.format('csv').options(header='true',inferschema='true').load (sys.argv[1])
parking.createOrReplaceTempView("parking")

result = spark.sql("SELECT plate_id, registration_state, count(*) as counts FROM parking GROUP BY plate_id, registration_state ORDER BY counts DESC, plate_id LIMIT 20")
result.select(format_string('%s, %s\t%d',result.plate_id, result.registration_state, result.counts)).write.save("task6-sql.out",format="text")



