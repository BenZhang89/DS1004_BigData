import sys
import string
from pyspark.sql.functions import format_string
from csv import reader
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load (sys.argv[1])
parking.createOrReplaceTempView("parking")

result = spark.sql("SELECT violation_code, sum(weekend)/8 as weekends, sum(week)/23 as weekdays FROM (SELECT violation_code, " + 
 "case when Day(issue_date) in (5,6,12,13,19,20,26,27) then 1 else 0 end as weekend, "+
 "case when not Day(issue_date) in (5,6,12,13,19,20,26,27) then 1 else 0 end as week "+
 "from parking) group by violation_code") 

result.select(format_string('%s\t%.2f, %.2f',result.violation_code, result.weekends, result.weekdays)).write.save("task7-sql.out",format="text")


