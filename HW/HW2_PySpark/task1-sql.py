import sys
import string
from pyspark.sql.functions import format_string
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()

parking = spark.read.format('csv').options(header='true',inferschema='true').load (sys.argv[1])
opening = spark.read.format('csv').options(header='true',inferschema='true').load (sys.argv[2])

parking.createOrReplaceTempView("parking")
opening.createOrReplaceTempView("opening")

parking = spark.sql("SELECT summons_number, plate_id, violation_precinct, violation_code, issue_date FROM parking")
opening = spark.sql("SELECT summons_number AS o_summons_number FROM opening")
joined = parking.join(opening, parking.summons_number == opening.o_summons_number, "left_outer")


joined.createOrReplaceTempView("joined")
result = spark.sql("SELECT summons_number, plate_id, violation_precinct, violation_code, issue_date FROM joined WHERE o_summons_number is null")
result.select(format_string('%d\t%s, %d, %d, %s',result.summons_number,result.plate_id,result.violation_precinct,result.violation_code,date_format(result.issue_date,'yyyy-MM-dd'))).write.save("task1-sql.out",format="text")
