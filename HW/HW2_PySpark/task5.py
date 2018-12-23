import sys
from pyspark import SparkContext
from operator import add
from csv import reader


sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
counts = lines.map(lambda x: ((x[14],x[16]), 1)).reduceByKey(add)
maxviolation = sc.parallelize([counts.max(key=lambda x:x[1])])
final = maxviolation.map(lambda x:'{0:s}, {1:s}\t {2:d}'.format(x[0][0],x[0][1], x[1]))
final.saveAsTextFile("task5.out")

sc.stop()
