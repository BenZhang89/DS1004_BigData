import sys
from pyspark import SparkContext
from operator import add
from csv import reader

#if __name__ == '__main__':
 #   if len(sys.argv) != 2:
  #      exit(-1)
sc = SparkContext()
lines = sc.textFile(sys.argv[1], 1)
lines = lines.mapPartitions(lambda x: reader(x))
counts = lines.map(lambda x: (x[2], 1)).reduceByKey(add)
final = counts.map(lambda x:'{0:s}\t{1:d}'.format(x[0],x[1]))
final.saveAsTextFile("task2.out")

sc.stop()
