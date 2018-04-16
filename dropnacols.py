import sys
from csv import reader
from pyspark.sql import SparkSession
from pyspark.sql.funcitons import col, count

spark = SparkSession.builder.appName("drop Nan").config("spark.some.config.option","some-value").getOrCreate()

def data_clean():

	def load_data(data):
		return spark.read.load(data, header = "true", inferSchema = 'true', format = 'csv')

	def dropna(df, per, drop = False):

		df_cols = df.columns
		df_nullCounts = [df.where(col(c).isNull).count() for c in df.columns]
		num_cols = len(df.columns)
		num_rows = df.count()
		df1 = df
		for i in range(num_cols):
			if (df_nullCounts[i]*100)/float(num_rows) > per:
				df1.drop(df_cols[i], inplace = True)

		return df1

	def replacena(df, bymean = False, bymode = False):

		df_cols = df.columns
		df_mean_cal = []




