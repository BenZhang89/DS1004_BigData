import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType,IntegerType,StringType
spark = SparkSession.builder.appName("project").config("spark.some.config.option", "some-value").getOrCreate()
def read_df(csv_file):
	return spark.read.format('csv').options(header='true',inferschema='true').load(csv_file)
def col_is_date(df):
	#return all the datetype columns 
	return [item[0] for item in df.dtypes if item[1].startswith('date')]
def col_is_string(df):
	#return all the string columns 
	return [item[0] for item in df.dtypes if item[1].startswith('string')]
def to_lower(df, col_names = 'all'):
	allcols= df.columns
	if col_names == 'all':
		str_cols = col_is_string(df)
		nonstr_cols = list(filter(lambda x: x not in str_cols, allcols))
		trans_col = [lower(col(col_name)).name(col_name) for col_name in str_cols]
		combined =nonstr_cols+trans_col
		rn = df.select(*combined)
	else:
		trans_col = [lower(col(col_name)).name(col_name) for col_name in col_names]
		non_trans = list(filter(lambda x: x not in trans_col, allcols))
		combined = non_trans + trans_col
		rn = df.select(*combined)
	return rn

def to_upper(df, col_names = 'all'):
	allcols= df.columns
	if col_names == 'all':
		str_cols = col_is_string(df)
		nonstr_cols = list(filter(lambda x: x not in str_cols, allcols))
		trans_col = [upper(col(col_name)).name(col_name) for col_name in str_cols]
		combined =nonstr_cols+trans_col
		rn = df.select(*combined)
	else:
		trans_col = [upper(col(col_name)).name(col_name) for col_name in col_names]
		non_trans = list(filter(lambda x: x not in trans_col, allcols))
		combined = non_trans + trans_col
		rn = df.select(*combined)
	return rn
		
	