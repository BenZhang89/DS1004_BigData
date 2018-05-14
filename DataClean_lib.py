from pyspark.sql.functions import col
from pyspark.sql.window import Window as W
from pyspark.sql import functions as F
from pyspark.sql import Row
import pyspark.sql.dataframe
from pyspark.sql.functions import collect_list,size,monotonically_increasing_id

from pyspark.sql.functions import *
from pyspark.sql.types import DateType,IntegerType,StringType
import numpy as np
from math import sqrt
from operator import add
from pyspark.mllib.clustering import KMeans, KMeansModel
from operator import itemgetter
from csv import reader
from pyspark.sql.types import *


def choose_k(sub_df):
    wssse_list = []
    for i in range(1,11):
        clusters = KMeans.train(sub_df, i, maxIterations=10, initializationMode="random")
        WSSSE = sub_df.map(lambda point: sqrt(sum([x**2 for x in (point -clusters.centers[clusters.predict(point)])/sd]))).reduce(add)
        wssse_list.append(WSSSE)
    wssse_minus = [(x-y)/(x+0.001) for x, y in zip(wssse_list[:-2], wssse_list[1:])]
    zipped = zip(range(1,10), np.abs(wssse_minus))
    k = sorted(zipped, key = itemgetter(1), reverse = True)[0][0]
    return k

def add_clustercol(x_):
    cl = 0
    center = clusters.centers[0]
    point = np.array(x_)
    mindist = sqrt(sum([x**2 for x in (point -center)/sd]))
    for i in range(1, len(clusters.centers)):
        center = clusters.centers[i]
        distance = sqrt(sum([x**2 for x in (point -center)/sd]))
        if distance < mindist:
            cl = i
            mindist = distance
    clcluster = clusters.centers[cl]
    return (int(cl), float(mindist))

def inclust(x_, t):
    cl = x_[0]
    distance = x_[1]
    if distance > float(t):
        cl = -1
    return (int(cl), float(distance))

def myfun(point):
    if point[0][0] == -1:
        return point[1]
def col_is_string(df):
    #return all the string columns 
    return [item[0] for item in df.dtypes if item[1].startswith('string')]

#---------------------------------------------------------------------------Help Functions

class DataClean:
    def __init__(self, df):
        """Class constructor.
        :param  df      DataFrame to be transformed.
        """
        assert (isinstance(df, pyspark.sql.dataframe.DataFrame)), \
            "Error, df argument must be a pyspark.sql.dataframe.DataFrame instance"

        # Dataframe
        self._df = df
        # SparkContext:
        self._sql_context = self._df.sql_ctx
        self._number_of_transformations = 0

#---------------------------------------------------------------------------Class

    def find_minor_format(self, column):
        """This function remove special chars in string columns, such as: .!"#$%&/()
        :param columns      list of names columns to be processed.
        columns argument can be a string or a list of strings."""
        # Check if columns argument must be a string datatype:
        assert isinstance(column, (str)), \
            "Error: columns argument must be a string." 
        spark = SparkSession \
            .builder \
            .appName("Python Spark SQL basic example") \
            .config("spark.some.config.option", "some-value") \
            .getOrCreate()
        #Adding a dummy index column to return indices
        mdf = self._df.withColumn("idx", monotonically_increasing_id())
        windowSpec = W.orderBy("idx")
        mdf = mdf.select(column, F.row_number().over(windowSpec).alias('idx'))

        #transform dataframe to rdd, input the data type of every cell in the given collumn
        rdd_datatype = mdf.rdd.map(lambda r: Row(index=r[1],datatype=str(type(r[0]))))
        df = spark.createDataFrame(rdd_datatype)

        mintype_ind = []
        #groupBy datatype, and break func if only one type
        df = df.groupBy('datatype').count().sort(col("count"))
        if df.count() == 1:
            print ("There is only one datatype" +  df.take(1)[0][0])
            pass

        else:
            # find the min of cell datatype
            min_count = df.take(1)[0][1]
            min_datatype = df.rdd.filter(lambda r: r['count'] == min_count)
            min_datatype = min_datatype.map(lambda r: r[0]).collect()

            for datatype in min_datatype:
                final_df = mdf.where(mdf.name == min_datatype)
                mintype_ind = mintype_ind + final_df.select("idx").rdd.flatMap(lambda x:x).collect()
                final_df.show()         
        # Returning the transformer object for able chaining operations
        return mintype_ind

    def findDupRows(self):
        #Adding a dummy index column to return indices
        mdf = self._df.withColumn("RowNoIndex", monotonically_increasing_id())
        mdf=mdf.groupBy(self._df.columns).agg(collect_list("RowNoIndex").alias("dup_ind")).where(size("dup_ind") > 1)
        dup_ind_list = mdf.select("dup_ind").rdd.flatMap(lambda x:x).collect()
        dup_ind = []
        for dup in dup_ind_list:
            dup_ind = dup_ind + dup[1:]
        #Note: Indexing starts from 0
        return dup_ind

    def remove_duplicates(self):
        """
        Remove duplicate values from specified columns.
        Parameters
        ----------
        cols  : List of columns to make the comparison, this only  will consider this subset of columns,
        for dropping duplicates. The default behavior will only drop the identical rows. 
        return: Return a new DataFrame with duplicate rows removed
        """
        return self._df.drop_duplicates()

    def remove_special_chars(self, columns):
        """This function remove special chars in string columns, such as: .!"#$%&/()
        :param columns columns argument can be a string or a list of strings."""

        # Check if columns argument must be a string or list datatype:
        assert isinstance(columns, (str, list)), \
            "Error: columns argument must be a string or a list." 
        # self._assert_type_str_or_list(columns, "columns")

        # Filters all string columns in dataFrame
        string_cols = [c for (c, t) in filter(lambda col: col[1] == 'string', self._df.dtypes)]

        # If None or [] is provided with column parameter:
        if columns == "*": columns = string_cols[:]

        # If columns is string, make a list:
        if isinstance(columns, str):
            columns = [columns]

        # Check if columns to be process are in dataframe

        col_not_string = (set([column for column in columns]).difference(set([column for column in self._df.columns])))
        assert (col_not_string == set()), 'Error: The following columns do not exits in dataFrame: %s' % col_not_string
        # self._assert_cols_in_df(columns_provided=columns, columns_df=self._df.columns)

        col_not_string = (set([column for column in columns]).difference(set([column for column in string_cols])))

        assert (
            col_not_string == set()), 'Error: At least on of the following columns is not string datatype: %s' \
                                      % col_not_string

        def delete_spec_chars(input_str):
            # Remove all punctuation and control characters
            for punct in (set(input_str) & set(string.punctuation)): ##???
                input_str = input_str.replace(punct, "")
            return input_str

        # User define function that does operation in cells

        function = udf(lambda cell: delete_spec_chars(cell) if cell is not None else cell, StringType())
        #if not StringType(), will be None, then return cell

        exprs = [function(c).alias(c) if (c in columns) and (c in string_cols)  else c for c in self._df.columns]

        self._df = self._df.select(*exprs)

        # self._add_transformation()  # checkpoint in case

        # Returning the transformer object for able chaining operations
        return self



    def show(self):
        # , n=20, truncate=True, vertical=False
        """Prints the first ``n`` rows to the console.
        param n: Number of rows to show.
        param truncate: If set to True, truncate strings longer than 20 chars by default.
            If set to a number greater than one, truncates long strings to length ``truncate``
            and align cells right.
        param vertical: If set to True, print output rows vertically (one line
            per column value)."""
        return self._df.show()


    def to_lower(self, col_names = 'all'):
        allcols= self._df.columns
        if col_names == 'all':
            str_cols = col_is_string(self._df)
            nonstr_cols = list(filter(lambda x: x not in str_cols, allcols))
            trans_col = [lower(col(col_name)).name(col_name) for col_name in str_cols]
            combined =nonstr_cols+trans_col
            rn = self._df.select(*combined)
        else:
            trans_col = [lower(col(col_name)).name(col_name) for col_name in col_names]
            non_trans = list(filter(lambda x: x not in trans_col, allcols))
            combined = non_trans + trans_col
            rn = self._df.select(*combined)
        return rn

    def to_upper(self, col_names = 'all'):
        allcols= self._df.columns
        if col_names == 'all':
            str_cols = col_is_string(self._df)
            nonstr_cols = list(filter(lambda x: x not in str_cols, allcols))
            trans_col = [upper(col(col_name)).name(col_name) for col_name in str_cols]
            combined =nonstr_cols+trans_col
            rn = self._df.select(*combined)
        else:
            trans_col = [upper(col(col_name)).name(col_name) for col_name in col_names]
            non_trans = list(filter(lambda x: x not in trans_col, allcols))
            combined = non_trans + trans_col
            rn = self._df.select(*combined)
        return rn

    def detect_outlier(self, col_names = 'all', cutoff=5):
        #filter out the numeric columns
        col_name = [item[0] for item in self._df.dtypes if item[1].startswith('int') or item[1].startswith('float')]
        num_col = len(col_name)
        if col_names == 'all':
            sub_df = self._df[col_name]
            sub_col = num_col
        else:
            sub_df = self._df[col_names]
            sub_col = len(col_names)
        sub_df = sub_df.rdd
        sub_df = sub_df.map(lambda x: np.array([x[i] for i in range(sub_col)]))
        sd = sub_df.map(lambda x: x).stdev()
        k = choose_k(sub_df)
        clusters = KMeans.train(sub_df, k, maxIterations = 10, initializationMode= 'random')
        rdd_clusts = sub_df.map(lambda x: add_clustercol(x))
        #Count number of points in each cluster
        rdd_cn = rdd_clusts.map(lambda x: (x[0],1)).reduceByKey(add).top(len(clusters.centers))
        rdd_w_clusts_wnullclust = rdd_clusts.map(lambda x: inclust(x,cutoff))
        rdd_null_cn = rdd_w_clusts_wnullclust.map(lambda y: (y[0],1)).reduceByKey(add).top(5)
        rdd_new = rdd_w_clusts_wnullclust.zipWithIndex()
        rdd_null = rdd_new.filter(lambda x: myfun(x))
        rdd_null_num = rdd_null.map(lambda x: x[1])
        return rdd_null_num.collect()

    def dropnacol(self, per = 0.2, dropcol = False, droprow = True):

        '''Drop Nan column based on user input percentage of empty \
             or drop Nan rows based on user preferrence'''


        start = time.time()
        if dropcol == True:
            df_cols = self.df.columns
            df_nullCounts = [self.df.where(col(c).isNull()).count() for c in self.df.columns]
            num_Cols = len(self.df.columns)
            num_Rows = self.df.count()
            df1 = self.df
            for i in range(num_Cols):
                    if (df_nullCounts[i]*100)/float(num_Rows) > per:
                        df1=df1.drop(df_cols[i])
            
            print('drop null columns done!')
            end = time.time()
            print(end - start)

            return df1

        if droprow == True:
            df1 = self.df.na.drop()
            print('drop null rows done!')
            end = time.time()
            print(end - start)

            return df1


    def replaceByvalue(self, col = [], bymean = False, bymode = False, byzero = False):

        '''Replace col empty column value \
             by mean or mode based '''
        start = time.time()
        imputeDF = self.df

        for x in col:   

            if bymean == True & isinstance(self.df.schema[x].dataType, (IntegerType, LongType)):
                    removeNa = self.df.na.drop(subset=[x])
                    meanValue = removeNa.agg(avg(x)).first()[0]
                    imputeDF = imputeDF.na.fill(meanValue, [x])

            elif byzero == True & isinstance(self.df.schema[x].dataType, (IntegerType, LongType)):
                    removeNa = self.df.na.drop(subset=[x])
                    imputeDF = imputeDF.na.fill(int(0), [x])
    
            elif bymode == True & isinstance(self.df.schema[x].dataType, (IntegerType, LongType)):
                removeNa = self.df.na.drop(subset=[x])
                cnts = removeNa.groupBy(x).count()
                mode = cnts.join(cnts.agg(max("count").alias("max_")), col("count") == col("max_")).limit(1).select(x)
                mode = mode.first()[0]
                imputeDF = imputeDF.na.fill(mode, [x])

        end = time.time()
        print(end - start)

        return imputeDF


    def standardize(self,  col = []):
        '''
        Add normalised columns to the input dataframe.
        formula = [(X - mean) / std_dev]
        Inputs : training dataframe, list of column name strings to be normalised
        Returns : dataframe with new normalised columns, averages and std deviation dataframes 
        '''
        # Find the Mean and the Standard Deviation for each column
        start = time.time()
        aggE = []
        aggS = []
        for column in col:
            aggE.append(mean(self.df[column]).alias(column))
            aggS.append(stddev(self.df[column]).alias(column + '_stddev'))
        
        averages = self.df.agg(*aggE).collect()[0]
        std_devs = self.df.agg(*aggS).collect()[0]
        
        # Standardise each dataframe, column by column
        for column in col:            
            # Standardise the TRAINING data
            self.df = self.df.withColumn(column + '_norm', ((self.df[column] - averages[column]) / 
                                                                  std_devs[column + '_stddev']))  
        
        end = time.time()
        print(end - start)
        
     
        return self.df, averages, std_devs


