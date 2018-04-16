
# coding: utf-8

# In[6]:

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import collect_list,size,monotonically_increasing_id
from pyspark.sql import Row

def findDupRows(self):
    #Adding a dummy index column to return indices
    mdf = self._jdf.withColumn("RowNoIndex", monotonically_increasing_id())
    mdf=mdf.groupBy(df.columns).agg(collect_list("RowNoIndex").alias("dup_ind")).where(size("dup_ind") > 1)
    dup_ind_list = mdf.select("dup_ind").rdd.flatMap(lambda x:x).collect()
    dup_ind = []
    for dup in dup_ind_list:
        dup_ind = dup_ind + dup[1:]
    #Note: Indexing starts from 0
    return dup_ind


# In[ ]:

def dropDupRows(self, subset=None):
    if subset is None:
        jdf = self._jdf.dropDuplicates()
    else:
        jdf = self._jdf.dropDuplicates(self._jseq(subset))
    return DataFrame(jdf, self.sql_ctx)

