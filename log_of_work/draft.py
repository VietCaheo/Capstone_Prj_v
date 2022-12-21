import os

import pyreadstat as pyd

ddict={}

for file in os.listdir():
    if file.endswith(".sas7bdat"):
        name = os.path.splitext(file)[0]
        df, meta = pyd.read_sas7bdat(file)
        # store the dataframe in a dictionary
        ddict[name]= df
        # alternatively bind to a new variable name
        exec(name + "= df.copy()")

##################################################
# for read multiple sab file by pyspark
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
 
def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)
i = 0
for file in files:
    if(i==0):
        df = spark.read.format('com.github.saurfang.sas.spark').load(file)
        cols= df.columns
        print(df.count())
    if(i>0):
        #df1=spark.read.format('com.github.saurfang.sas.spark').load(file)
        df = unionAll(df,spark.read.format('com.github.saurfang.sas.spark').load(file).select([col for col in cols]))
        print(df.count())
    i = i+1
df.count()

########################################################
# read one file
spark = SparkSession.builder.\
config("spark.jars.repositories", "https://repos.spark-packages.org/").\
config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
enableHiveSupport().getOrCreate()

#  read only one file into df_spark
df_spark = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat')

# _V: try to read whole sas7bdat files
# df_spark = spark.read.format('com.github.saurfang.sas.spark').load('../../data/18-83510-I94-Data-2016/*.*')