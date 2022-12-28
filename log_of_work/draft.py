# -------------------------------------------------------
# dealing with datetime converting from sas datafile
# https://knowledge.udacity.com/questions/741863

get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(float(x))).isoformat() if x else None)

df_Immigration = df_Immigration.withColumn("arrdate", get_date(df_Immigration.arrdate))
df_Immigration = df_Immigration.withColumn("depdate", get_date(df_Immigration.depdate))

# -------------------------------------------------------

# drop duplicated: 
dropDisDF = df.dropDuplicates(["department","salary"])

##########################################################
#  timestamp NaN check in Schema Spark
from pyspark.sql import functions as f

df.select(*[
    (
        f.count(f.when((f.isnan(c) | f.col(c).isNull()), c)) if t not in ("timestamp", "date")
        else f.count(f.when(f.col(c).isNull(), c))
    ).alias(c)
    for c, t in df.dtypes if c in cols_check
]).show()

cols_check = [list of columns]

##########################################################


spark = create_spark_session()

# In case need to staging, save target table in S3, Redshift, need to define as input, output here also
# input_data = 
# to staging parquets file of each table into S3 
# output_data = "s3a://viet-datalake-bucket/"

# 12 files of Immigration Datasets, consider to limit data when run test for time saving
input_Immig_data= '../../data/18-83510-I94-Data-2016'
# another datasets by signle file
input_UsCities_data = './us-cities-demographics.csv'
input_AirPort_data  = './airport-codes_csv.csv'
# This file with more than 1mil rows
input_CityTemper_data ='../../data2/GlobalLandTemperaturesByCity.csv'
# Step1 Explore Data and NaN + Duplicated check 
# Immigration datasets sas78dat files, due to large amount, NaN check will be done in processing multiple file below
# ---------------------------------------------
Check_NaN('temperature', input_CityTemper_data)
Check_NaN('uscity', input_UsCities_data)
Check_NaN('airport', input_AirPort_data)
# Step2: Do write up the doc
# Some cleaning for NaN and Duplicated data were done in process each datasets right below
# ---------------------------------------------
# Step3: Build a complete ETL process
# ---------------------------------------------
    # Load data file from data set
    # Cleaning data : NaN and duplicated if need
    # Loading data to target tables
    # Write spark parquet files
# for local read by Spark DataFrame
# Propotype want to build:  process_function(spark, input, output)
# To read the dataset using the PySpark 
process_Immigra_data(spark, input_Immig_data )
process_UsCities_data(spark, input_UsCities_data )
process_AirPort_data(spark, input_AirPort_data )
process_CityTemper_data(spark, input_CityTemper_data )
# ##########################################################
import pyspark.sql.functions as f

#_______v_______v_______v_______v_______v_______v_______v_______v_______v_______v_______v
#  to check how many duplicated row in each df
dup_count_df_Airport = len(df_Airport) - len(df_Airport.drop_duplicates())
print("to count duplicated row in df_Airport is {}".format(dup_count_df_Airport))

dup_count_df_Temper = len(df_Temper) - len(df_Temper.drop_duplicates())
print("to count duplicated row in df_Temper is {}".format(dup_count_df_Temper))

dup_count_df_UsCities = len(df_UsCities) - len(df_UsCities.drop_duplicates())
print("to count duplicated row in df_Temper is {}".format(dup_count_df_UsCities))

#  check how many duplicated by column in each df
#  ...


# -------------------------------------------------------------------------
print("to check any duplicated with interested column in each df ... \n")

# dup_ident = df_Airport[df_Airport.duplicated('name')]
# print("duplicated row in df_Airport is ... \n {}".format(dup_ident))
# print("duplicated row in df_UsCities is ... \n {}".format(df_UsCities[df_UsCities.duplicated()]))
# print("duplicated row in df_Temper is ... \n {}".format(df_Temper.duplicated(subset=['dt'], keep='last')))

# with each df read by pysark, the way to check is different
# check duplicate row in df_ImmigAll by sql command
      
df_ImmigAll_cols = df_ImmigAll.columns
print("to see list of columns df_ImmigAll .=>> \n {}".format(df_ImmigAll_cols))

# print("show distinct row in whole df_ImmigAll ... \n ")
# df_ImmigAll.select(df_ImmigAll.columns).distinct()

# print("show count row in whole df_ImmigAll ... \n ")
# df_ImmigAll.select(df_ImmigAll.columns).count()

# print("to count all row of df_ImmigAll ... \n {}".format(df_ImmigAll.count()))

print("Just SQL query to inspect the duplicates row ... \n")
df_ImmigAll.groupby(df_ImmigAll_cols) \
    .count() \
    .where(f.col('count') > 1) \
    .select(f.sum('count')) \
    .show()
#_______v_______v_______v_______v_______v_______v_______v_______v_______v_______v_______v

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
#########################################################
# Data Cleaning with NaN and Duplicated value

# immigration data
fact_immigration.dropna(subset=['cic_id'])
dim_immigration_personal.dropna(subset=['cic_id'])
dim_immigration_air.dropna(subset=['cic_id'])

fact_immigration.drop_duplicates(subset = 'cic_id', keep = 'first')
dim_immigration_personal.drop_duplicates(subset = 'cic_id', keep = 'first')
dim_immigration_air.drop_duplicates(subset = 'cic_id', keep = 'first')

# temperature data
df_temp_us.dropna()
df_temp_us.drop_duplicates(subset = 'dt', keep = 'first')

# demography data
df_demo_info.dropna()
df_demo_stat.dropna()
df_demo_info.drop_duplicates(subset = 'city', keep = 'first')
df_demo_stat.drop_duplicates(subset = 'city', keep = 'first')


####################################################################################
# Consolidate process data by function 27Dec2022
def Check_NaN(type, input_path):
    """ Explore dataset by pandas df and check NaN values and Duplicated if needed.
    """
    if type == 'temperature':
        df= pd.read_csv(input_path)
        print("to see first 05 rows of Temperature df ... \n")
        df.head()

        print("to check NaN values for entry Temperature df ... \n")
        print(df.isnull().sum())

        print("to check duplicate values in Temperature df ... \n")
        dup_count = len(df) - len(df.drop_duplicates())
        print("to count duplicated row in df Temper is {}".format(dup_count))

    if type == 'uscity':
        df = pd.read_csv(input_path, header=None, encoding="utf-8", sep=";")
        print("to see first 2 rows in df_UsCities ... \n")
        print(df.head(5))

        print("to check NaN values for entry df_UsCities ... \n")
        print(df.isnull().sum())
    
    if type == 'airport':
        df= pd.read_csv(input_path, header=None, encoding="utf-8")
        print("to see first 2 rows in df_Airport ... \n")
        print(df.head(5))

        print("to check NaN values for entry df_Airport ... \n")
        print(df.isnull().sum())

        # to check how many duplicated row in each df
        dup_count = len(df) - len(df.drop_duplicates())
        print("to count duplicated row in df_Airport is {}".format(dup_count))
# ==============================================================
def process_Immigra_data(spark, input_data):
    """ Function use to read all 12 sas7bat data files of Immigration datasets.
    Loading data files, check NaN and duplicate value, do cleaning data, and Loading into target table
    """

    # Immig_datapath= '../../data/18-83510-I94-Data-2016'
    namefiles = os.listdir(input_data)

    # return list of full path files
    files = [os.path.join(input_data, f) for f in namefiles if os.path.isfile(os.path.join(input_data, f))]

    # print("to see list of file name path files ->> \n")
    # print(files)

    # To union all files in Immigration data files path
    def unionAll(*dfs):
        return reduce(DataFrame.unionAll, dfs)

    i = 0
    print("To see accumulating df.count for 12 data files .... \n")
    for file in files:
    # tempo to test first 3 file
        if(i==3): 
            break
        if(i==0):
            dfS = spark.read.format('com.github.saurfang.sas.spark').load(file)
            cols= dfS.columns
            print(dfS.count())
        if(i>0):
            dfS = unionAll(dfS,spark.read.format('com.github.saurfang.sas.spark').load(file).select([col for col in cols]))
            print(dfS.count())
        i = i+1

    print("to see Schema of Immigra_data ... \n")
    dfS.printSchema()
    dfS.show(5)

    print("to show NaN values in dfS of Immigra_data... \n")
    dfS.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dfS.columns]).show()


    print("Droping NaN values here prior to  load to tables ...")

    # base on intention Fact and Dimension Table being created, select 'i94cit','i94addr' as subset when drop_nan
    NaNcount = dfS.count() - dfS.dropna(how='any', subset=['i94cit','i94addr']).count()

    print("To show how many rows is being dropped when filtered by i94cit and i94addr... {}".format(NaNcount))

    print("Droping NaN  ....... \n")
    dfS.dropna(how='any', subset=['i94cit','i94addr'])


    # Check duplicated and drop as subset specified
    print("to check any duplicated in dfS_ImmigAll Spark df ... \n")

    # columns = dfS.columns
    # if dfS.count() > dfS.dropDuplicates(columns).count():
    #   raise ValueError('Data in dfS Immigration data has duplicates ...')

    print("To Drop duplicate for Schema dfS_ImmigAll with subset 'cicid' ...  \n")
    dfS.drop_duplicates(subset='cicid').show()


    print("Loading data to table later ... ")


    print("Writing Parquet files for dfS  ... \n")
    # write to parquet when read all 12 files
    dfS.write.parquet("sas_data1")
    dfS=spark.read.parquet("sas_data1")


# ------------------------------------------------------------------------------
def process_UsCities_data(spark, input_data):

    # fname_UsCities = './us-cities-demographics.csv'
    dfS = spark.read.csv(input_data, sep=";")

    print("to see Spark Schema of UsCities ... \n")
    dfS.printSchema()
    dfS.show(2)
    

    # drop Duplicated value of dfS with subset filtering
    dup_count_dfS = dfS.count() - dfS.drop_duplicates(subset = 'City').count()
    print("to count duplicated row in UsCities  is {} \n".format(dup_count_dfS))

    print("Drop UsCities_Demo with subset 'city' ... by quantity \n {}")
    dfS.drop_duplicates(subset = 'City').show()


    print("Writing parquet file for dfS_UsCities ... \n")
    dfS.write.parquet("USCities_data2")
    dfS=spark.read.parquet("USCities_data2")

# ------------------------------------------------------------------------
def process_CityTemper_data(spark, input_data):

    dfS = spark.read.csv(input_data)
    print("to see how many rows in  dfS.count: {}".format(dfS.count()))
    
    print("to see Spark Schema of dfS ... \n")
    dfS.printSchema()
    dfS.show(2)

    # Handle with NaN in Temperature dfS
    print("Handle NaN and duplicate in Spark dfS Schema ... \n")

    # PySpark dfS.count() will count all row include NaN

    NaNcount = dfS.count() - dfS.dropna().count()
    print("How many NaN values in Temperature dfS ... {}".format(NaNcount))
    print("Droping NaN for Temperature Schema: dfS ... \n")
    dfS = dfS.dropna()

    print("Drop duplicate for Temperature data with subset 'dt' ... \n {}")
    dfS.drop_duplicates(subset = 'dt').show()

    print("Loading data to table at here later ... ")
    
    print("Writing parquet file for dfS ... \n")
    dfS.write.parquet("Temp_data2")
    dfS=spark.read.parquet("Temp_data2")
# -----------------------------------------------------------------------------
def process_AirPort_data(spark, input_data):
    
    dfS = spark.read.csv(input_data)
    print("to see Spark Schema of dfS ... \n")
    dfS.printSchema()
    dfS.show(2)
    
    print("Writing parquet file for dfS ... \n")
    dfS.write.parquet("Airport_data2")
    dfS=spark.read.parquet("Airport_data2")




df = df.select([col(c).alias(
        c.replace( '(', '')
        .replace( ')', '')
        .replace( ',', '')
        .replace( ';', '')
        .replace( '{', '')
        .replace( '}', '')
        .replace( '\n', '')
        .replace( '\t', '')
        .replace( ' ', '_')
    )])

