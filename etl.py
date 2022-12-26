# etl

# Do all imports and installs here
import pandas as pd
from pyspark.sql import SparkSession

from functools import reduce
from pyspark.sql import DataFrame
import pyspark.sql.functions as f

# to check NaN for SparkDF
from pyspark.sql.functions import isnan, when, count, col

import os
from os import listdir
from os.path import isfile, join


# -----------------------------------------------------------------------------
#  To read dase sets file by PySpark, to use PySpark.sql and functions later
# dataframe read by Spark, name starting withg dfS_
def create_spark_session():
	spark = SparkSession.builder.\
		config("spark.jars.repositories", "https://repos.spark-packages.org/").\
		config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
		enableHiveSupport().getOrCreate()
	return spark


# -----------------------------------------------------------------------------
# Step 1: Scope the Project and Gather Data
def Check_NaN(type, input_path):
	""" Explore dataset by pandas df and check NaN values and Duplicated if needed.
	"""
	if type == 'temperature':
		df= pd.read_csv(input_path)
		print("to see first 05 rows of Temperature df ... \n")
		df.head()

		print("to check NaN values for entry Temperature df ... \n")
		print(df.isnull().sum())

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

# ------------------------------------------------------------------------------------------------------------
# From output Step-1 exploring data, write up to docs for Steps2 
# From the Step2 docs: build flow of each function to process data in Step3
# ------------------------------------------------------------------------------------------------------------

# Step 3; build ETL process by each function define process data 
# Read multiple data sas7dat file of immigration datasets
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
    # 	raise ValueError('Data in dfS Immigration data has duplicates ...')

    # ----------------------------------------------------------------------
	print("should adding the Loading data to table later ... ")


	print("Writing and Reading Parquet files  for dfS  ... \n")
	dfS.write.parquet("sas_data1")
	dfS=spark.read.parquet("sas_data1")

# ------------------------------------------------------------------------------------------------------------

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


	print("Writing and Reading Parquet files for dfS_UsCities ... \n")
	dfS.write.parquet("USCities_data2")
	dfS=spark.read.parquet("USCities_data2")
# ---------------------------------------------------------------
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
	
	print("Writing and Reading Parquet for dfS ... \n")
	dfS.write.parquet("Temp_data2")
	dfS=spark.read.parquet("Temp_data2")
# -----------------------------------------------------------------------------
def process_AirPort_data(spark, input_data):
	
	dfS = spark.read.csv(input_data)
	print("to see Spark Schema of dfS ... \n")
	dfS.printSchema()
	dfS.show(2)
	
	print("Writing and Reading Parquet files for dfS ... \n")
	dfS.write.parquet("Airport_data2")
	dfS=spark.read.parquet("Airport_data2")
# -----------------------------------------------------------------------------

# Step 3: Define the Data Model


# Step 4: Run Pipelines to Model the Data 
## 4.1 Create the data model


## 4.2 Data Quality Checks


def main():
    spark = create_spark_session()

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


    # Step1 Explore Data and NaN 
    # --------------------------
    Check_NaN('temperature', input_CityTemper_data)
    Check_NaN('uscity', input_UsCities_data)
    Check_NaN('airport', input_AirPort_data)


    # Step2: Do write up the doc
    # --------------------------

    # Step3: Build a complete ETL process
    # -----------------------------------
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

    # Step 4: Run Pipelines to Model the Data
    # ------------------------------------------


if __name__ == "__main__":
    main()
