# etl

# Do all imports and installs here
import pandas as pd
from pyspark.sql import SparkSession

from functools import reduce
from pyspark.sql import DataFrame
import pyspark.sql.functions as f

# to check NaN for SparkDF
# from pyspark.sql.functions import isnan, when, count, col
from pyspark.sql.functions import count, col

import os
from os import listdir
from os.path import isfile, join

# Assistance functions
from schemas_assist.DataClean import cleaning_Immigra_data, \
											cleaning_Dim_Immigra, \
											cleaning_UsCities_data, \
											cleaning_Airport_data, \
											cleaning_CityTemper_data

from schemas_assist.CreatTables import creat_D_DateTime


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
		print("\n to see first 05 rows of Temperature df ... \n")
		print("temperature --------------------------------\n")
		print(df.head(3))
		print("-----------------------------------------\n")

		print("\n to check NaN values for entry Temperature df ... \n")
		print(df.isnull().sum())

	if type == 'uscity':
		df = pd.read_csv(input_path, header=None, encoding="utf-8", sep=";")
		print("\n to see first 2 rows in df_UsCities ... \n")
		print("uscity --------------------------------\n")
		print(df.head(3))
		print("-----------------------------------------\n")

		# print("\n to check NaN values for entry df_UsCities ... \n")
		# print(df.isnull().sum())
	
	if type == 'airport':
		df= pd.read_csv(input_path, header=None, encoding="utf-8")
		print("\n to see first 2 rows in df_Airport ... \n")
		print("airport --------------------------------\n")
		print(df.head(3))
		print("-----------------------------------------\n")

		# print("\n to check NaN values for entry df_Airport ... \n")
		# print(df.isnull().sum())




# ------------------------------------------------------------------------------------------------------------
# From output Step-1 exploring data, write up to docs for Steps2 
# From the Step2 docs: build flow of each function to process data in Step3

# ------------------------------------------------------------------------------------------------------------
# Step 3; build ETL process by each function define process data 

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
	print("\n To see accumulating df.count for 12 data files .... \n")
	for file in files:
		if(i==0):
			dfS = spark.read.format('com.github.saurfang.sas.spark').load(file)
			cols= dfS.columns
			print(dfS.count())
		if(i>0):
			dfS = unionAll(dfS,spark.read.format('com.github.saurfang.sas.spark').load(file).select([col for col in cols]))
			print(dfS.count())
		# tempo to saving time run test
		if(i==0): 
			break
		i = i+1

	print("\n Schema of Immigra_data ------------------------------------- \n")
	dfS.printSchema()
	dfS.show(3)

	# ________vvv Debug ....________vvv Debug ....________vvv Debug ....________vvv Debug ....
	print("Process creating Dimensional DateTime table here .... \n ")
	creat_D_DateTime(dfS)

	# ________vvv Debug ....________vvv Debug ....________vvv Debug ....________vvv Debug ....



	print("\n Process basic cleaning data for Immigration datasets ...")
	NaN_sublist=['i94cit']
	Dub_sublist=['cicid']
	dfS = cleaning_Immigra_data(dfS, NaN_sublist, Dub_sublist)


	print("Loading data to table later ... ")
	# creat fact table Fact_Immigrant 
	# 


	# Write parquet of Fact_Immigrant table
	# 

	# droping more NaN in detail information, prior to loading to  D_Immigrant_detail
	print("\n Droping NaN values for Dim Immigration detail info ... \n")

	# people without citizenship and flight number info will be rejected to investing more
	# NaN_sublist = ['fltno']
	dfS = cleaning_Dim_Immigra(dfS, NaN_subset = ['fltno'])


	# creat dimensional table D_Immigrant_detail
	# 


	# Write parquet of D_Immigrant_detail table
	# 



	# print("\n Writing and Reading Parquet partitionBy `i94yr`... \n")
	# dfS.write\
	#	.mode('overwrite')\
	#	.partitionBy('i94yr')\
	#	.parquet("sas_data1")
	# dfS=spark.read.parquet("sas_data1")

# end of process_Immigra_data(xxx)
# ------------------------------------------------------------------------------------------------------------
def process_UsCities_data(spark, input_data):

	# dfS = spark.read.csv(input_data, sep=";")
	dfS = spark.read.options(header='True', inferSchema='True')\
					.csv(input_data, sep=";")

	print("Schema of UsCities ------------------------------------------ \n")
	dfS.printSchema()
	dfS.show(2)

	
	# UsCities consider no need to do NaN drop
	dfS = cleaning_UsCities_data(dfS, Dup_subset=['City'])


	# before write parquet, rename invalid characters in column name
	dfS = dfS.select([col(c).alias(
		c.replace( '(', '')
		.replace( ')', '')
		.replace( ',', '')
		.replace( ';', '')
		.replace( '{', '')
		.replace( '}', '')
		.replace( '\n', '')
		.replace( '\t', '')
		.replace( ' ', '_')
	) for c in dfS.columns])

	# print("Writing and Reading Parquet files for dfS_UsCities : partitionBy 'Race' ... \n")
	# Using City and State Code as the key
	# dfS.write\
	#	.mode('overwrite')\
	#	.partitionBy('Race')\
	#	.parquet("USCities_data2")
	# dfS=spark.read.parquet("USCities_data2")
# ---------------------------------------------------------------
def process_AirPort_data(spark, input_data):
	
	dfS = spark.read.options(header='True', inferSchema='True')\
					.csv(input_data)

	print("\n Schema of dfS Airport --------------------------------------- \n")
	dfS.printSchema()
	dfS.show(2)

	dfS = cleaning_Airport_data(dfS, Dup_subset=['ident', 'name'])


	# print("\n Writing and Reading Parquet files for dfS Airport partitionBy iso_country... \n")
	# dfS.write\
	#	.mode('overwrite')\
	#	.partitionBy('iso_country')\
	#	.parquet("Airport_data2")
	# dfS=spark.read.parquet("Airport_data2")
# -----------------------------------------------------------------------------
def process_CityTemper_data(spark, input_data):

	dfS = spark.read.options(header='True', inferSchema='True')\
					.csv(input_data)
	print("to see how many rows in  dfS.count: {}".format(dfS.count()))
	
	print("\n Schema of dfS---------------------------------------------- \n")
	dfS.printSchema()
	dfS.show(2)


	dfS = cleaning_CityTemper_data(dfS, NaN_subset=['AverageTemperature'], Dup_subset=['City'])


	print("Loading data to table at here later ... ")
	
	# print("Writing and Reading Parquet partitionBy `Country` ... \n")
	# dfS.write\
	#	.mode('overwrite')\
	#	.partitionBy('Country')\
	#	.parquet("Temp_data2")
	# dfS=spark.read.parquet("Temp_data2")
# -----------------------------------------------------------------------------
# Step 3: Define the Data Model: Done by Schema diagram and Desciption in Jupyter NoteBook


# -----------------------------------------------------------------------------
# Step 4: Run Pipelines to Model the Data 
## 4.1 Create the data model


## 4.2 Data Quality Checks


def main():
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
	# Check_NaN('temperature', input_CityTemper_data)
	# Check_NaN('uscity', input_UsCities_data)
	# Check_NaN('airport', input_AirPort_data)


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

""" 	process_UsCities_data(spark, input_UsCities_data )

	process_AirPort_data(spark, input_AirPort_data )
	process_CityTemper_data(spark, input_CityTemper_data ) """

	# Step 4: Run Pipelines to Model the Data
	# ---------------------------------------------


if __name__ == "__main__":
	main()
