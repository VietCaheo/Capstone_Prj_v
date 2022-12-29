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

# TO GET
# import sqlContext.implicits._

# Assistance functions
from schemas_assist.DataClean import cleaning_Immigra_data, \
                                            cleaning_Dim_Immigra, \
                                            cleaning_UsCities_data, \
                                            cleaning_Airport_data, \
                                            cleaning_CityTemper_data
from schemas_assist.DataClean import extract_CityName

from schemas_assist.CreatTables import creat_D_DateTime, \
                                        creat_D_Immigrant_detail, \
                                        creat_Fact_Immigrant, \
                                        creat_D_USCities, \
                                        creat_D_Airport



# -----------------------------------------------------------------------------
#  To read dase sets file by PySpark, to use PySpark.sql and functions later
# dataframe read by Spark, name starting withg dfS_
def create_spark_session():
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        enableHiveSupport().getOrCreate()
    return spark


#____________________vvv____________________vvv____________________vvv____________________vvv
# Step 1: Scope the Project and Gather Data
def Pd_Read_datasets(type, input_path):
    """ Explore dataset by pandas df and check NaN values and Duplicated if needed.
    This function almost for explore datasets and check number NaN and duplicated value
    """
    if type == 'temperature':
        print("pd reading data set temperature ... \n")
        df= pd.read_csv(input_path)
        # print(df.head(3))

        print("\n to check NaN values for entry Temperature df ... \n")
        print(df.isnull().sum())

    cities_name = []
    if type == 'uscity':
        print("pd reading data set uscity ... \n")
        df = pd.read_csv(input_path, encoding="utf-8", sep=";")
        # print(df.head(3))

        cities_name = df["City"].unique()
        # print("to see list city name ... {}".format(cities_name))
        print("to see how many city name without duplicate {}".format(len(cities_name)))

        return cities_name

        # print("\n to check NaN values for entry df_UsCities ... \n")
        # print(df.isnull().sum())
    
    if type == 'airport':
        print("pd reading data set airport ... \n")
        df= pd.read_csv(input_path, header=None, encoding="utf-8")
        # print(df.head(3))

        # print("\n to check NaN values for entry df_Airport ... \n")
        # print(df.isnull().sum())


# ------------------------------------------------------------------------
# From output Step-1 exploring data, write up to docs for Steps2 
# From the Step2 docs: build flow of each function to process data in Step3

# ------------------------------------------------------------------------
# Step 3; build ETL process by each function define process data 
#____________________vvv____________________vvv____________________vvv____________________vvv
def MultipleRead_sas_files(spark, input_da_path, file_to_load=1):
    """ An auxliary function to handling muliple read sas data file.
    Input given by sas data path files.
    Output is Spark Data Frame after read raw sas data files
    Optional to select How many file was read for saving run time to test."""

    # Immig_datapath= '../../data/18-83510-I94-Data-2016'
    namefiles = os.listdir(input_da_path)

    # return list of full path files
    files = [os.path.join(input_da_path, f) for f in namefiles if os.path.isfile(os.path.join(input_da_path, f))]

    # To union all files in Immigration data files path
    def unionAll(*dfs):
        return reduce(DataFrame.unionAll, dfs)

    i = 0
    for file in files:
        if(i==0):
            dfS = spark.read.format('com.github.saurfang.sas.spark').load(file)
            cols= dfS.columns
            print(dfS.count())
        if(i>0):
            dfS = unionAll(dfS,spark.read.format('com.github.saurfang.sas.spark').load(file).select([col for col in cols]))
            print(dfS.count())
        # break accumulating as expected file_to_load to run test
        if((file_to_load-1) == i): 
            break
        i = i+1
    
    return dfS

#____________________vvv____________________vvv____________________vvv____________________vvv
def process_Immigra_data(spark, input_data):
    """ Function use to do as ETL sequences for tables: Fact_Immigrant; D_Immigrant_detail; D_DateTime
        -> Read data sets to build schema
        -> Implement data cleaning (if needed)
        -> Write to parquet files
        -> Read parquet for verification.
    """

    sas_files_toload = 1
    print("\n Accumulating Read into sas_data Schema for {} sas data files .... \n".format(sas_files_toload))
    # Run test only first file for saving run time
    dfS = MultipleRead_sas_files(spark, input_data, sas_files_toload)

    print("\n Schema of staging Immigra_data ------------------------------------- \n")
    dfS.printSchema()
    dfS.show(5)

    print("\n Loading data to table .... \n")
    D_DateTime_table = creat_D_DateTime(dfS)

    print("\n Write parquet of D_DateTime_table table  ... ... \n")
    D_DateTime_table.write \
                      .mode('overwrite') \
                      .partitionBy('ArriveYear') \
                      .parquet("D_DateTime_table_par")
    D_DateTime_table=spark.read.parquet("D_DateTime_table_par")
    print("Finished writing parquet of D_DateTime_table \n")

    # Filter list is depend on user's purpose in future use-case
    print("\n Process basic cleaning data before load to Fact_Immigrant table  ...")
    NaN_sublist=['i94cit']
    Dup_sublist=['cicid']
    dfS = cleaning_Immigra_data(dfS, NaN_sublist, Dup_sublist)

    print("\n Loading data to table .... \n")
    Fact_Immigrant_table = creat_Fact_Immigrant(dfS)

    print("Writing parquet files for Fact_Immigrant_table ... \n")
    Fact_Immigrant_table.write \
                        .mode('overwrite') \
                        .partitionBy('VisaType') \
                        .parquet("Fact_Immigrant_table_par")
    Fact_Immigrant_table=spark.read.parquet("Fact_Immigrant_table_par")

    
    # Do more cleaning Data Dim_table of Immigration info
    print("\n Droping NaN values for Dim Immigration detail info ... \n")
    dfS = cleaning_Dim_Immigra(dfS, NaN_subset = ['fltno'])

    print("\n Loading data to table .... \n")
    D_Immigrant_detail_table = creat_D_Immigrant_detail(dfS)

    
    # Write parquet of D_Immigrant_detail table
    # Immigrant_detail_table_par
    D_Immigrant_detail_table.write \
                              .mode('overwrite') \
                              .partitionBy('ArriveMode') \
                              .parquet("Immigrant_detail_table_par")
    D_Immigrant_detail_table=spark.read.parquet("Immigrant_detail_table_par")

    print("Finished the process for Immigration Data ... \n")

#____________________vvv____________________vvv____________________vvv____________________vvv
def process_UsCities_data(spark, input_data, port_mapto_city):
    """ Process us-cities data file for get dataframe for each city information.
    """

    # dfS = spark.read.csv(input_data, sep=";")
    dfS = spark.read.options(header='True', inferSchema='True')\
                    .csv(input_data, sep=";")

    print("Schema of staging UsCities ..... \n")
    dfS.printSchema()
    dfS.show(3)

    # UsCities consider no need to do NaN drop
    dfS = cleaning_UsCities_data(dfS, Dup_subset=['City'])

    print("\n Loading data to table .... \n")
    D_USCities_table = creat_D_USCities(spark, dfS, port_mapto_city)


    print("Start Writing parquet files for D_USCities_table ... \n")
    D_USCities_table.write \
                        .mode('overwrite') \
                        .partitionBy('StateCode') \
                        .parquet("D_USCities_table_par")
    D_USCities_table=spark.read.parquet("D_USCities_table_par")


    # before write parquet, rename invalid characters in column name
    # dfS = dfS.select([col(c).alias(
    #     c.replace( '(', '')
    #     .replace( ')', '')
    #     .replace( ',', '')
    #     .replace( ';', '')
    #     .replace( '{', '')
    #     .replace( '}', '')
    #     .replace( '\n', '')
    #     .replace( '\t', '')
    #     .replace( ' ', '_')
    # ) for c in dfS.columns])

    print("Finished for us-cities data file.")

#____________________vvv____________________vvv____________________vvv____________________vvv
def process_AirPort_data(spark, input_data, port_mapto_city):
    
    dfS = spark.read.options(header='True', inferSchema='True')\
                    .csv(input_data)

    print("\n Schema of staging Airport ----------------\n")
    dfS.printSchema()
    dfS.show(3)

    dfS = cleaning_Airport_data(dfS, Dup_subset=['ident', 'name'])

    print("\n Loading data to table .... \n")
    D_AirPort_table = creat_D_Airport(spark, dfS, port_mapto_city)


    print("\n Writing and Reading Parquet files for dfS Airport ... ... \n")
    D_AirPort_table.write\
                   .mode('overwrite')\
                   .partitionBy('Type')\
                   .parquet("Airport_parquet")
    D_AirPort_table=spark.read.parquet("Airport_parquet")
# -----------------------------------------------------------------------------
def process_CityTemper_data(spark, input_data):

    dfS = spark.read.options(header='True', inferSchema='True')\
                    .csv(input_data)
    print("to see how many rows in  dfS.count: {}".format(dfS.count()))
    
    print("\n Schema of staging Temp dfS-------------------------- \n")
    dfS.printSchema()
    dfS.show(2)

    dfS = cleaning_CityTemper_data(dfS, NaN_subset=['AverageTemperature'], Dup_subset=['City'])

    print("Loading data to table at here later ... ")

# Step 4: Run Pipelines to Model the Data 
## 4.1 Create the data model


## 4.2 Data Quality Checks

#____________________vvv____________________vvv____________________vvv____________________vvv
def main():
    spark = create_spark_session()

    # 12 files of Immigration Datasets, consider to limit data when run test for time saving
    input_Immig_data= '../../data/18-83510-I94-Data-2016'
    input_UsCities_data = './us-cities-demographics.csv'
    input_AirPort_data  = './airport-codes_csv.csv'

    # This file with more than 1mil rows
    input_CityTemper_data ='../../data2/GlobalLandTemperaturesByCity.csv'
    i94_sas_Labels_Descriptions = './I94_SAS_Labels_Descriptions.SAS'

    # ---------------------------------------------------------------------------
    # org_cities_name = []
    # std_cities_name = []

    # org_cities_name = Pd_Read_datasets('uscity', input_UsCities_data)

    # for city in org_cities_name:
    #     # city = city.replace(" ", "").upper()
    #     city = city.upper()
    #     std_cities_name.append(city)

    # print("to check some ele after remove space and make upper() ... \n {} \n {} \n {}".format(std_cities_name[0], std_cities_name[3], std_cities_name[2]))
    # ---------------------------------------------------------------------------
    # std_cities_name looks like [SILVERSPRING, RANCHOCUCAMONGA, ...], use it for city name look-up in extract_CityName as W1

    # Process the Labels_Descriptions file
    #  -> W1: try lookup by independent list: std_cities_name  extract_CityName(i94_sas_Labels_Descriptions, std_cities_name) !!
    #  -> W2: Create a df from sas_Labels_Description: and use this df for sql join later
    
    print("Start process the SAS labels Description file ... \n")
    port_mapto_city = extract_CityName(i94_sas_Labels_Descriptions)

    # Process one by one data set source file for ETL
    print("\n ----------------------------------------------------")
    print("Start process the Immigration Data sas7dat ... \n")
    process_Immigra_data(spark, input_Immig_data)

    print("\n ----------------------------------------------------")
    print("Start process the UsCities Demographic data ... \n")
    process_UsCities_data(spark, input_UsCities_data, port_mapto_city)

    print("\n ----------------------------------------------------")
    print("Start process the Airport data ... \n")
    process_AirPort_data(spark, input_AirPort_data, port_mapto_city)

    # print("\n ----------------------------------------------------")
    # print("Start process the World Temperature data ... \n")
    # process_CityTemper_data(spark, input_CityTemper_data )


if __name__ == "__main__":
    main()
