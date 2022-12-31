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
                                        creat_D_Airport, \
                                        creat_D_WorldTemp
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

    before_parquets_write = D_DateTime_table.count()

    print("\n Write parquet of D_DateTime_table table  ... ... \n")
    D_DateTime_table.write \
                      .mode('overwrite') \
                      .partitionBy('ArriveYear') \
                      .parquet("D_DateTime_table_par")
    D_DateTime_table=spark.read.parquet("D_DateTime_table_par")

    after_parquets_read = D_DateTime_table.count()
    
    Data_QualityCheck(before_parquets_write, after_parquets_read)

    # Filter list is depend on user's purpose in future use-case
    print("\n Process basic cleaning data before load to Fact_Immigrant table  ...")
    NaN_sublist=['i94cit']
    Dup_sublist=['cicid']
    dfS = cleaning_Immigra_data(dfS, NaN_sublist, Dup_sublist)

    print("\n Loading data to table .... \n")
    Fact_Immigrant_table = creat_Fact_Immigrant(dfS)

    before_par_write_FactImm = Fact_Immigrant_table.count()

    print("Writing parquet files for Fact_Immigrant_table ... \n")
    Fact_Immigrant_table.write \
                        .mode('overwrite') \
                        .partitionBy('VisaType') \
                        .parquet("Fact_Immigrant_table_par")
    Fact_Immigrant_table=spark.read.parquet("Fact_Immigrant_table_par")

    after_par_read_FactImm = Fact_Immigrant_table.count()

    Data_QualityCheck(before_par_write_FactImm, after_par_read_FactImm)

    
    # Do more cleaning Data Dim_table of Immigration info
    print("\n Droping NaN values for Dim Immigration detail info ... \n")
    dfS = cleaning_Dim_Immigra(dfS, NaN_subset = ['fltno'])

    print("\n Loading data to table .... \n")
    D_Immigrant_detail_table = creat_D_Immigrant_detail(dfS)

    before_par_write_DimImm = D_Immigrant_detail_table.count()

    # Write parquet of D_Immigrant_detail table
    # Immigrant_detail_table_par
    D_Immigrant_detail_table.write \
                              .mode('overwrite') \
                              .partitionBy('ArriveMode') \
                              .parquet("Immigrant_detail_table_par")
    D_Immigrant_detail_table=spark.read.parquet("Immigrant_detail_table_par")

    after_par_read_DimImm = D_Immigrant_detail_table.count()

    Data_QualityCheck(before_par_write_DimImm, after_par_read_DimImm)

    print("Finished process Immigration Data \n")

    # return a list of dfS from Fact and Dim Imm
    return [Fact_Immigrant_table, D_Immigrant_detail_table]

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

    before_par_write_UScity = D_USCities_table.count()

    print("Start Writing parquet files for D_USCities_table ... \n")
    D_USCities_table.write \
                        .mode('overwrite') \
                        .partitionBy('StateCode') \
                        .parquet("D_USCities_table_par")
    D_USCities_table=spark.read.parquet("D_USCities_table_par")

    after_par_read_USCity = D_USCities_table.count()

    Data_QualityCheck(before_par_write_UScity, after_par_read_USCity)

    print("Finished for us-cities data file.")

    return D_USCities_table
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

    before_parquets_write = D_AirPort_table.count()

    print("\n Writing and Reading Parquet files for dfS Airport ... ... \n")
    D_AirPort_table.write\
                   .mode('overwrite')\
                   .partitionBy('Type')\
                   .parquet("Airport_parquet")
    D_AirPort_table=spark.read.parquet("Airport_parquet")

    after_parquets_read = D_AirPort_table.count()

    Data_QualityCheck(before_parquets_write, after_parquets_read)
# -----------------------------------------------------------------------------
def process_CityTemper_data(spark, input_data, port_mapto_city):

    dfS = spark.read.options(header='True', inferSchema='True')\
                    .csv(input_data)
    print("to see how many rows in  dfS.count: {}".format(dfS.count()))
    
    print("\n Schema of staging Temp dfS-------------------------- \n")
    dfS.printSchema()
    dfS.show(2)

    dfS = cleaning_CityTemper_data(dfS, NaN_subset=['AverageTemperature'], Dup_subset=['City'])

    print("\n Loading data to table .... \n")
    D_WorldTemp_table = creat_D_WorldTemp(spark, dfS, port_mapto_city)

    before_parquets_write = D_WorldTemp_table.count()

    print("\n Writing and Reading Parquet files for dfS WorldTemp ... \n")
    D_WorldTemp_table.write\
                     .mode('overwrite')\
                     .partitionBy('CityName')\
                     .parquet("WorldTemp_parquet")
    D_WorldTemp_table=spark.read.parquet("WorldTemp_parquet")

    after_parquets_read = D_WorldTemp_table.count()

    Data_QualityCheck(before_parquets_write, after_parquets_read)

    return D_WorldTemp_table

#____________________vvv____________________vvv____________________vvv____________________vvv
def Data_QualityCheck(before_write, after_write):
    """ Function to check NUmber of table_Df has to write Parquet files is consistent or not
    """

    print("Do Quality check: data existing and data consistency... \n")
    if(0 == before_write) or (0 == after_write):
        raise ValueError("Fail to Loading data to table or Write to parquet files ...")
        
    if (before_write != after_write):
        raise ValueError("Data is inconsistent before and after write Df target-table into parquet files ... \n ")
    else:
        print("Data is Existing in target tables and is Consistent before and after Write parquet. OK NOW, MOVE NEXT! ")
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
    # This for debug to see City Name as extracted from sas_labels_description file, then look-up for CityName on each dataset for visual check.
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

    Immig_Result=[]
    # Process one by one data set source file for ETL
    print("\n ----------------------------------------------------")
    print("Start process the Immigration Data sas7dat ... \n")
    Immig_Result = process_Immigra_data(spark, input_Immig_data)
    Fact_Imm_df = Immig_Result[0]
    D_Imm_df = Immig_Result[1]

    print("\n ----------------------------------------------------")
    print("Start process the UsCities Demographic data ... \n")
    UsCities_df = process_UsCities_data(spark, input_UsCities_data, port_mapto_city)

    print("\n ----------------------------------------------------")
    print("Start process the Airport data ... \n")
    process_AirPort_data(spark, input_AirPort_data, port_mapto_city)

    print("\n ----------------------------------------------------")
    print("Start process the World Temperature data ... \n")
    WorldTemp_dfS = process_CityTemper_data(spark, input_CityTemper_data, port_mapto_city)
    print("FINISHED PROCESS ALL DATA SET INCLUDE DATA QUALITY CHECK \n")

    #Show out evidence of datasets large enough
    print("show out evidence of WorldTemp dataset with > 1mils rows .. \n")
    print("Please note, Final Temp table is droped NaN and duplicated by `City` above ...\n")
    print("Total of row in WorldTemp data set is 364130 + 8235082 + {}".format(WorldTemp_dfS.count()))

    print("show out evidence of Immigration dataset is large > 1mils row ... \n")
    print("This is number of row of Fact_Immigration table by staging only one sas7bdat file {}\n \n".format(Fact_Imm_df.count()))

    # Bulid Demo Query for ForeignBorn in D_UsCities:
    # Create a query to group count immigrants by their cities and include the "foreign_born" field in this city. The results could be ordered by the immigrant counts to see if cities with the most immigrants are indeed cities with the largest foreign-born counts.
    print("Do SQL command to: group count immigrants by their cities and include the foreign_born field in this city. The results could be ordered by the immigrant counts to see if cities with the most immigrants are indeed cities with the largest foreign-born counts \n \n ")
    Fact_Imm_df.createOrReplaceTempView("Fact_Imm")
    D_Imm_df.createOrReplaceTempView("D_Imm")
    UsCities_df.createOrReplaceTempView("UsCities")

    sqlDF = spark.sql(""" SELECT COUNT(*) AS count_F_Immg
                        FROM Fact_Imm AS f_imm
                        JOIN UsCities AS city
                        ON f_imm.AirPortCode = city.AirPortCode
                        JOIN D_Imm AS d_imm
                        ON f_imm.cicid_Immigrant = d_imm.cicid_Immigrant
                        GROUP BY city.ForeignBorn
                        ORDER BY count_F_Immg DESC
                    """)
    sqlDF.show(50)
    
if __name__ == "__main__":
    main()
