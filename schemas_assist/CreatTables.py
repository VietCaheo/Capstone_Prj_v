"""Module file dedicated for Loading process to target tables.
    + Input: 
        Spark DataFrame in flow of main()
        Star Schema diagram
    + Output: table will be written into parquet files
"""

# Lib use for convert ts to datetime iso format
import datetime as dt
from datetime import datetime

from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, \
                                  dayofweek, date_format, \
                                  hour, weekofyear



def creat_Fact_Immigrant(dfS):
    """ creat the Fact_Immigrant from Immigration US data set i94 sas7dat files """

    
# ________v________v________v________v________v________v________v________v
def creat_D_Immigrant_detail(dfS_i94sas):
    """ Creat D_Immigrant_detail working as a detail immigration people to US, data from from Immigration US data set.
   Use-case is intend to serving security investigation of each US-immigrated people in future"""

    # Simply pick from data frame in flow process the i94 data
    D_Immigrant_detail = dfS_i94sas.select(col('cicid').alias('cicid_Immigrant'),\
                                            col('i94cit').alias('Citizenship'),\
                                            col('depdate').alias('DepartureDate'),\
                                            col('i94mode').alias('ArriveMode'),\
                                            col('biryear').alias('BirthYear'),\
                                            col('gender').alias('Gender'),\
                                            col('fltno').alias('FlightNumber'),\
                                            col('matflag').alias('MatchArriveDeparture'))
# ________v________v________v________v________v________v________v________v
def creat_D_WorldTemp(dfS_Temp):
    """ Creat D_WorldTemp from data set GlobalLandTemperatureByCity"""

    #  Looks like the `dt` is timestamp type that supported by spark, could directly use to adding to table
    # a FK is city_code got from joining by a auxiliary dfS just for get city_code and country_code in  i94_label_description data
    D_WorldTemp =  dfS_Temp.select(col('dt').alias('DateTime'), \
                                    col('AverageTemperature').alias('AvgTemperature'), \
                                    col('City').alias('CityName'), \
                                    col('Latitude').alias('Latitude'), \
                                    col('Longitude').alias('Longtitude'), \
                                    col('city_code xxxx')
                                )

# ________v________v________v________v________v________v________v________v
def creat_D_USCities(dfS_USCities):
    """ Creat D_USCities from data set us-cities-demographics """

    D_USCities = dfS_USCities.select(col('').alias('CityName'), \
                                        col('').alias('StateCode'), \
                                        col('').alias('StateName'), \
                                        col('').alias('MedianAge'), \
                                        col('').alias('TotalPopulation'), \
                                        col('').alias('Veterans'), \
                                        col('').alias('ForeignBorn'), \
                                        col('').alias('AvgHouseholdSize'), \
                                        col('').alias('Race'), \
                                        col('city_code xxxx'))

# ________v________v________v________v________v________v________v________v
def creat_D_DateTime(dfS_i94sas):
    """ Creat D_DateTime from Immigration US data set i94 sas7dat files.
    Input extract from arrdate of i94 data"""

    # Should select get datetime data from a dataset and extract to each field `Year, Month, Day, DayOfWeek`

    # Need to convert from timestamp in sas_data to iso format
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(float(x))).isoformat() if x else None)
    dfS_i94sas = dfS_i94sas.withColumn("arrdate", get_date(dfS_i94sas.arrdate))
    dfS_i94sas = dfS_i94sas.withColumn("depdate", get_date(dfS_i94sas.depdate))

    # ater this arrdate and depdate will be looks like 2016-04-07, care only about arrdate, with no NaN consists

    # print("____________VVV____________VVV____________VVV____________VVV ... \n")
    # print("just for see dfS sas data after convert arrdate depdate to iso ... \n")
    # dfS_i94sas.show(5)

    # print("____________VVV____________VVV____________VVV____________VVV ... \n")


    # get the root column isodate , with distinct()
    D_DateTime_df = dfS_i94sas.select(col('arrdate').alias('ArriveDate'), \
                                      year(col('arrdate')).alias('Year'), \
                                      month(col('arrdate')).alias('Month'), \
                                      dayofmonth(col('arrdate')).alias('Day'), \
                                      date_format(col('arrdate'), "EEEE").alias('DayOfWeek'), \
                                      )
    
    print("Just to check DateTime_Df after do convert time format ... \n")
    D_DateTime_df.show(10)

    return D_DateTime_df

    # add more columns that derivates from above `ArriveDate`
    # D_DateTime_df = D_DateTime_df.withColumn(year)

