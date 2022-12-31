"""Module file dedicated for Loading process to target tables.
    + Input: 
        Spark DataFrame in flow of main()
        Star Schema diagram
    + Output: table will be written into parquet files
"""

# Lib use for convert ts to datetime iso format
import datetime as dt
from datetime import datetime

from pyspark.sql.functions import udf, col, monotonically_increasing_id, upper
from pyspark.sql.functions import year, month, dayofmonth, \
                                  dayofweek, date_format, \
                                  hour, weekofyear

# for self check table just crated
from pyspark.sql.functions import isnan, when, count, col

from pyspark.sql import SparkSession



def creat_Fact_Immigrant(dfS):
    """ creat the Fact_Immigrant_table from Immigration US data set i94 sas7dat files """
    # arrdate need to convert from ts to iso_type
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(float(x))).isoformat() if x else None)
    dfS = dfS.withColumn("arrdate", get_date(dfS.arrdate))
    dfS = dfS.withColumn("depdate", get_date(dfS.depdate))

    # to add unique PK for Fact table
    # add incresing_id to make PK for FactTable:
    dfS = dfS.withColumn('Immig_Fact_ID', monotonically_increasing_id())

    Fact_Immigrant_table = dfS.select('Immig_Fact_ID', \
                                       col('cicid').alias('cicid_Immigrant'),\
                                       col('i94port').alias('AirPortCode'),\
                                       col('arrdate').alias('ArriveDate'),\
                                       col('depdate').alias('DepartureDate'),\
                                       col('i94res').alias('FromResidence'),\
                                       col('visatype').alias('VisaType'), \
                                       col('admnum').alias('AdminNumber')      
                                    )
    print("Show schema of Fact_Immigrant table ...\n")
    Fact_Immigrant_table.printSchema()
    Fact_Immigrant_table.show(5)

    return Fact_Immigrant_table
    
# ________________vvv________________vvv________________vvv ________________vvv
def creat_D_Immigrant_detail(dfS):
    """ Creat D_Immigrant_detail working as a detail immigration people to US, data from from Immigration US data set.
   Use-case is intend to serving security investigation of each US-immigrated people in future"""

    # Simply pick from data frame in flow process the i94 data
    # cicid onsider as PK of this Dim table
    D_Immigrant_detail_table = dfS.select(col('cicid').alias('cicid_Immigrant'),\
                                            col('i94cit').alias('Citizenship'),\
                                            col('i94addr').alias('ArriveStateCode'),\
                                            col('depdate').alias('DepartureDate'),\
                                            col('i94mode').alias('ArriveMode'),\
                                            col('biryear').alias('BirthYear'),\
                                            col('gender').alias('Gender'),\
                                            col('fltno').alias('FlightNumber'),\
                                            col('matflag').alias('MatchArriveDeparture')
                                    )
    print("Show schema of D_Immigrant_detail_table ...\n")
    D_Immigrant_detail_table.printSchema()
    D_Immigrant_detail_table.show(3)
    return D_Immigrant_detail_table

# ________________vvv________________vvv________________vvv ________________vvv
def creat_D_WorldTemp(spark, dfS, port_mapto_city):
    """ Creat D_WorldTemp from data set GlobalLandTemperatureByCity for quering temperature or coordinate data in the city world"""

    #Rename for dt column
    D_Temperature_table = dfS.withColumnRenamed('dt','DateTime')

    # Create spark DataFrame from pd.df port_mapto_city
    port_mapto_city_df = spark.createDataFrame(port_mapto_city)

    # join 02 dfS by look-up city_name, keep outer at Temperature side when Joining
    output_temperature_dfS = port_mapto_city_df.join(D_Temperature_table, \
                                                        on=(port_mapto_city_df.CityName == D_Temperature_table.City),\
                                                        how='right_outer')
    

    output_temperature_dfS =  output_temperature_dfS.select(port_mapto_city_df.PortCode.alias('AirPortCode'), \
                                                            'DateTime', \
                                                            col('AverageTemperature').alias('AvgTemperature'), \
                                                            col('City').alias('CityName'), \
                                                            'Latitude', \
                                                            'Longitude'
                                                            )

    print("Show schema of new output_temperature_dfS  ...\n")
    output_temperature_dfS.printSchema()
    output_temperature_dfS.show(5)

    print("\n how many rows in new table ... {}".format(output_temperature_dfS.count()))

    # Optional observation, put comment for saving run time
    # NaNcount = output_temperature_dfS.count() - output_temperature_dfS.dropna(how='any', subset='AirPortCode').count()
    # print("\n verfiry any How many NULL in AirPortCode column {}".format(NaNcount))

    return output_temperature_dfS

# ________________vvv________________vvv________________vvv ________________vvv
def creat_D_USCities(spark, dfS, port_mapto_city):
    """ Creat D_USCities from data set us-cities-demographics """

    # rename column `City` and change to upper-case
    # D_USCities_table = dfS.withColumnRenamed('City','CityName')
    D_USCities_table = dfS.withColumn('City', upper(col('City')))

    # Create spark DataFrame from pd.df
    port_mapto_city_df = spark.createDataFrame(port_mapto_city)

    # join 02 dfS, keep whole CityName in us-cities data side
    output_city_df = port_mapto_city_df.join(D_USCities_table, \
                                               on=(port_mapto_city_df.CityName == D_USCities_table.City),\
                                                how='right_outer')

    # Adding the PortCode from i94port to target table
    output_city_df = output_city_df.select(port_mapto_city_df.PortCode.alias('AirPortCode'), \
                                            col('City').alias('CityName'), \
                                            col('State Code').alias('StateCode'), \
                                            col('State').alias('StateName'), \
                                            col('Median Age').alias('MedianAge'), \
                                            col('Total Population').alias('TotalPopulation'), \
                                            col('Number of Veterans').alias('Veterans'), \
                                            col('Foreign-born').alias('ForeignBorn'), \
                                            col('Average Household Size').alias('AvgHouseholdSize'), \
                                            col('Race').alias('Race'))

    print("Show schema of Us_citites table after Joining ...\n")
    output_city_df.printSchema()
    output_city_df.show(5)
    print("\n how many rows in new table ... {}".format(output_city_df.count()))

    NaNcount = output_city_df.count() - output_city_df.dropna(how='any', subset='AirPortCode').count()
    print("\n verfiry any How many NULL in AirPortCode column {}".format(NaNcount))

    return output_city_df

# ________________vvv________________vvv________________vvv ________________vvv
def creat_D_Airport(spark, dfS, port_mapto_city):
    """Creat D_Airport for detail information of airport, 
    join to Immigration table by AirPortCode (i94port), and city name that extracted from i94port also.
    """

    # move column `municipality` to  upper-case
    D_Airport_table = dfS.withColumn('municipality', upper(col('municipality')))

    # Create spark DataFrame from pd.df
    port_mapto_city_df = spark.createDataFrame(port_mapto_city)

    # Join two df, keep outer join at D_Airport_table staging side.
    output_city_df = port_mapto_city_df.join(D_Airport_table, \
                                               on=(port_mapto_city_df.CityName == D_Airport_table.municipality),\
                                               how='right_outer')

    # `ident` is served as unique ID value for AirPort table
    output_city_df = output_city_df.select( port_mapto_city_df.PortCode.alias('AirPortCode'), \
                                            col('ident').alias('AirPortID'), \
                                            col('type').alias('Type'), \
                                            col('name').alias('AirportName'), \
                                            col('municipality').alias('CityName'), \
                                            col('elevation_ft').alias('Elevation'), \
                                            col('iso_country').alias('Country'), \
                                            col('coordinates').alias('Coordinates')
                                          )


    print("Show schema of new D_Airport_table ...\n")
    output_city_df.printSchema()
    output_city_df.show(5)

    print("\n how many rows in new table ... {}".format(output_city_df.count()))

    NaNcount = output_city_df.count() - output_city_df.dropna(how='any', subset='AirPortCode').count()
    print("\n verfiry any How many NULL in AirPortCode column {}".format(NaNcount))

    return output_city_df

# ________________vvv________________vvv________________vvv ________________vvv
def creat_D_DateTime(dfS):
    """ Creat D_DateTime from Immigration US data set i94 sas7dat files.
    Input extract from arrdate of i94 data"""

    # Need to convert from timestamp in sas_data to iso format and extract to each field `Year, Month, Day, DayOfWeek`
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(float(x))).isoformat() if x else None)
    dfS = dfS.withColumn("arrdate", get_date(dfS.arrdate))
    dfS = dfS.withColumn("depdate", get_date(dfS.depdate))

    # Ater this arrdate and depdate will be looks like 2016-04-07, care only about arrdate, with no NaN consists
    # print("____________VVV_Debug info ... ... ... \n")
    # print("just for see dfS sas data after convert arrdate depdate to iso ... \n")
    # dfS.show(5)

    # Get the root column isodate , with distinct()
    D_DateTime_table = dfS.select(col('arrdate').alias('ArriveDate'), \
                                      year(col('arrdate')).alias('ArriveYear'), \
                                      month(col('arrdate')).alias('ArriveMonth'), \
                                      dayofmonth(col('arrdate')).alias('ArriveDay'), \
                                      date_format(col('arrdate'), "EEEE").alias('Arrive_DayOfWeek'), \
                                      col('depdate').alias('DepartureDate')
                                      )

    print("Show schema of D_DateTime_table ...\n")
    D_DateTime_table.printSchema()
    D_DateTime_table.show(3)
    return D_DateTime_table
