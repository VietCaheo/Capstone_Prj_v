"""Module DataClean to do auxilliary jobs before loading data to target tables such as:
	+ Data cleaning
	+ Handling  specified case in data
"""

import pyspark.sql.functions as f

# to check NaN for SparkDF
from pyspark.sql.functions import isnan, when, count, col

#__________vv__________vv__________vv__________vv
# Basic clean prior to loading to Fact table
def cleaning_Immigra_data(dfS, NaN_subset=[], Dup_subset=[]):
	""" Do Data Cleaning for Immigration datasets.
		Identify columns consist the NaN value, duplicated, and handle them
	"""

	print("\n show NaN values in dfS of Immigra_data...")
	dfS.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dfS.columns]).show()

	# ---------------------------------------------------
	# Handle with NaN
	print("\n Droping NaN values here prior to  load to tables ... \n")

	# base on intention Fact and Dimension Table being created, select 'i94cit', as subset when drop_nan
	NaNcount = dfS.count() - dfS.dropna(how='any', subset=NaN_subset).count()

	print("\n To show how many rows is being dropped when filtered by i94cit and ... {}".format(NaNcount))

	print("Droping NaN  ....... \n")
	cleaned_dfS = dfS.dropna(how='any', subset=NaN_subset)
	# ---------------------------------------------------

	# Handle with Duplicated
	# Check duplicated and drop as subset specified
	print("to check any duplicated in dfS_ImmigAll ... \n")

	print("\n Number of Immigra before drop_duplicates {} \n".format(cleaned_dfS.count()))
	print("\n Drop duplicated by cicid for make sure cicid is unique for each Immigrant Info...")
	cleaned_dfS = cleaned_dfS.drop_duplicates(Dup_subset)
	# dfS.show(3)
	print("\n Number of Immigra after drop_duplicates {} \n".format(cleaned_dfS.count()))

	return cleaned_dfS

#__________vv__________vv__________vv__________vv
# Drop more NaN for interested field for dim table
# subset list might use `i94addr`
def cleaning_Dim_Immigra(dfS, NaN_subset=[], Dup_subset=[]):
	""" Immagine Dim table using for detail security investigation in future.
	So, some field will be droped if it NaN. using kwarg to make sure user known which field will be rejected with NaN
	"""

	# base on intention Fact and Dimension Table
	NaNcount = dfS.count() - dfS.dropna(how='any', subset=NaN_subset).count()

	print("\n How many rows were dropped with subset {} ... {}".format(NaN_subset, NaNcount))

	print("Droping NaN  ....... \n")
	cleaned_dfS = dfS.dropna(how='any', subset=NaN_subset)

	# Duplicate value , sofar dont care for dim table

	return cleaned_dfS

#__________vv__________vv__________vv__________vv
def cleaning_UsCities_data(dfS, NaN_subset=[], Dup_subset=[]):
	""" Fuction to cleaning data for UsCites datasets
	"""

	print("\n Show NaN values in dfS of UsCities_data...")
	dfS.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dfS.columns]).show()

	# Handle with NaN
	# Number of NaN value in this UsCities is small, and no need to drop
	# Some important field no NaN are: City/ State/ State Code/ 

	# Handle with Duplicated
	# Make sure `City` is unique for this Df
	print("----------------------------------------------------------------------------")
	print("\n Rows number in UsCities before drop_duplicates with subset{} {} \n".format(Dup_subset, dfS.count()))
	print("Drop duplicated with subset {}} ...  \n".format(Dup_subset))
	cleaned_dfS = dfS.drop_duplicates(subset = Dup_subset)
	# cleaned_dfS.show(3)
	print("\n Rows numbers in UsCities after drop_duplicates with subset{} {} \n".format(Dup_subset, cleaned_dfS.count()))

	return cleaned_dfS

#__________vv__________vv__________vv__________vv__________vv
def cleaning_Airport_data(dfS, NaN_subset=[], Dup_subset=[]):
	""" Function to check NaN, Duplicate value and handling with them
	"""

	print("\n to show NaN values in dfS of AirPort_data...")
	dfS.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dfS.columns]).show()
	
	# Handle with NaN - consider no drop NaN for Airport, 
	# We have `ident`/ iso_country/ iso_region/ or coordinates are no NaN
	# This just let be here for future improment


	# Handle with Duplicated
	# Drop duplicated with `ident` and `municipality`
	print("\n Rows number of AirPort dfS before drop_duplicates with subset {} is : {} \n".format(Dup_subset, dfS.count()))
	
	print("Drop duplicate with subset 'ident', 'name' ... \n {}")
	cleaned_dfS = dfS.drop_duplicates(subset = Dup_subset)
	# dfS.show(5)
	print(" \n AirPort dfS after drop_duplicates with subset{} {} \n".format(Dup_subset, cleaned_dfS.count()))

	return cleaned_dfS

#__________vv__________vv__________vv__________vv__________vv
def cleaning_CityTemper_data(dfS, NaN_subset=[], Dup_subset=[]):
	""" Function to check and handle NaN and duplicate value in TemperatureCity datasets
	"""
	# cleaning -----------------------------------------------------------------------------------
	print("\n to show NaN values in dfS of CityTemper_data...")
	# dfS.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dfS.columns]).show()

	# TempData include timestamp type, need dedicated check 
	dfS.select(*[
	(
		f.count(f.when((f.isnan(c) | f.col(c).isNull()), c)) if t not in ("timestamp", "date")
		else f.count(f.when(f.col(c).isNull(), c))
	).alias(c)
	for c, t in dfS.dtypes if c in dfS.columns]).show()


	# Handle with NaN
	print("Temperature Df drop all row with blank {}... \n".format(NaN_subset))

	# PySpark dfS.count() will count all row include NaN
	NaNcount = dfS.count() - dfS.dropna(how='any', subset=NaN_subset).count()
	print("How many NaN values in Temperature dfS ... {}".format(NaNcount))

	print("Droping NaN for Temperature with subset {} ... \n".format(NaN_subset))
	cleaned_dfS = dfS.dropna(how='any', subset=NaN_subset)


	# Handle with Duplicated
	print(" \n Row number of  Temperature dfS before drop_duplicates by {} is: {} \n".format(Dup_subset, cleaned_dfS.count()))
	print("Drop duplicate with subset 'dt' ... \n {}")
	cleaned_dfS = cleaned_dfS.drop_duplicates(subset =Dup_subset)
	# dfS.show(5)
	print(" \n Row number of Temperature dfS after drop_duplicates  by {} is: {}  \n".format(Dup_subset, cleaned_dfS.count()))
	# by `dt`: 3167

	return cleaned_dfS

#__________vv__________vv__________vv__________vv__________vv