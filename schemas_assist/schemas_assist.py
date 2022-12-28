"""File use for some auxilliary job before loading data to target tables such as:
	+ data cleaning
	+ creat tables using Pyspark.sql query
"""

# to check NaN for SparkDF
from pyspark.sql.functions import isnan, when, count, col

# Basic clean prior to loading to Fact table
def cleaning_Immigra_data(dfS):
	""" Do Data Cleaning for Immigration datasets.
		Identify columns consist the NaN value, duplicated, and handle them
	"""

	print("\n show NaN values in dfS of Immigra_data...")
	dfS.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dfS.columns]).show()

	# ---------------------------------------------------
	# Handle with NaN
	print("\n Droping NaN values here prior to  load to tables ... \n")

	# base on intention Fact and Dimension Table being created, select 'i94cit', as subset when drop_nan
	NaNcount = dfS.count() - dfS.dropna(how='any', subset=['i94cit']).count()

	print("\n To show how many rows is being dropped when filtered by i94cit and ... {}".format(NaNcount))

	print("Droping NaN  ....... \n")
	dfS.dropna(how='any', subset=['i94cit'])
	# ---------------------------------------------------

	# Handle with Duplicated
	# Check duplicated and drop as subset specified
	print("to check any duplicated in dfS_ImmigAll ... \n")

	print("\n Number of Immigra before drop_duplicates {} \n".format(dfS.count()))
	print("\n Drop duplicated by cicid for make sure cicid is unique for each Immigrant Info...")
	dfS.drop_duplicates(["cicid"])
	# dfS.show(3)
	print("\n Number of Immigra after drop_duplicates {} \n".format(dfS.count()))


# Drop more NaN for interested field for dim table
# subset list might use `i94addr`
def cleaning_Dim_Immigra(dfS, **sublist=[]):
	""" Immagine Dim table using for detail security investigation in future.
	So, some field will be droped if it NaN
	"""

	print("\n Droping NaN values for Dim Immigration detail info ... \n")

	# base on intention Fact and Dimension Table
	NaNcount = dfS.count() - dfS.dropna(how='any', subset=sublist).count()

	print("\n To show how many rows were dropped ... {}".format(NaNcount))

	print("Droping NaN  ....... \n")
	dfS.dropna(how='any', subset=sublist)


