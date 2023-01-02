# Lesson from Capstone Project

## First Approach to Gather and Explore raw Datasets

### The most important when choosing data

* The most important is immagine how the final tables will be used after finished all target tables
* Then collect the related and appropriate data
### Take a first look on raw data

* Need to read the raw dataset to dataframe firstly
* Filter by NaN value in each dataset and print out as count how many NaN value on each column
* Filter by Duplicated value on each columns
* In this case: the nicest is using PySpark to reading dataset and printSchema to show out. Another way is use pandas rd (but pandas could not provide good view about schema, and issue about way to dealing with schema later)

## Define Data Model and design the Schema

* Almost dataset in real world now large (> 1mil lines), so choosing which data model ???
* From Data Model: it's most important step to design the diagram of target tables.
    * For example: From first approach step, we need immagine the purpose will applied from result-table, and build the appropriate the diagram for joining information between tables.

## Build main ETL:

* Let's say using PySpark for read raw dataset and build the dataframe by PySpark api froom step1 above.
* to build ETL: read rawdata set will be got the Schema of data. This will work as staging table which includes all information field from raw dataset.
* `Staging` because: in real-world raw dataset may be very large, and storage in S3 for Ex.
* Transform and Loading: 
    * Need care about Cleaning Data activities
    * Choose which data field will become `key`  for joining data
    * Data key may need extract from hiden information in raw dataset.
    * At Loading step: Can choose how target data table will be storage (local, DB?; Redshift; S3 )
## Data Quality Check:

* Spark SQL: support very strong with familiar SQL commands, it should leverage it to do query test data.
* Need to care about Consistency of data before and after write parquet files (if choose parquet to write)
* Immagine real use-case when query data from target tables.

## Immagine some use-case for query from final tables

* It should always care from first step to collecting dataset and explore data
* Use-case and requirement query come by randomly by any data field we have already in.

## Bonus link and information from Mentor
* regarding the data model in real world:
    * https://www.datastax.com/blog/2017/08/data-model-meets-world-blog-series
    * https://eng.uber.com/uber-big-data-platform/

* heavy-read system design:
    * https://medium.com/@narengowda/netflix-system-design-dbec30fede8d
* Experience when implementing data pipeline:
    * https://medium.com/velotio-perspectives/lessons-learnt-while-building-an-etl-pipeline-for-mongodb-amazon-redshift-using-apache-airflow-543bb0b75017    
