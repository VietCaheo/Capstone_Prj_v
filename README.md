# DE_ Capstone Project
    Combine all learn from this program.

## Project Overview:
Datasets can be used from Udacity provided in wspace Or prapare by learner himself (using the free data source below)

Udacity Provided project:
    -> work with 4 datasets to complete
        + I94 Immigration Data (to U.S)
        + World Temperature Data
        + U.S. City Demographic Data
        + Airport Code Table

## Steps TODO (Project Instruction):
[Step-1]: Scope the project and gather data
    -> Identify and gather the data you'll be using for your project (at least two sources and more than 1 million rows). See Project Resources for ideas of what data you can use.
    -> Explain what end use cases you'd like to prepare the data for (e.g., analytics table, app back-end, source-of-truth database, etc.)

    
    https://learn.udacity.com/nanodegrees/nd027/parts/e326347a-ba9d-49cd-8eef-dd130767b89d/lessons/65d245bd-0d22-4b64-8147-f71c689afa57/concepts/cfc04b84-633d-43cc-950f-732360646797
    
    Data resource:
        Google Dataset Serach:                                  https://toolbox.google.com/datasetsearch
        Kaggle Datasets:                                        https://www.kaggle.com/datasets
        Github public data:                                     https://github.com/awesomedata/awesome-public-datasets
        Data.gov:                                               https://catalog.data.gov/dataset
        Dataquest:                                              https://www.dataquest.io/blog/free-datasets-for-projects/
        KDnuggets: Datasets for Data Mining and Data Science    https://www.kdnuggets.com/datasets/index.html
        UCI Machine Learning Repository                         https://archive.ics.uci.edu/ml/datasets.php
        Reddit: r/datasets/                                     https://www.reddit.com/r/datasets/
    
    Software Development:
        https://github.com/toddmotto/public-apis
        https://blog.rapidapi.com/most-popular-apis/
        https://developers.facebook.com/docs/graph-api

    -> Example: using Kaggle Datasets 

[Step-2]: Explore and access data
    -> Explore the data to identify data quality issues, like missing values, duplicate data, etc.
    -> Document steps neccessary to clean the data
[Step-3]: Define the Data Model
    -> Map out the conceptual data model and explain why you choose that model.
    -> List the steps neccessary to pipeline the data into the chosen data model.
[Step-4]: Run ETL to Model the Data
    -> Create the data pipelines and the data model
    -> Include a data dictionary (?)
    -> Run data quality check:
        + Integrity constraints on the relational database (e.g unique key, data type, etc)
        + Do Unit tests
        + Source/ Count check to ensure completeness
[Step-5]: Complete Project Write Up
    -> What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
    -> Clearly state the rationale for the choice of tools and technologies for the project
    -> Document the steps of the process
    -> Propose how often the data should be updated and why.
    -> Post your write-up and final data model in a GitHub repo.
    -> Include a description of how you would approach the problem differently under the following scenarios:
        + If the data was increased by x100
        + If the pipeline was run on a daily basis by 7am
        + If the database needed to be accessed by 100+ people

## Udacity Provided Project:
[!Uda note]: We purposely did not include a lot of detail about the data and instead point you to the sources. This is to help you get experience doing a self-guided project and researching the data yourself.


### Datasets - Uda Provied Project:
Udacity Provided project:
will work with 4 datasets to complete
    + I94 Immigration Data (to U.S): 
        -> This data comes from the US National Tourism and Trade Office 
        -> Full dataset : https://travel.trade.gov/research/reports/i94/historical/2016.html
        -> Immigration data files in workspace (Note: total size 6GB) : "../../data/18-83510-I94-Data-2016"
    + World Temperature Data:
        -> come from Kaggle
        -> source https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data
        -> Temperature Data files: (total size: 590MB) ../../data2/GlobalLandTemperaturesByCity.csv
    + U.S. City Demographic Data:
        -> his data comes from OpenSoft. 
        -> source: https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/
    + Airport Code Table
        -> This is a simple table of airport codes and corresponding cities.
        -> source: https://datahub.io/core/airport-codes#data

### Accessing the Data:
Immigration Data:
    [!] The most important decison for modeling data: level of aggregation
        For Ex. Aggregation by airport by month? or by city by year -> this lead to How join the data with other datasets

Temperature Data:

### to be Asked and Referring

-> Read multiple .sas7bat files into the data frame
    https://stackoverflow.com/questions/64246782/converting-each-sas-dataset-to-dataframe-in-pandas  (-> this one for pandas, and can install `pyreadstat`)
    
    https://knowledge.udacity.com/questions/904245  <and below> 
    https://stackoverflow.com/questions/3207219/how-do-i-list-all-files-of-a-directory

-> loading data into dataframe: 
    https://learn.udacity.com/nanodegrees/nd104/parts/cd0024/lessons/ls0520/concepts/1437455d-54a5-4bd6-8096-b7a730a4deee



### Implementing ...:
[Step-1]:

[Step-2]: Explore and access data
Identify data quality issues:
    -> Include NULL (NaN) value in some data files:
        + Need to check which column of data have the NaN (empty) value
            use method pd.DatafFrame.isnull() to check NaN value in df (read from csv files)
        + Count the NaN value in each df
        + Selecting method to dealing with NaN value: 
            Eliminating
            Substituting (by zero, by forward fill, backward fill, ... )



