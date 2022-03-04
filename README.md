# Project Title : The pollution problems that related to immigration in the U.S.
### Data Engineering Capstone Project

#### Project Summary
Our world is facing a self-made poison that is the destruction of ecosystems, including natural resources and the environment. Waste problems, water and air pollution Including deforestation, it is undeniable that this is the poison that all humans spread to the earth. This demonstrates that no matter where humans go, there will be pollution, whether large or small. Air pollution, in particular, is one of the more and more widespread environmental problems every year, mainly due to vehicle and industrial fumes. The smoke directly affects human health.

The question arises, to what extent will changes in local populations directly affect the amount of polluting gas?

This project will consider only the U.S. area, which includes Immigration Data, World Temperature Data, Demographic Data, Airline data, and Pollution Data.
All this information is collected and used to make Data Models in Data Lakes that can show data or relationships according to the questions set.

### Step 1: Scope the Project and Gather Data

#### Scope 
The plan for this project starts with a review of the dataset used to create a data model, which includes: Demographic Data, Immigration Data, U.S. Pollution data, World Temperature Data and Airline data. After getting the relationship between the Dataset, design data models, building the data pipelines in Data Lakes for creating the data models, check Data Quality, write SQL queries to answer questions and write summary docs.

This project addresses, the environmental problem of human-caused pollution by considering the population and immigrant data that occurred in the US in each city, along with pollution data to analyze from the data how much migration occurring in the U.S. causes pollution rates.

**Data Lakes** are used in this project with composed components as follows:
1. Data Source and Target uses **AWS S3** as Object Storage.
2. Use **Python language**, **Apache Spark** and **Data Lakes** technology to build an ETL pipeline on **AWS EMR** Cluster

#### Describe and Gather Data 
The data sets that have been used in the project are listed below:

1. **I94 Immigration Data:** This data comes from the US National Tourism and Trade Office. You can read more about it [here](https://travel.trade.gov/research/reports/i94/historical/2016.html). Data on individual travel to the U.S. for the year 2016. The data is stored in separate 'sas7bdat' files for 1 month each.
2. **World Temperature Data:** This dataset came from Kaggle. You can read more about it [here](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data). Daily temperature data for each city and state in the U.S. is stored in a 'csv' file. 
3. **U.S. City Demographic Data:** This data comes from OpenSoft. You can read more about it [here](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/). This dataset contains information about the demographics of all US cities and census-designated places with a population greater or equal to 65,000. This dataset is stored as a 'csv' file.
4. **U.S. Pollution Data:** This dataset came from Kaggle. This dataset deals with pollution in the U.S. Pollution in the U.S. has been well documented by the U.S. EPA but it is a pain to download all the data and arrange them in a format that interests data scientists. Hence I gathered four major pollutants (Nitrogen Dioxide, Sulphur Dioxide, Carbon Monoxide and Ozone) for every day from 2000 - 2016 and place them neatly in a CSV file. You can read more about it [here](https://www.kaggle.com/sogun3/uspollution).
5. **Airline Database:** This dataset came from OpenFlights. As of January 2012, the OpenFlights Airlines Database contains 5888 airlines. Some of the information is public data and some is contributed by users. You can read more about it [here](https://openflights.org/data.html#airline). 

> Remark: The **Airline Database** are difference with **Airport Code Table** pre-datasets that Udacity provided to use this project beacause the Airport Code Table cannot mapping with the I94 Immigration Data.

### Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
Map out the conceptual data model and explain why you chose that model:

The main table of the data model is the 'Immigration' table. This data provides the individual migrant information of the U.S. and will be aggregated to find summary value by date and city for reference to other tables. The 'Demographic' table are provided information about summary people by city and state in 2015. The 'Pollution' table are provided information about air pollution (NO2, O3, SO2, and O3) by City, State, Country, and date from 2000 to 2016. The 'WorldTemp' table is provided information about World Temperature by City, Country, and date from 1743 to 2013. The 'Airline' table is provided information about the Airline including Name, IATA, ICAO, etc. And, the 'Time' table collects information about the datetime. This model demonstrates a relationship between Immigration data and Pollution data that may be relevant for one of the real factors of environmental problems. So, to come to such a conclusion requires more information and experimentation.

The conceptual data model is designed as following:

![CapstoneProject-DataModels drawio](https://user-images.githubusercontent.com/18514543/156819828-0e3faa4b-4f59-4716-9f80-1820bf4a3f8d.png)

#### 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data (Data Lakes) into the chosen data model:

1. Data Source is exported and placed in AWS S3
2. Read Data files from AWS S3
3. Run ETL process with Spark
4. Write output data from ETL process to AWS S3

Which looks like the diagram below:

![CapstoneProject-Pipeline drawio](https://user-images.githubusercontent.com/18514543/156819731-61ca4725-492e-4d04-99bc-8f5e0e4b69b6.png)


### Step 4: Run Pipelines to Model the Data 
#### 4.1 Create the data model
Build the data pipelines to create the data model.

#### 4.2 How to run the Python Scripts

**Run on AWS EMR**

1. Create AWS EMR Cluster as following link: 
    - [Udacity: AWS CLI - Create EMR Cluster](https://classroom.udacity.com/nanodegrees/nd027/parts/67bd4916-3fc3-4474-b0fd-197dc014e709/modules/3671171a-8bf9-4f75-a913-37439ad281b3/lessons/f08d8ac9-b44a-4717-9da1-9714829da6c9/concepts/9c346cbc-5cf9-4e76-bdf5-a81ce94b55fe)
    - [Udacity: How to execute ETL of Data Lake project on Spark Cluster in AWS?](https://knowledge.udacity.com/questions/46619#552992)

    Or, create AWS EMR Cluster with AWS CLI as follow example below:

    ``` shell
    aws emr create-cluster --name <INPUT_NAME> --use-default-roles --release-label emr-5.34.0 --instance-count 3 --applications Name=Spark Name=JupyterEnterpriseGateway --ec2-attributes KeyName=<INPUT_KEYNAME>,SubnetId=<INPUT_SUBNET> --instance-type m5.xlarge --region us-west-2 --profile <INPUT_PROFILE>
    
    # Example:
    aws emr create-cluster --name emr-cluster-capstone-project-13 --use-default-roles --release-label emr-5.34.0 --instance-count 3 --applications Name=Spark Name=JupyterEnterpriseGateway --ec2-attributes KeyName=emr-cluster-demo_4,SubnetId=subnet-0f50ed7eb6774646a --instance-type m5.xlarge --region us-west-2 --profile cnpawsadmin
    
    # And, you will receive result messages as below:
    
    {
    "ClusterId": "j-2GMHZ63X7KGYA",
    "ClusterArn": "arn:aws:elasticmapreduce:us-west-2:453256782086:cluster/j-2GMHZ63X7KGYA"
    }
    
    ```
    

2. Connect the AWS EMR Cluster with SSH as following link : [Connect to the master node using SSH](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-ssh.html)

3. Run command to configure the AWS CLI on EMR cluster.

    ``` bash
    export AWS_ACCESS_KEY_ID=<input_your_key_id>
    export AWS_SECRET_ACCESS_KEY=<input_your_access_key>
    pip3 install pandas
    ```


4. Open **Capstone Project_ETL.py**, edit the `output_data` values in main() function with **your S3 URI** for collect the process's output and SAVE it.


5. Upload/Create **config.cfg** and **Capstone Project_ETL.py** files to EMR Cluster


6. Open **config.cfg**, edit the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` values in "[AWS]" session with **AWS Access Key ID** and **AWS Secret Access Key**, respectively.


7. Run script **Capstone Project_ETL.py** on the EMR Cluster for start to reads data from S3, processes that data using Spark, and writes them back to S3

    ``` bash
    /home/workspace# which spark-submit
    /usr/bin/spark-submit
    /home/workspace# /usr/bin/spark-submit --packages saurfang:spark-sas7bdat:2.0.0-s_2.11 --master yarn "CapstoneProject_ETL.py"
    ```
    
8. After the script are suessful, you maybe need to terminate the AWS EMR Cluster. Please see AWS CLI for terminate the cluster as below:

    ``` bash
    aws emr terminate-clusters --cluster-ids <INPUT_CLUSTERID> --region us-west-2
    
    ```
    
> _INPUT_CLUSTERID_ : you can find the ClusterId from output message of create AWS EMR Cluster (Step 1)


#### 4.3 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:

#### 4.5 Data dictionary 
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.

**Immigration table**
	
* cicid: double (nullable = true) : unique number for the immigrants
* i94yr: double (nullable = true) : 4 digit year
* i94mon: double (nullable = true) : Numeric month
* i94cit: string (nullable = true) : Citizenship
* i94res: string (nullable = true) : Residence
* i94port: string (nullable = true) : City, State code
* i94port_city: string (nullable = true) : City
* i94port_statecode: string (nullable = true) : State code
* arrdate_todate: string (nullable = true) : Arrival Date in the U.S.
* i94mode: string (nullable = true) : Travel by Land, Air , and Sea
* i94addr: string (nullable = true) : is where the immigrants resides in U.S.
* depdate_todate: string (nullable = true) : Departure Date from the U.S.
* i94bir: double (nullable = true) : Age of Respondent in Years
* i94visa: string (nullable = true) : Visa categories (Business, Pleasure, and Student)
* biryear: double (nullable = true) : 4 digit year of birth
* gender: string (nullable = true) : Non-immigrant sex
* airline: string (nullable = true) : Airline used to arrive in U.S.
* fltno: string (nullable = true) : Flight number of Airline used to arrive in U.S.
 
**WorldTemp table**

* Record_Date: string (nullable = true) : Date of records
* AverageTemperature: string (nullable = true) : Average Temperature
* AverageTemperatureUncertainty: string (nullable = true) : Average Temperature Uncertainty
* City: string (nullable = true) : City
* Country: string (nullable = true) : Country

**Demographic table**

* City: string (nullable = true) : City
* State: string (nullable = true) : State
* Median_Age: string (nullable = true) : Median Age of Population
* Male_Population: string (nullable = true) : Male Population
* Female_Population: string (nullable = true) : Female Population
* Total_Population: string (nullable = true) : Total Population
* NumberofVeterans: string (nullable = true) : Number of Veterans
* Foreign_born: string (nullable = true) : Foreign-born
* Average_Household_Size: string (nullable = true) : Average Household Size
* State_Code: string (nullable = true) : State code
* Race: string (nullable = true) : Race
* Count: string (nullable = true) : Count by Race

**Pollution table**

* No: string (nullable = true) : Number of records
* State_Code: string (nullable = true) : State Code
* County_Code: string (nullable = true) : County Code
* Site_Num: string (nullable = true) : Site Num
* Address: string (nullable = true) : Address
* State: string (nullable = true) : State
* County: string (nullable = true) : County
* City: string (nullable = true) : City
* Date_Local: string (nullable = true) : Date of records
* NO2_Units: string (nullable = true) : Units of NO2
* NO2_Mean: double (nullable = true) : Mean of NO2
* NO2_1st_Max_Value: double (nullable = true) = 1st Max Value of NO2
* NO2_1st_Max_Hour: double (nullable = true) = 1st Max Hour of NO2
* NO2_AQI: double (nullable = true) = AQI of NO2
* O3_Units: string (nullable = true) : Units of O3
* O3_Mean: double (nullable = true) : Mean of O3
* O3_1st_Max_Value: double (nullable = true) = 1st Max Value of O3
* O3_1st_Max_Hour: double (nullable = true) = 1st Max Hour of O3
* O3_AQI: double (nullable = true) = AQI of O3
* SO2_Units: string (nullable = true) : Units of SO2
* SO2_Mean: double (nullable = true) : Mean of SO2
* SO2_1st_Max_Value: double (nullable = true) = 1st Max Value of SO2
* SO2_1st_Max_Hour: double (nullable = true) = 1st Max Hour of SO2
* SO2_AQI: double (nullable = true) = AQI of 
* CO_Units: string (nullable = true) : Units of CO
* CO_Mean: double (nullable = true) :  : Mean of CO
* CO_1st_Max_Value: double (nullable = true) = 1st Max Value of CO
* CO_1st_Max_Hour: double (nullable = true) = 1st Max Hour of CO
* CO_AQI: double (nullable = true) = AQI of CO

**Airline table**

* Airline_ID: string (nullable = true) : Unique OpenFlights identifier for this airline
* Name: string (nullable = true) : Name of the airline.
* Alias: string (nullable = true) : Alias of the airline. For example, All Nippon Airways is commonly known as "ANA"
* IATA: string (nullable = true) : 2-letter IATA code, if available
* ICAO: string (nullable = true) : 3-letter ICAO code, if available
* Callsign: string (nullable = true) : Airline callsign
* Country: string (nullable = true) : Country or territory where airport is located
* Active: string (nullable = true) : "Y" if the airline is or has until recently been operational, "N" if it is defunct. This field is not reliable: in particular, 
             major airlines that stopped flying long ago, but have not had their IATA code reassigned (eg. Ansett/AN), will incorrectly show as "Y".

**Time table**

* date_ref: string (nullable = true) : Date are referenced in Data Model
* day: integer (nullable = true) : Day of month
* week: integer (nullable = true) : Week of year
* month: integer (nullable = true) : Month of year
* year: integer (nullable = true) : 4 digit year
* weekday: integer (nullable = true) : Day of week

#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project.

> In this project, I used Python language, Apache Spark, and Data Lakes technology running on AWS with S3 and EMR.
The main reason is that Data Lakes support Unstructured data which in this project uses 2 types of data files: sas7bdat and CSV.
Next, it is possible to Data Analysis without inserting into a pre-defined schema/table, also known as "Schema-On-Read".
It's can reduce the time to analyze the data considerably. This can be further applied for predictive analytics or machine learning.


* Propose how often the data should be updated and why.

> Our Data Models should use at least Daily or Monthly data to update the data. Since the analysis uses Historical Data, the more data the model is, the more efficient the model is, and predictive analytics or machine learning will produce more accurate results.

* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x.
 
 > For the Infrastructure side, it needs to consider the scale up and scale out of the AWS EMR cluster to support more data/sources.
On the Data side, there must be steps to do Data Quality & Cleansing in order to get the most efficient data.
 
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
 
 > First, add/edit processes to work effectively with Daily Data, such as correct ETL Process, setting Job Schedules, Data Quality, and process of verifying data with Daily Data received and data after processing to ensure that it is correct. and reliable.
 
 * The database needed to be accessed by 100+ people.
 
 > Data Lakes may not be suitable for mass user access, which requires adding a Pipeline to Load processed data to Relational Table. Therefore, consider using an RDBMS such as AWS Redshift that has the ability to support heavy concurrent usage, suitable for both OLTP and OLAB Workload.
It also supports Scaling to be able to support a large number of users.
 
 
#### Reference:

I have been searching for some ideas or other help to find solutions in my code that are shown as below:
    
1. Pandas read csv() Tutorial: Importing Data ... [Link](https://knowledge.udacity.com/questions/368715)
2. How to add header row to a pandas DataFrame ... [Link](https://stackoverflow.com/questions/34091877/how-to-add-header-row-to-a-pandas-dataframe)
3. Pandas – Read only the first n rows of a CSV file ... [Link](https://knowledge.udacity.com/questions/368715)
4. Find Minimum, Maximum, and Average Value of PySpark Dataframe column ... [Link](https://www.geeksforgeeks.org/find-minimum-maximum-and-average-value-of-pyspark-dataframe-column/)
5. MAXIMUM OR MINIMUM VALUE OF COLUMN IN PYSPARK ... [Link](https://www.datasciencemadesimple.com/maximum-or-minimum-value-of-column-in-pyspark/)
6. Best way to get the max value in a Spark dataframe column ... [Link](https://stackoverflow.com/questions/33224740/best-way-to-get-the-max-value-in-a-spark-dataframe-column)
7. PySpark Read CSV file into DataFrame ... [Link](https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe/#header)
8. Spark 2.4 CSV Load Issue with option "nullvalue" ... [Link](https://stackoverflow.com/questions/56752408/spark-2-4-csv-load-issue-with-option-nullvalue)
9. How to Replace a String in Spark DataFrame | Spark Scenario Based Question ... [Link](https://www.learntospark.com/2021/03/replace-a-string-in-spark.html)
10. How to Replace Null Values in Spark DataFrames ... [Link](https://towardsdatascience.com/how-to-replace-null-values-in-spark-dataframes-ab183945b57d)
11. Navigating None and null in PySpark ... [Link](https://mungingdata.com/pyspark/none-null/)
12. Create Spark SQL isdate Function – Date Validation ... [Link](https://dwgeek.com/create-spark-sql-isdate-function-date-validation.html/)
13. How to Write Spark UDFs (User Defined Functions) in Python ... [Link](https://www.bmc.com/blogs/how-to-write-spark-udf-python/)
14. The Ultimate Guide to Data Cleaning ... [Link](https://towardsdatascience.com/the-ultimate-guide-to-data-cleaning-3969843991d4)
15. Python: The Boolean confusion ... [Link](https://towardsdatascience.com/python-the-boolean-confusion-f7fc5288f0ce)
16. Column definitions of immigration data ... [Link](https://knowledge.udacity.com/questions/185453)
17. How i94yr and i94mon columns are related to depdate and arrdate columns in I94 immigration ?. ... [Link](https://knowledge.udacity.com/questions/297018)
18. Matching i94port (I94 Immigration Dataset) to local_code (Airport Codes Dataset) ... [Link](https://knowledge.udacity.com/questions/613179)
19. SAS date format conversion in spark ... [Link](https://knowledge.udacity.com/questions/741863)
20. Trying to convert a SAS time format to Pandas datetime ... [Link](https://knowledge.udacity.com/questions/783322)
21. How to convert SAS numeric date to Date data type in SQL? ... [Link](https://knowledge.udacity.com/questions/381099)
22. what is cicid in immigration file ... [Link](https://knowledge.udacity.com/questions/709930)
23. Column definitions of immigration data ... [Link](https://knowledge.udacity.com/questions/185453)
24. I am confused by what the i94addr represents, could you please explain how it should be used ... [Link](https://knowledge.udacity.com/questions/418299)
25. Not clear what they are asking with \"Integrity constraints on the relational database (e.g., unique key, data type, etc.)\" ... [Link](https://knowledge.udacity.com/questions/767640)
26. Failed to find data source: com.github.saurfang.sas.spark error ... [Link](https://knowledge.udacity.com/questions/224269)
27. Can not read metadata from sas7bdat file ... [Link](https://knowledge.udacity.com/questions/386959)
28. ModuleNotFoundError: No module named 'pandas' ... [Link](http://net-informations.com/ds/err/pderr.htm)
29. Reading in SAS files from S3 ... [Link](https://knowledge.udacity.com/questions/197139)
30. How to keep SSH connections alive ... [Link](https://www.a2hosting.com/kb/getting-started-guide/accessing-your-account/keeping-ssh-connections-alive)
31. I am trying load data into S3 for Capstone project but I get error - Exception: Java gateway process exited before sending its port number ... [Link](https://knowledge.udacity.com/questions/703078)
32. Read a sas7bdat as Spark Dataframe ... [Link](https://knowledge.udacity.com/questions/72864)
33. Reading and appending files into a spark dataframe ... [Link](https://stackoverflow.com/questions/57824016/reading-and-appending-files-into-a-spark-dataframe)
34. Python Pandas Read multiple SAS files from a list into separate dataframes ... [Link](https://stackoverflow.com/questions/52894787/python-pandas-read-multiple-sas-files-from-a-list-into-separate-dataframes)
35. Read all the SAS files in the folder to a single dataframe in a single load. ... [Link](https://knowledge.udacity.com/questions/280093)
36. Read Immigration Dataset in SAS format AND Write as parquet format on S3. ... [Link](https://github.com/MaulikDave9/DEND-Immigration-2016/blob/f3f00480a0d438878280a03007102341b75f5cf7/Miscellaneous/Test_SparkRead_I94SAS.ipynb)
37. How to read multiple .sas7bdat files into data frame ?.. ... [Link](https://knowledge.udacity.com/questions/292327)
38. Reading in SAS files from S3 ... [Link](https://knowledge.udacity.com/questions/197139)
39. Spark SQL Join Types with examples ... [Link](https://sparkbyexamples.com/spark/spark-sql-dataframe-join/)
40. Spark vs NoSQL:Is (Hadoop or S3(for storage)+Spark(for querying)) combination more powerful than NoSQL like Cassandra as we can join tables in spark ? ... [Link](https://knowledge.udacity.com/questions/708563)
41. How to calculate cumulative sum using sqlContext ... [Link](https://stackoverflow.com/questions/34726268/how-to-calculate-cumulative-sum-using-sqlcontext)
42. How to get cumulative sum ... [Link](https://stackoverflow.com/questions/2120544/how-to-get-cumulative-sum)
43. EMR notebook fails because JupyterEnterpriseGateway application not installed on existing cluster AWS [closed] ... [Link](https://stackoverflow.com/questions/66729542/emr-notebook-fails-because-jupyterenterprisegateway-application-not-installed-on)
44. create-cluster ... [Link](https://docs.aws.amazon.com/cli/latest/reference/emr/create-cluster.html)
45. PySpark and SparkSQL Basics ... [Link](https://towardsdatascience.com/pyspark-and-sparksql-basics-6cb4bf967e53)
46. PySpark Window Functions ... [Link](https://sparkbyexamples.com/pyspark/pyspark-window-functions/#aggregate-functions)
47. SQL PARTITION BY Clause overview ... [Link](https://www.sqlshack.com/sql-partition-by-clause-overview/)
48. Unable to use PARTITION BY with COUNT(*) when multiple columns are used ... [Link](https://stackoverflow.com/questions/35026644/unable-to-use-partition-by-with-count-when-multiple-columns-are-used)
49. Demographics dataset 'count' column ... [Link](https://knowledge.udacity.com/questions/319686)
50. Splitting a string in SparkSQL ... [Link](https://stackoverflow.com/questions/44688168/splitting-a-string-in-sparksql)
51. Spark split() function to convert string to Array column ... [Link](https://sparkbyexamples.com/spark/convert-delimiter-separated-string-to-array-column-in-spark/#:~:text=Spark%20SQL%20provides%20split(),e.t.c%2C%20and%20converting%20into%20ArrayType.)
52. Spark SQL String Functions Explained ... [Link](https://sparkbyexamples.com/spark/usage-of-spark-sql-string-functions/)
53. Spark Docs » Functions ... [Link](https://spark.apache.org/docs/2.3.0/api/sql/index.html)
54. PySpark Convert String Type to Double Type ... [Link](https://sparkbyexamples.com/pyspark/pyspark-convert-string-type-to-double-type-float-type/)
55. PySpark Drop Rows with NULL or None Values ... [Link](https://sparkbyexamples.com/pyspark/pyspark-drop-rows-with-null-values/)
56. PySpark Groupby Explained with Example ... [Link](https://sparkbyexamples.com/pyspark/pyspark-groupby-explained-with-example/)
57. SQL - AVG Function ... [Link](https://www.tutorialspoint.com/sql/sql-avg-function.htm#:~:text=SQL%20AVG%20function%20is%20used,a%20field%20in%20various%20records.&text=You%20can%20take%20average%20of,typed%20pages%20by%20every%20person.)
58. Spark Cast String Type to Integer Type (int) ... [Link](https://sparkbyexamples.com/spark/spark-cast-string-type-to-integer-type-int/#:~:text=In%20Spark%20SQL%2C%20in%20order,selectExpr()%20and%20SQL%20expression.)
59. PySpark to_date() – Convert String to Date Format ... [Link](https://sparkbyexamples.com/pyspark/pyspark-to_date-convert-string-to-date-format/)
60. Python Spark Cumulative Sum by Group Using DataFrame ... [Link](https://stackoverflow.com/questions/45946349/python-spark-cumulative-sum-by-group-using-dataframe)
61. Check Type: How to check if something is a RDD or a DataFrame? ... [Link](https://stackoverflow.com/questions/36731365/check-type-how-to-check-if-something-is-a-rdd-or-a-dataframe)
62. AttributeError: ‘DataFrame’ object has no attribute ‘map’ in PySpark ... [Link](https://sparkbyexamples.com/pyspark/attributeerror-dataframe-object-has-no-attribute-map-in-pyspark/)
63. ValueError: Cannot convert column into bool ... [Link](https://stackoverflow.com/questions/48282321/valueerror-cannot-convert-column-into-bool)
64. understanding level =0 and group_keys ... [Link](https://stackoverflow.com/questions/49859182/understanding-level-0-and-group-keys)
65. How to calculate top 5 max values in Pyspark ... [Link](https://www.learneasysteps.com/how-to-calculate-top-5-max-values-in-pyspark/)
66. Comprehensive Guide to Grouping and Aggregating with Pandas ... [Link](https://pbpython.com/groupby-agg.html)
67. Pandas .groupby(), Lambda Functions, & Pivot Tables ... [Link](https://mode.com/python-tutorial/pandas-groupby-and-python-lambda-functions/)
68. pandas.DataFrame.groupby ... [Link](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.groupby.html)
69. Pandas groupby() Syntax ... [Link](https://sparkbyexamples.com/pandas/pandas-groupby-explained-with-examples/)
70. Indexing and selecting data ... [Link](https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html)
71. Time series / date functionality ... [Link](https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases)
72. Pyspark Column Transformation: Calculate Percentage Change for Each Group in a Column ... [Link](https://stackoverflow.com/questions/57470314/pyspark-column-transformation-calculate-percentage-change-for-each-group-in-a-c)
73. Rate of Change (ROC) ... [Link](https://www.investopedia.com/terms/r/rateofchange.asp#:~:text=Rate%20of%20change%20is%20used,value%20from%20an%20earlier%20period.)
74. 8 Examples of Data Lake Architectures on Amazon S3 ... [Link](https://www.upsolver.com/blog/examples-of-data-lake-architecture-on-amazon-s3)
75. Immigration Dataset Dictionary ... [Link](https://knowledge.udacity.com/questions/529723)
76. Data dictionary given in workspace, do we need to create reference table for some field ... [Link](https://knowledge.udacity.com/questions/230175)
77. What is Data Dictionary ... [Link](https://www.tutorialspoint.com/What-is-Data-Dictionary)
78. data dictionary ... [Link](https://knowledge.udacity.com/questions/196779)
79. error output for long running process for Data Lake project ... [Link](https://knowledge.udacity.com/questions/131502)
80. udacity/workspaces-student-support ... [Link](https://github.com/udacity/workspaces-student-support/tree/master/jupyter)
81. Display the Pandas DataFrame in table style ... [Link](https://www.geeksforgeeks.org/display-the-pandas-dataframe-in-table-style/)
82. Climate Change: Earth Surface Temperature Data ... [Link](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data?select=GlobalLandTemperaturesByCity.csv)
83. Country code and names ... [Link](https://www.kaggle.com/leogenzano/country-code-and-names)
84. U.S. Pollution Data ... [Link](https://www.kaggle.com/sogun3/uspollution)
85. Animation, Basemap, Plotly for Air Quality Index ... [Link](https://www.kaggle.com/jaeyoonpark/animation-basemap-plotly-for-air-quality-index)
86. Air Data Basic Information ... [Link](https://www.epa.gov/outdoor-air-quality-data/air-data-basic-information)
87. Air Quality Index (AQI) ... [Link](https://www.epa.gov/sites/default/files/2014-05/documents/zell-aqi.pdf)
88. U.S. Electric Power Generators ... [Link](https://www.kaggle.com/saurabhshahane/us-electric-power-generators)
89. Form EIA-860 detailed data with previous form data (EIA-860A/860B) ... [Link](https://www.eia.gov/electricity/data/eia860/)
90. Airline Database ... [Link](https://www.kaggle.com/open-flights/airline-database)
91. Airport, airline and route data ... [Link](https://openflights.org/data.html#airline)
92. Aviation-Data-Download ... [Link](https://www.aviationfile.com/aviation-data-download/)
93. ICAO airport code ... [Link](https://en.wikipedia.org/wiki/ICAO_airport_code)
94. Airline codes ... [Link](https://en.wikipedia.org/wiki/Airline_codes#:~:text=The%20ICAO%20airline%20designator%20is,designator%20and%20a%20telephony%20designator.)
95. List of airline codes ... [Link](https://en.wikipedia.org/wiki/List_of_airline_codes)
96. ISO 3166-1 alpha-2 ... [Link](https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2)
97. List of ISO 3166 country codes ... [Link](https://en.wikipedia.org/wiki/List_of_ISO_3166_country_codes#UNI4)
98. Spark SAS Data Source (sas7bdat) ... [Link](https://github.com/saurfang/spark-sas7bdat)
99. US Cities: Demographics ... [Link](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/information/?dataChart=eyJxdWVyaWVzIjpbeyJjb25maWciOnsiZGF0YXNldCI6InVzLWNpdGllcy1kZW1vZ3JhcGhpY3MiLCJvcHRpb25zIjp7fX0sImNoYXJ0cyI6W3siYWxpZ25Nb250aCI6dHJ1ZSwidHlwZSI6ImNvbHVtbiIsImZ1bmMiOiJBVkciLCJ5QXhpcyI6Im1lZGlhbl9hZ2UiLCJzY2llbnRpZmljRGlzcGxheSI6dHJ1ZSwiY29sb3IiOiIjRkY1MTVBIn1dLCJ4QXhpcyI6ImNpdHkiLCJtYXhwb2ludHMiOjUwLCJzb3J0IjoiIn1dLCJ0aW1lc2NhbGUiOiIiLCJkaXNwbGF5TGVnZW5kIjp0cnVlLCJhbGlnbk1vbnRoIjp0cnVlfQ%3D%3D)
100. Scale up vs Scale out: What’s the difference? ... [Link](https://opsani.com/blog/scale-up-vs-scale-out-whats-the-difference/#:~:text=Scaling%20out%20is%20adding%20more,to%20handle%20a%20greater%20load.)
