import pandas as pd
import datetime as dt
import configparser
import os
from datetime import datetime
from pyspark.sql.functions import udf
from pyspark.sql.functions import min, max
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql import SparkSession


config = configparser.ConfigParser()
config.read('config.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create SparkSession and Config to use in ETL as below:
    
    - Config spark.jars.repositories to https://repos.spark-packages.org/
    - Config spark.jars.packages to org.apache.hadoop:hadoop-aws:2.7.3, saurfang:spark-sas7bdat:2.0.0-s_2.11
    - Config spark.hadoop.fs.s3a.impl to org.apache.hadoop.fs.s3a.S3AFileSystem
    - Config spark.hadoop.fs.s3a.awsAccessKeyId to os.environ['AWS_ACCESS_KEY_ID']
    - Config spark.hadoop.fs.s3a.awsSecretAccessKey to os.environ['AWS_SECRET_ACCESS_KEY']
    - Config spark.hadoop.fs.s3a.multiobjectdelete.enable to false
    
    """ 
        
    spark = SparkSession \
        .builder \
        .appName("Udacity_DataEngineering-CapstoneProject_Phakphoom") \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.3, saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.awsAccessKeyId", os.environ['AWS_ACCESS_KEY_ID']) \
        .config("spark.hadoop.fs.s3a.awsSecretAccessKey", os.environ['AWS_SECRET_ACCESS_KEY']) \
        .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false") \
        .enableHiveSupport() \
        .getOrCreate()
    
    return spark

def process_immigration_data(spark, input_data, output_data):
    """
    Function for Load data from S3, ETL and write to parquet files in S3 (immigration table)
    
    - Immigration data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/I94_Immigration_Data-2016/) => df_immigration
    - Mapping i94addr data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/I94_Immigration_Data_Mapping/I94ADDR_Mapping.csv) => df_i94addr
    - Mapping i94cit & i94res data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/I94_Immigration_Data_Mapping/I94CIT_I94RES_Mapping.csv) => df_i94cit_i94res
    - Mapping i94mode data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/I94_Immigration_Data_Mapping/I94MODE_Mapping.csv) => df_i94mode
    - Mapping i94port data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/I94_Immigration_Data_Mapping/I94PORT_Mapping.csv) => df_i94port
    - Mapping i94visa data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/I94_Immigration_Data_Mapping/I94VISA_Mapping.csv) => df_i94visa
    
    INPUT:
    - spark : pyspark.sql.SparkSession. That use Spark session
    - input_data : string. S3 Location of source data
    - output_data : string. S3 Location of target data
    """ 

    # Read all mapping data that use in immigration data    
    mapping_data = input_data + "I94_Immigration_Data_Mapping/"
    df_i94addr = spark.read.option("header",True).options(delimiter=',') \
        .csv(mapping_data + "I94ADDR_Mapping.csv")
    df_i94cit_i94res = spark.read.option("header",True).options(delimiter=',') \
        .csv(mapping_data + "I94CIT_I94RES_Mapping.csv")
    df_i94mode = spark.read.option("header",True).options(delimiter=',') \
        .csv(mapping_data + "I94MODE_Mapping.csv")
    df_i94port = spark.read.option("header",True).options(delimiter=',') \
        .csv(mapping_data + "I94PORT_Mapping.csv")
    df_i94visa = spark.read.option("header",True).options(delimiter=',') \
        .csv(mapping_data + "I94VISA_Mapping.csv")

    # Bring all mapping data to create view for use in Spark SQL
    df_i94addr.createOrReplaceTempView("I94ADDR_Mapping")
    df_i94cit_i94res.createOrReplaceTempView("I94CIT_I94RES_Mapping")
    df_i94mode.createOrReplaceTempView("I94MODE_Mapping")
    df_i94port.createOrReplaceTempView("I94PORT_Mapping")
    df_i94visa.createOrReplaceTempView("I94VISA_Mapping")

    # Get filepath to Immigration data file
    # Use this for provide manual path
    #immigration_data = input_data + "I94_Immigration_Data-2016/*.sas7bdat"
    #immigration_data = input_data + "I94_Immigration_Data-2016/i94_apr16_sub.sas7bdat"
    immigration_data = input_data + "I94_Immigration_Data-2016/"
    
    # variable for count data
    count_immigration_final_table = 0

    # variable for create target directory 
    immigration_location= os.path.join(output_data, "Immigration")

    # create udf for transform sas dateformat
    get_date = udf(lambda x: (dt.datetime(1960, 1, 1).date() + dt.timedelta(float(x))).isoformat() if x else None)

    # Read multiple .sas7bdat files into data frame
    months = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
    for m in months:
        sasfilename = "{}{}{}{}".format(immigration_data,"i94_",m,"16_sub.sas7bdat")
        #df_spark_i94 = spark.read.format('com.github.saurfang.sas.spark').load(inputf)
        #df_spark_i94.write.mode('append').partitionBy("i94mon").parquet(i94ParquetPath)
    
        # Read immigration data (sas7bdat) file
        df_spark = spark.read.format('com.github.saurfang.sas.spark').load(sasfilename)

        # Convert date fields
        df_immigration = df_spark.withColumn("arrdate_todate", get_date(df_spark.arrdate)).\
                                withColumn("depdate_todate", get_date(df_spark.depdate))
    
        # Create the data model of Immigration
        df_immigration.createOrReplaceTempView("Immigration_data")
        df_immigration_final = spark.sql("""
                        SELECT DISTINCT cicid, i94yr, i94mon, I94CIT_I94RES1.i94cntyl as i94cit, I94CIT_I94RES2.i94cntyl as i94res, 
                            trim(i94prtl) as i94port, trim(SPLIT(i94prtl,',')[0]) as i94port_city, trim(SPLIT(i94prtl,',')[1]) as i94port_statecode,
                            arrdate_todate, i94model as i94mode, i94addrl as i94addr, depdate_todate, i94bir, 
                            I94VISA.I94VISA as i94visa, biryear, gender, airline, fltno   
                        FROM Immigration_data AS IMM 
                        LEFT JOIN I94ADDR_Mapping AS I94ADDR on IMM.i94addr = I94ADDR.value 
                        LEFT JOIN I94CIT_I94RES_Mapping AS I94CIT_I94RES1 on IMM.i94cit = I94CIT_I94RES1.value 
                        LEFT JOIN I94CIT_I94RES_Mapping AS I94CIT_I94RES2 on IMM.i94res = I94CIT_I94RES2.value 
                        LEFT JOIN I94MODE_Mapping AS I94MODE on IMM.i94mode = I94MODE.value 
                        LEFT JOIN I94PORT_Mapping AS I94PORT on IMM.i94port = I94PORT.value 
                        LEFT JOIN I94VISA_Mapping AS I94VISA on IMM.i94visa = I94VISA.value 
                    """)
        # Add more fields for partition files
        df_immigration_final = df_immigration_final \
            .withColumn('month',month(df_immigration_final.arrdate_todate)) \
            .withColumn('year',year(df_immigration_final.arrdate_todate)) 
    
        # Count records of Immigration table for data quality checks
        count_immigration_final_table += df_immigration_final.count()

        # write Immigration table to parquet files partitioned by year and month
        if m == 'jan':
            df_immigration_final.write.partitionBy("year", "month").parquet(immigration_location, mode="overwrite")
        else:
            df_immigration_final.write.partitionBy("year", "month").parquet(immigration_location, mode="append")

    # Print count records of Immigration table
    print('The data in "{}" table have {} record(s)'.format('Immigration', count_immigration_final_table))


def process_worldtemp_data(spark, input_data, output_data):
    """
    Function for Load data from S3, ETL and write to parquet files in S3 (worldtemp table)
    
    - World Temperature data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/WorldTemperature_Data/) => df_worldtemp
    
    INPUT:
    - spark : pyspark.sql.SparkSession. That use Spark session
    - input_data : string. S3 Location of source data
    - output_data : string. S3 Location of target data
    """

    # Get filepath to World Temperature data file
    # Use this for provide manual path
    worldtemp_data = input_data + "WorldTemperature_Data/GlobalLandTemperaturesByCity.csv"
    
    # Read World Temperatur data (csv) file
    df_worldtemp = spark.read.option("header",True) \
        .csv(worldtemp_data)
    
    # Create the data model of World Temperature Data
    df_worldtemp.createOrReplaceTempView("WorldTemp")
    df_worldtemp_final = spark.sql("""
                    SELECT DISTINCT dt as Record_Date, AverageTemperature, AverageTemperatureUncertainty, UPPER(City) as City, UPPER(Country) as Country  
                    FROM WorldTemp 
                    WHERE AverageTemperature IS NOT NULL OR AverageTemperatureUncertainty IS NOT NULL 
                """)
    
    # Add more fields for partition files
    df_worldtemp_final = df_worldtemp_final \
        .withColumn('month',month(df_worldtemp_final.Record_Date)) \
        .withColumn('year',year(df_worldtemp_final.Record_Date)) 
    
    # Count records of World Temperature table for data quality checks
    count_worldtemp_final_table = df_worldtemp_final.count()
    print('The data in "{}" table have {} record(s)'.format('WorldTemp', count_worldtemp_final_table))
    
    # write WorldTemp table to parquet files partitioned by year and month
    worldtemp_location= os.path.join(output_data, "WorldTemp")
    df_worldtemp_final.write.partitionBy("year", "month").parquet(worldtemp_location, mode="overwrite")
    
def process_demographic_data(spark, input_data, output_data):
    """
    Function for Load data from S3, ETL and write to parquet files in S3 (demographic table)
    
    - Demographic data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/U.S.City_Demographic_Data/) => df_demog_us
    
    INPUT:
    - spark : pyspark.sql.SparkSession. That use Spark session
    - input_data : string. S3 Location of source data
    - output_data : string. S3 Location of target data
    """

    # Get filepath to U.S. City Demographic data file
    # Use this for provide manual path
    demographic_data = input_data + "U.S.City_Demographic_Data/us-cities-demographics.csv"
    
    # Read Demographic data (csv) file
    df_demog_us = spark.read.option("header",True).options(delimiter=';') \
        .csv(demographic_data)
    
    # Create the data model of U.S. City Demographic Data
    df_demog_us.createOrReplaceTempView("Demographic")
    df_demog_us_final = spark.sql("""
                    SELECT DISTINCT UPPER(City) as City, 
                        UPPER(State) as State, 
                        `Median Age` AS Median_Age, 
                        `Male Population` AS Male_Population, 
                        `Female Population` AS Female_Population, 
                        `Total Population` AS Total_Population, 
                        `Number of Veterans` AS NumberofVeterans, 
                        `Foreign-born` AS Foreign_born, 
                        `Average Household Size` AS Average_Household_Size, 
                        `State Code` AS State_Code, 
                        Race, 
                        Count
                    FROM Demographic  

                """)
    
    # Count records of U.S. City Demographic table for data quality checks
    count_demog_us_final_table = df_demog_us_final.count()
    print('The data in "{}" table have {} record(s)'.format('Demographic', count_demog_us_final_table))
    
    # write Demographic table to parquet files
    demog_us_location= os.path.join(output_data, "Demographic")
    df_demog_us_final.write.parquet(demog_us_location, mode="overwrite")

def process_pollution_data(spark, input_data, output_data):
    """
    Function for Load data from S3, ETL and write to parquet files in S3 (pollution table)
    
    - Pollution data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/U.S.Pollution_Data/) => df_pollution
    
    INPUT:
    - spark : pyspark.sql.SparkSession. That use Spark session
    - input_data : string. S3 Location of source data
    - output_data : string. S3 Location of target data
    """

    # Get filepath to U.S. Pollution data file
    # Use this for provide manual path
    pollution_data = input_data + "U.S.Pollution_Data/pollution_us_2000_2016.csv"
    
    # Read Pollution data (csv) file
    df_pollution = spark.read.option("header",True).options(delimiter=',') \
        .csv(pollution_data)
    
    # Create the data model of U.S. Pollution Data
    df_pollution.createOrReplaceTempView("Pollution")
    df_pollution_final = spark.sql("""
                    SELECT DISTINCT _c0 AS No,
                        `State Code` AS State_Code,
                        `County Code` AS County_Code,
                        `Site Num` AS Site_Num,
                        `Address` AS Address,
                        UPPER(State) AS State,
                        UPPER(County) AS County,
                        UPPER(City) AS City,
                        `Date Local` AS Date_Local,
                        `NO2 Units` AS NO2_Units,
                        DOUBLE(`NO2 Mean`) AS NO2_Mean,
                        DOUBLE(`NO2 1st Max Value`) AS NO2_1st_Max_Value,
                        DOUBLE(`NO2 1st Max Hour`) AS NO2_1st_Max_Hour,
                        DOUBLE(`NO2 AQI`) AS NO2_AQI,
                        `O3 Units` AS O3_Units,
                        DOUBLE(`O3 Mean`) AS O3_Mean,
                        DOUBLE(`O3 1st Max Value`) AS O3_1st_Max_Value,
                        DOUBLE(`O3 1st Max Hour`) AS O3_1st_Max_Hour,
                        DOUBLE(`O3 AQI`) AS O3_AQI,
                        `SO2 Units` AS SO2_Units,
                        DOUBLE(`SO2 Mean`) AS SO2_Mean,
                        DOUBLE(`SO2 1st Max Value`) AS SO2_1st_Max_Value,
                        DOUBLE(`SO2 1st Max Hour`) AS SO2_1st_Max_Hour,
                        DOUBLE(`SO2 AQI`) AS SO2_AQI,
                        `CO Units` AS CO_Units,
                        DOUBLE(`CO Mean`) AS CO_Mean,
                        DOUBLE(`CO 1st Max Value`) AS CO_1st_Max_Value,
                        DOUBLE(`CO 1st Max Hour`) AS CO_1st_Max_Hour,
                        DOUBLE(`CO AQI`) AS CO_AQI
                    FROM Pollution  

                """)
    
    # Delete Records that have NULL value
    df_pollution_final = df_pollution_final.na.drop()

    # Add more fields for partition files
    df_pollution_final = df_pollution_final \
        .withColumn('month',month(df_pollution_final.Date_Local)) \
        .withColumn('year',year(df_pollution_final.Date_Local))
    
    # Count records of U.S. Pollution table for data quality checks
    count_pollution_final_table = df_pollution_final.count()
    print('The data in "{}" table have {} record(s)'.format('Pollution', count_pollution_final_table))
    
    # write Pollution table to parquet files partitioned by year and month
    pollution_location= os.path.join(output_data, "Pollution")
    df_pollution_final.write.partitionBy("year", "month").parquet(pollution_location, mode="overwrite")

def process_airline_data(spark, input_data, output_data):
    """
    Function for Load data from S3, ETL and write to parquet files in S3 (airline table)
    
    - Airlines data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/OpenFlights/) => df_airlines
    
    INPUT:
    - spark : pyspark.sql.SparkSession. That use Spark session
    - input_data : string. S3 Location of source data
    - output_data : string. S3 Location of target data
    """

    # Get filepath to OpenFlights (airlines) data file
    # Use this for provide manual path
    airline_data = input_data + "OpenFlights/airlines.dat"
    
    # Read Airlines data (csv) file
    df_airlines = spark.read.option("header",True).options(delimiter=',') \
        .csv(airline_data)

    # Replace specific value to NULL value
    df_airlines = df_airlines.na.replace("\\N",None)
    
    # Create the data model of OpenFlights (airlines) Data
    df_airlines.createOrReplaceTempView("Airline")
    df_airlines_final = spark.sql("""
                    SELECT DISTINCT AirlineID AS Airline_ID, 
                        Name, 
                        Alias, 
                        IATA,
                        ICAO,
                        Callsign,
                        UPPER(Country) AS Country,
                        Active  
                    FROM Airline
                    WHERE IATA IS NOT NULL OR IATA <> '-'

                """)
    
    # Count records of Airline table for data quality checks
    count_airlines_final_table = df_airlines_final.count()
    print('The data in "{}" table have {} record(s)'.format('Airline', count_airlines_final_table))
    
    # write Airline table to parquet files
    airlines_location= os.path.join(output_data, "Airline")
    df_airlines_final.write.parquet(airlines_location, mode="overwrite")

def process_time_data(spark, input_data, output_data):
    """
    Function for Load data from S3, ETL and write to parquet files in S3 (time table)
    
    - Immigration data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/I94_Immigration_Data-2016/) => df_immigration
    - World Temperature data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/WorldTemperature_Data/) => df_worldtemp
    - Pollution data (s3://capstoneproject-phakphoom/Capstone_Project/DataSource/U.S.Pollution_Data/) => df_pollution
    
    INPUT:
    - spark : pyspark.sql.SparkSession. That use Spark session
    - input_data : string. S3 Location of source data
    - output_data : string. S3 Location of target data
    """

    # Prepare Directory for Temp Storage
    time_location_temp = os.path.join(output_data, "Time_temp")

    # Read in Immigration data to use for Time table
    immigration_output = output_data + "Immigration/"
    df_immigration = spark.read.parquet(immigration_output)
    df_immigration.createOrReplaceTempView("Immigration_data")

    # Read Immigration data for TIME Data 
    df_time_immigration = spark.sql("""
                    SELECT DISTINCT arrdate_todate AS date_ref FROM Immigration_data WHERE arrdate_todate IS NOT NULL 
                    UNION 
                    SELECT DISTINCT depdate_todate AS date_ref FROM Immigration_data WHERE depdate_todate IS NOT NULL 
                """)

    # Extract columns to create Time table
    df_time_immigration = df_time_immigration.withColumn('day',dayofmonth(df_time_immigration.date_ref)) \
        .withColumn('week',weekofyear(df_time_immigration.date_ref)) \
        .withColumn('month',month(df_time_immigration.date_ref)) \
        .withColumn('year',year(df_time_immigration.date_ref)) \
        .withColumn('weekday',dayofweek(df_time_immigration.date_ref))

    # Write only TIME data to Temp Storage
    df_time_immigration.write.parquet(time_location_temp, mode="overwrite")
    
    # Read in WorldTemp data to use for Time table
    worldtemp_output = output_data + "WorldTemp/"
    df_worldtemp = spark.read.parquet(worldtemp_output)
    df_worldtemp.createOrReplaceTempView("WorldTemp")

    # Read WorldTemp data for TIME Data 
    df_time_worldtemp = spark.sql("""
                    SELECT DISTINCT Record_Date AS date_ref FROM WorldTemp WHERE Record_Date IS NOT NULL 
                """)

    # Extract columns to create Time table
    df_time_worldtemp = df_time_worldtemp.withColumn('day',dayofmonth(df_time_worldtemp.date_ref)) \
        .withColumn('week',weekofyear(df_time_worldtemp.date_ref)) \
        .withColumn('month',month(df_time_worldtemp.date_ref)) \
        .withColumn('year',year(df_time_worldtemp.date_ref)) \
        .withColumn('weekday',dayofweek(df_time_worldtemp.date_ref))

    # Write only TIME data to Temp Storage
    df_time_worldtemp.write.parquet(time_location_temp, mode="append")
    
    # Read in Pollution data to use for Time table
    pollution_output = output_data + "Pollution/"
    df_pollution = spark.read.parquet(pollution_output)
    df_pollution.createOrReplaceTempView("Pollution")

    # Read Pollution data for TIME Data 
    df_time_pollution = spark.sql("""
                    SELECT DISTINCT Date_Local AS date_ref FROM Pollution WHERE Date_Local IS NOT NULL 
                """)

    # Extract columns to create Time table
    df_time_pollution = df_time_pollution.withColumn('day',dayofmonth(df_time_pollution.date_ref)) \
        .withColumn('week',weekofyear(df_time_pollution.date_ref)) \
        .withColumn('month',month(df_time_pollution.date_ref)) \
        .withColumn('year',year(df_time_pollution.date_ref)) \
        .withColumn('weekday',dayofweek(df_time_pollution.date_ref))

    # Write only TIME data to Temp Storage
    df_time_pollution.write.parquet(time_location_temp, mode="append")

    ########################################################################
    # Read in Immigration data to use for Time table
    time_temp_output = output_data + "Time_temp/"
    df_time_temp = spark.read.parquet(time_temp_output)
    df_time_temp.createOrReplaceTempView("Time_temp")
    
    # Create the data model of TIME Data
    df_time_final = spark.sql("""
                    SELECT DISTINCT date_ref, day, week, month, year, weekday 
                    FROM Time_temp  
                """)
    
    # Count records of Time table for data quality checks
    count_time_table = df_time_final.count()
    print('The data in "{}" table have {} record(s)'.format('Time', count_time_table))
    
    # write time table to parquet files partitioned by year and month
    time_location= os.path.join(output_data, "Time")
    df_time_final.write.partitionBy("year", "month").parquet(time_location, mode="overwrite")

def main():
    """
    Create Spark Session and provide input and output location in S3. Its Start point for this ETL process. 
    
    - Read the config AWS credential from `dl.cfg`
    - Call create_spark_session() to create Spark Session for use in other function
    - Prepare input_data and output_data location in S3
    - Call process_immigration_data() function for Load data from S3, ETL and write to parquet files in S3 (immigration table)
    - Call process_worldtemp_data() function for Load data from S3, ETL and write to parquet files in S3 (worldtemp table)
    - Call process_demographic_data() function for Load data from S3, ETL and write to parquet files in S3 (demographic table)
    - Call process_pollution_data() function for Load data from S3, ETL and write to parquet files in S3 (pollution table)
    - Call process_airline_data() function for Load data from S3, ETL and write to parquet files in S3 (airline table)
    - Call process_time_data() function for Load data from S3, ETL and write to parquet files in S3 (time table)
    - Finally, stop the Spark session
    
    """
    
    print("## Welcome to 'The pollution problems that related to immigration in the U.S.' project ##")
    print("=> The ETL Process is Start!")
    
    spark = create_spark_session()
    input_data = "s3a://capstoneproject-phakphoom/Capstone_Project/DataSource/"
    output_data = "s3a://capstoneproject-phakphoom/Capstone_Project/Datalake_Target/"
    
    print('\n***** Summary records each tables *****')
    process_immigration_data(spark, input_data, output_data)    
    process_worldtemp_data(spark, input_data, output_data)
    process_demographic_data(spark, input_data, output_data)
    process_pollution_data(spark, input_data, output_data)
    process_airline_data(spark, input_data, output_data)
    process_time_data(spark, input_data, output_data)
    print("=> The ETL Process is End!!")

    spark.stop()


if __name__ == "__main__":
    main()
