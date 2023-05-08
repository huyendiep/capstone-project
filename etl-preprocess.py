import os 
import configparser
import glob
import boto3
import pandas as pd
import psycopg2
from datetime import datetime
from helpers import code_mapper, check_write_to_s3


#Spark imports
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, DoubleType
from pyspark.sql.functions import col, to_date, expr, count, month, avg


def upload_files_to_s3(bucket, dir_path):
    """
    Upload all raw files in a directory to S3
    """
    session = boto3.Session()
    s3 = session.resource('s3')
    
    for filepath in glob.iglob(dir_path):
        s3.meta.client.upload_file(Filename=filepath, Bucket=bucket, Key=filepath)

def create_spark_session():
    """
    Fetches the existing Spark session if there is one,
    otherwise a new Spark session is created
    with hadoop-aws package
    """
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()
    
    return spark

def process_fact_table(filepath, s3_bucket_path):
    spark = create_spark_session()
    df_i94=spark.read.parquet(filepath)

    fact_immigration = df_i94.select(['cicid',
    'i94cit',
    'i94res',
    'i94port',
    'arrdate',
    'i94mode',
    'i94addr',
    'depdate',
    'i94bir',
    'i94visa',
    'dtadfile',
    'visapost',
    'biryear',
    'dtaddto',
    'gender',
    'airline',
    'admnum',
    'fltno'])
    
    fact_immigration_converted_dates = (
    fact_immigration
        .withColumn('arrdate', expr("date_add(date('1960-01-01'), CAST(arrdate AS INT))"))
        .withColumn('depdate', expr("date_add(date('1960-01-01'), CAST(depdate AS INT))"))
        .withColumn('dtadfile',to_date(col("dtadfile"), "yyyyMMdd"))
        .withColumn('dtaddto',to_date(col("dtadfile"), "MMddyyyy"))
                     )

    convert_to_int_columns =['i94cit','i94res', 'i94mode', 'i94bir', 'i94visa', 'biryear']

    convert_to_big_int_columns = ['cicid', 'admnum']


    fact_immigration_correct_data_type = fact_immigration_converted_dates

    for column in convert_to_int_columns:
        fact_immigration_correct_data_type = fact_immigration_correct_data_type.withColumn(column, col(column).cast(IntegerType()))

    for column in convert_to_big_int_columns: 
        fact_immigration_correct_data_type = fact_immigration_correct_data_type.withColumn(column, col(column).cast(LongType()))

    old_to_new_names = [
    {'source_column_name': 'cicid', 'update_column_name': 'immigration_id'}
    ,{'source_column_name': 'i94cit', 'update_column_name': 'country_of_birth'}
    ,{'source_column_name': 'i94res', 'update_column_name': 'country_of_residence'}
    ,{'source_column_name': 'i94port', 'update_column_name': 'arrival_port'}
    ,{'source_column_name': 'arrdate', 'update_column_name': 'arrival_date'}
    ,{'source_column_name': 'i94mode', 'update_column_name': 'arrival_mode'}
    ,{'source_column_name': 'i94addr', 'update_column_name': 'address_state_code'}
    ,{'source_column_name': 'depdate', 'update_column_name': 'departure_date'}
    ,{'source_column_name': 'i94bir', 'update_column_name': 'respondent_age'}
    ,{'source_column_name': 'i94visa', 'update_column_name': 'visa_category'}
    ,{'source_column_name': 'dtadfile', 'update_column_name': 'date_file'}
    ,{'source_column_name': 'visapost', 'update_column_name': 'visa_issue_authority'}
    ,{'source_column_name': 'biryear', 'update_column_name': 'year_of_birth'}
    ,{'source_column_name': 'dtaddto', 'update_column_name': 'permitted_to_stay_until'}
    ,{'source_column_name': 'gender', 'update_column_name': 'gender'}
    ,{'source_column_name': 'airline', 'update_column_name': 'arrival_airline'}
    ,{'source_column_name': 'admnum', 'update_column_name': 'admission_number'}
    ,{'source_column_name': 'fltno', 'update_column_name': 'flight_number'}]
    
    fact_immigration_update_column_names=fact_immigration_correct_data_type

    for column_name_pair in old_to_new_names:
        fact_immigration_update_column_names = fact_immigration_update_column_names.withColumnRenamed(column_name_pair['source_column_name'], column_name_pair['update_column_name'])
        
    fact_immigration = fact_immigration_update_column_names
    
    fact_immigration_table_output_s3_path = f"{s3_bucket_path}/fact_tables/fact_immigration"

    fact_immigration.write.mode("overwrite").parquet(fact_immigration_table_output_s3_path)
    
    check_write_to_s3("fact_immigration", fact_immigration, fact_immigration_table_output_s3_path)

def process_dimension_table_origin(bucket, filepath, s3_bucket_path):
    """
    Creates dimension table for mapping the country code to the human-readable name of the country
    Extracts country codes and equivalent values from the label description file into a dict
    Turns the mapping dict into a DataFrame, assigns proper column names and converts into correct data types
    Converts into Spark DataFrame and writes to S3
    """

    i94cit_res = code_mapper(bucket, filepath, "i94cntyl")
    
    df_dimension_origin= pd.Series(i94cit_res).reset_index()

    #Change column name
    df_dimension_origin.columns=["country_code", "country_name"]

    #Change data type
    df_dimension_origin["country_code"] = df_dimension_origin["country_code"].astype(int)
    
    dimension_origin_remove_dups = df_dimension_origin.drop_duplicates()
    
    spark = create_spark_session()
    
    dimension_origin = spark.createDataFrame(dimension_origin_remove_dups)

    dimension_origin_table_output_s3_path = f"{s3_bucket_path}/dimension_tables/dimension_origin"

    dimension_origin.write.mode("overwrite").parquet(dimension_origin_table_output_s3_path)
    
    check_write_to_s3("dimension_origin", dimension_origin, dimension_origin_table_output_s3_path)

def process_dimension_table_states(filepath, s3_bucket_path):
    """
    Creates a dimension table about the U.S. states based on the csv file
    Reads csv file, performs transformation on such as renaming column headers
    Writes the data to S3
    """
    
    spark = create_spark_session()
    
    df_states_data=spark.read.option("header",True).csv(filepath)
    
    #Renames columns
    dimension_states = (df_states_data
                        .withColumnRenamed('State', 'state_name')
                        .withColumnRenamed('Abbreviation', 'abbreviation')
                        .dropDuplicates()
                       )
    dimension_states_table_output_s3_path = f"{s3_bucket_path}/dimension_tables/dimension_states"

    dimension_states.write.mode("overwrite").parquet(dimension_states_table_output_s3_path)
    
    check_write_to_s3("dimension_states", dimension_states, dimension_states_table_output_s3_path)

def process_dimension_table_unemployment(filepath, s3_bucket_path):
    """
    Creates the unemployment rate dimension table
    Reads cs vfile, performs transformation on such as renaming column headers
    Rolls up the data to U.S. state grain
    Writes data to S3
    """
    
    spark = create_spark_session()
    
    df_unemployment_data=spark.read.option("header",True).csv(filepath)
    
    # Correct data types of columns
    dimension_unemployment_correct_data_type = (df_unemployment_data
                                                .withColumn('year', col('year').cast(IntegerType()))
                                                .withColumn('rate', col('rate').cast(DoubleType()))
    )
    # Convert name of month to numeric month
    dimension_unemployment_converted_date = (dimension_unemployment_correct_data_type
                                             .withColumn('month', month(to_date("month", 'MMMMM')))
                                            )
    # Drop duplicates
    dimension_unemployment_no_dups = dimension_unemployment_converted_date.dropDuplicates()
    
    # Roll up the data to state level (instead of county level) set by taking the average rate of unemployment rate
    # Needed to be on the same granularity as the states level
    dimension_unemployment_roll_up_avg = (dimension_unemployment_no_dups
                                          .groupBy("year", "month", "state")
                                          .agg(avg("rate").alias("avg_rate"))
                                         )
    dimension_unemployment = dimension_unemployment_roll_up_avg
    
    dimension_unemployment_table_output_s3_path = f"{s3_bucket_path}/dimension_tables/dimension_unemployment"
    
    dimension_unemployment.write.mode("overwrite").parquet(dimension_unemployment_table_output_s3_path)
    
    check_write_to_s3("dimension_states", dimension_unemployment, dimension_unemployment_table_output_s3_path)
    
    
def main():
    """
    - Fetches config values
    - Processes the raw data into fact tables
    - Processes the raw data into dimension tables
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["KEY"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["SECRET"]

    BUCKET = config.get('S3','BUCKET')
    S3_BUCKET_PATH = "s3a://"+BUCKET
    S3_BUCKET_COPY_PATH = "s3://"+BUCKET

    SAS_FILE = S3_BUCKET_PATH+"/"+config.get('FILE_PATH','SAS_FILE')
    ORIGIN_FILE = config.get('FILE_PATH','ORIGIN_FILE')
    STATES_FILE = S3_BUCKET_PATH+"/"+config.get('FILE_PATH','STATES_FILE')
    UNEMPLOYMENT_FILE = S3_BUCKET_PATH+"/"+config.get('FILE_PATH','UNEMPLOYMENT_FILE')

    #Upload all raw csv files to S3
    upload_files_to_s3(bucket=BUCKET, dir_path='raw_data/*.csv')
    
    upload_files_to_s3(bucket=BUCKET, dir_path='raw_data/*.SAS')
    
    #Upload all raw SAS files to S3
    upload_files_to_s3(bucket=BUCKET, dir_path='raw_data/*/*')
    
    process_fact_table(filepath=SAS_FILE, s3_bucket_path=S3_BUCKET_PATH)
    
    process_dimension_table_origin(bucket=BUCKET, filepath=ORIGIN_FILE, s3_bucket_path=S3_BUCKET_PATH)
    
    process_dimension_table_states(filepath=STATES_FILE, s3_bucket_path=S3_BUCKET_PATH)
    
    process_dimension_table_unemployment(filepath=UNEMPLOYMENT_FILE, s3_bucket_path=S3_BUCKET_PATH)

if __name__ == "__main__":
    main()
