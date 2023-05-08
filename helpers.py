from sql_queries import get_primary_key_query
import warnings
import configparser
from pyspark.sql import SparkSession
import boto3
import os

def code_mapper(bucket_name, s3_key, header):
    """
    Creates a mapper dictionary from the label description file provided
    """
    s3 = boto3.client('s3')

    obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
    f_content = obj['Body'].read().decode('utf-8')
    f_content = f_content.replace('\t', '')
    f_content2 = f_content[f_content.index(header):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic

def check_write_to_s3(table_name, upload_df, s3_uploaded_path):
    """
    Data-qualty checks that the data was written to s3 correctly
    Reads the data from s3
    Compares row count of the current dataframe to the parquet file that was just uploaded
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["KEY"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["SECRET"]
    
    spark = create_spark_session()
    
    test_output=spark.read.parquet(s3_uploaded_path)
    
    print(f"Checking data write to S3 for table: {table_name}")
                 
    if upload_df.count() != test_output.count():
        warnings.warn(f"{test_output.count()} rows, expected only {upload_df.count()} rows")
    else:
        print(f"Data write successful! {table_name}: Expected {upload_df.count()} rows, got {test_output.count()} rows from recently written file")
        
def get_primary_keys(cur, conn):

    query=get_primary_key_query

    get_primary_key_info_schema=cur.execute(query)
    
    all_primary_keys = cur.fetchall()
    
    table_primary_key_dict = {}

    for row in all_primary_keys:
        if row[0] not in table_primary_key_dict:
            table_primary_key_dict[row[0]] = [row[1]]
        else:
            table_primary_key_dict[row[0]] += [row[1]]
            
    return table_primary_key_dict

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