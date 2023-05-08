import os 
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from helpers import get_primary_keys

        
def load_staging_tables(cur, conn):
    """
    Copies the files stored in the s3 bucket and load data into staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Transforms the staging data and inserts into fact and dimension tables
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

    
def dq_null_primary_key_check(tables_to_check, cur, conn):
    """
    Runs a data quality check for null primary keys
    Executes a query to find any row where any of the primary keys is null
    Raises Exception if any of the primary keys returns null
    """
    
    table_primary_key_dict=get_primary_keys(cur, conn)
    
    for table in tables_to_check:
        table_primary_keys = table_primary_key_dict.get(table)
        
        if len(table_primary_keys)>1:
            primary_key_str=" OR ".join(primary_key + " IS NULL" for primary_key in table_primary_keys)
            
        else:
            primary_key_str=table_primary_keys[0]
        
        primary_key_null_query = f"""
        SELECT *
        FROM {table}
        WHERE {primary_key_str} IS NULL
        LIMIT 1
        """
        
        result=cur.execute(primary_key_null_query)
        result = cur.fetchall()
        
        if len(result) >0:
            raise Exception(f"Data quality check failed. There are rows where primary keys are null! Check this row record {result}.")
        else:
            print(f"Data quality check passed. There are no null primary keys.")

def dq_duplicated_primary_key_check(tables_to_check, cur, conn):
    """
    Runs a data quality check duplicated keys 
    Executes a query to compare row count against distinct count primary keys.
    Raises Exception of this comparison yields FALSE, which means that primary keys are not unique
    """
    
    table_primary_key_dict=get_primary_keys(cur, conn)
        
    for table in tables_to_check:
        table_primary_keys = table_primary_key_dict.get(table)
        if len(table_primary_keys)>1:
            primary_key_str=" || '_' || ".join(primary_key for primary_key in table_primary_keys)
        else:
            primary_key_str=table_primary_keys[0]
            
        duplicated_primary_key_query = f"""
        SELECT COUNT(*) = COUNT(DISTINCT {primary_key_str})
        FROM {table}
        """
        
        result=cur.execute(duplicated_primary_key_query)
        result = cur.fetchall()
        
        if len(result) >0:
            result = result[0][0]

            if result is False:
                raise Exception(f"Data quality check failed. Duplicated primary keys found.")
            else:
                print("Data quality check passed. Primary keys are not duplicated.")

def main():
    """
    - Fetches config values
    - Establishes connection with the sparkify database and gets cursor to it.
    - Loads data into the staging tables
    - Inserts data into fact and dimension tables
    - Runs 2 data quality checks on the fact and dimension tables
    - Finally, closes the connection.
    """

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    os.environ["AWS_ACCESS_KEY_ID"] = config["AWS"]["KEY"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS"]["SECRET"]

    BUCKET = config.get('S3','BUCKET')
    S3_BUCKET_PATH = "s3a://"+BUCKET
    S3_BUCKET_COPY_PATH = "s3://"+BUCKET

#     SAS_FILE = config.get('FILE_PATH','SAS_FILE')
#     STATES_FILE = config.get('FILE_PATH','STATES_FILE')
#     UNEMPLOYMENT_FILE = config.get('FILE_PATH','UNEMPLOYMENT_FILE')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"\
                            .format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    table_names=["fact_immigration", "dimension_origin", "dimension_states", "dimension_unemployment"]
    
    get_primary_keys(cur, conn)
    
    dq_null_primary_key_check(table_names, cur, conn)
    dq_duplicated_primary_key_check(table_names, cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()
