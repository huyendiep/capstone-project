import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


# DROP TABLES
#Staging tables
staging_fact_immigration_table_drop = "DROP TABLE IF EXISTS staging_fact_immigration"
staging_dimension_states_table_drop = "DROP TABLE IF EXISTS staging_dimension_states"
staging_dimension_origin_table_drop = "DROP TABLE IF EXISTS staging_dimension_origin"
staging_dimension_unemployment_table_drop = "DROP TABLE IF EXISTS staging_dimension_unemployment"

#Fact & dim tables
fact_immigration_table_drop = "DROP TABLE IF EXISTS fact_immigration"
dimension_states_table_drop = "DROP TABLE IF EXISTS dimension_states"
dimension_origin_table_drop = "DROP TABLE IF EXISTS dimension_origin"
dimension_unemployment_table_drop = "DROP TABLE IF EXISTS dimension_unemployment"


# CREATE STAGING TABLES

staging_fact_immigration_table_create = ("""
CREATE TABLE staging_fact_immigration (
 immigration_id bigint PRIMARY KEY NOT NULL
, country_of_birth bigint
, country_of_residence bigint
, arrival_port varchar
, arrival_date date
, arrival_mode int
, address_state_code varchar
, departure_date date
, respondent_age int
, visa_category int
, date_file date
, visa_issue_authority varchar
, year_of_birth int
, permitted_to_stay_until date
, gender varchar
, arrival_airline varchar
, admission_number bigint
, flight_number varchar
)
""")

# CREATE DIMENSION TABLES
staging_dimension_origin_table_create = ("""
CREATE TABLE staging_dimension_origin (
    country_code bigint PRIMARY KEY NOT NULL,
    country_name varchar
        )
""")


staging_dimension_states_table_create = ("""
CREATE TABLE staging_dimension_states (
    state_name varchar PRIMARY KEY NOT NULL,
    abbreviation varchar
    )
""")


staging_dimension_unemployment_table_create = ("""
CREATE TABLE staging_dimension_unemployment (
    year int NOT NULL,
    month int NOT NULL,
    state varchar NOT NULL,
    avg_rate double precision,
    PRIMARY KEY (year, month, state)
    )
""")


# CREATE FACT TABLES
fact_immigration_table_create = ("""
CREATE TABLE fact_immigration (
 immigration_id bigint PRIMARY KEY NOT NULL
, country_of_birth int
, country_of_residence int
, arrival_port varchar
, arrival_date date
, arrival_mode int
, address_state_code varchar
, departure_date date
, respondent_age int
, visa_category int
, date_file date
, visa_issue_authority varchar
, year_of_birth int
, permitted_to_stay_until date
, gender varchar
, arrival_airline varchar
, admission_number bigint
, flight_number varchar
)
""")


# CREATE DIMENSION TABLES
dimension_origin_table_create = ("""
CREATE TABLE dimension_origin (
    country_code bigint PRIMARY KEY NOT NULL,
    country_name varchar
    )
""")

dimension_states_table_create = ("""
CREATE TABLE dimension_states (
    state_name varchar PRIMARY KEY NOT NULL,
    abbreviation varchar
    )
""")


dimension_unemployment_table_create = ("""
CREATE TABLE dimension_unemployment (
    year int NOT NULL,
    month int NOT NULL,
    state varchar NOT NULL,
    avg_rate double precision,
    PRIMARY KEY (year, month, state)
    )
""")


# COPY FROM S3

#FACT TABLE
S3_BUCKET_COPY_PATH = "s3://"+config.get('S3','BUCKET')

fact_immigration_table_read_s3_path = f"{S3_BUCKET_COPY_PATH}/fact_tables/fact_immigration"

fact_immigration_copy = ("""
COPY staging_fact_immigration FROM '{}'
iam_role '{}'
FORMAT as PARQUET
""").format(fact_immigration_table_read_s3_path, config.get('IAM_ROLE','ARN'))


#DIMENSIONS TABLES
dimension_tables = ["dimension_origin", "dimension_states", "dimension_unemployment"]

dimension_table_copy = []

for table in dimension_tables:

    read_from_s3_path = f"{S3_BUCKET_COPY_PATH}/dimension_tables/{table}"

    dimension_copy_query = ("""
        COPY staging_{} FROM '{}'
        iam_role '{}'
        FORMAT as PARQUET
    """).format(table, read_from_s3_path, config.get('IAM_ROLE','ARN'))

    dimension_table_copy+=[dimension_copy_query]

# INSERT INTO FACT TABLES
fact_immigration_table_insert = (
        """
        INSERT INTO fact_immigration (
        immigration_id
        , country_of_birth
        , country_of_residence
        , arrival_port
        , arrival_date
        , arrival_mode
        , address_state_code
        , departure_date
        , respondent_age
        , visa_category
        , date_file
        , visa_issue_authority
        , year_of_birth
        , permitted_to_stay_until
        , gender
        , arrival_airline
        , admission_number
        , flight_number
        )
        SELECT DISTINCT
        immigration_id
        , country_of_birth
        , country_of_residence
        , arrival_port
        , arrival_date
        , arrival_mode
        , address_state_code
        , departure_date
        , respondent_age
        , visa_category
        , date_file
        , visa_issue_authority
        , year_of_birth
        , permitted_to_stay_until
        , gender
        , arrival_airline
        , admission_number
        , flight_number
        FROM staging_fact_immigration
        WHERE immigration_id NOT IN (SELECT immigration_id FROM fact_immigration)
        """)


# INSERT INTO DIMENSION TABLES
dimension_states_table_insert = ("""
DELETE FROM dimension_states
USING staging_dimension_states
WHERE dimension_states.state_name = staging_dimension_states.state_name;

INSERT INTO dimension_states (
    state_name 
    , abbreviation 
)
SELECT DISTINCT
    state_name
    , abbreviation 
FROM staging_dimension_states
"""                              
)

dimension_origin_table_insert = (
"""
DELETE FROM dimension_origin 
USING staging_dimension_origin 
WHERE dimension_origin.country_code = staging_dimension_origin.country_code; 

INSERT INTO dimension_origin (
    country_code,
    country_name
)
SELECT DISTINCT
    country_code
    , country_name
FROM staging_dimension_origin
""")


dimension_unemployment_table_insert = (
"""
DELETE FROM dimension_unemployment
USING staging_dimension_unemployment
WHERE dimension_unemployment.year = staging_dimension_unemployment.year
AND dimension_unemployment.month = staging_dimension_unemployment.month
AND dimension_unemployment.state = staging_dimension_unemployment.state;

INSERT INTO dimension_unemployment (
    year
    , month
    , state
    , avg_rate
)
SELECT DISTINCT
    year
    , month
    , state
    , avg_rate
FROM staging_dimension_unemployment
"""
)

#DATA QUALITY CHECKS

get_primary_key_query = ("""
SELECT tc.table_name, column_name
    FROM information_schema.key_column_usage as kcu
    JOIN information_schema.table_constraints tc on kcu.constraint_name= tc.constraint_name 
    WHERE constraint_type='PRIMARY KEY'""")

# QUERY LISTS
create_table_queries = [
    staging_fact_immigration_table_create,
    staging_dimension_origin_table_create,
    staging_dimension_states_table_create,
    staging_dimension_unemployment_table_create,
    fact_immigration_table_create,
    dimension_origin_table_create,
    dimension_states_table_create,
    dimension_unemployment_table_create
]

drop_table_queries = [
    staging_fact_immigration_table_drop,
    staging_dimension_states_table_drop,
    staging_dimension_origin_table_drop,
    staging_dimension_unemployment_table_drop,
    fact_immigration_table_drop,
    dimension_states_table_drop,
    dimension_origin_table_drop,
    dimension_unemployment_table_drop]

copy_table_queries = [fact_immigration_copy] + dimension_table_copy

insert_table_queries = [fact_immigration_table_insert, dimension_states_table_insert, dimension_origin_table_insert, dimension_unemployment_table_insert]
