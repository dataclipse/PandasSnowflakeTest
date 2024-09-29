import pandas as pandas_data
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import warnings

#Ignore UserWarnings
warnings.filterwarnings("ignore", category=UserWarning)

# Snowflake connection parameters
SNOWFLAKE_ACCOUNT = 'QPOWBAL-VL87933'
SNOWFLAKE_USER = 'COVID_DATA_USER'
SNOWFLAKE_PASSWORD = '*********' #removed before pushed to github
SNOWFLAKE_WAREHOUSE = 'COMPUTE_WH'
SNOWFLAKE_DATABASE = 'COVID_DATA'
SNOWFLAKE_SCHEMA = 'COVID_DATA_SCHEMA'
SNOWFLAKE_ROLE = 'COVID_DATA_RW'

print("Connecting...\n")
# Establish a Snowflake connection
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    role=SNOWFLAKE_ROLE,
    IgnoreCase=True
)

#Create Cursor for the Connection
cursor = conn.cursor()

# Query to select all data from a specific table
query = "SELECT COUNTRY, CASES_NEW, DEATHS_NEW, DATE FROM COVID19_WHO_SITUATION_REPORTS;" 

# Fetch data from Snowflake into a Pandas DataFrame
df = pandas_data.read_sql(query, conn)

# Check for missing data
print("Number of null values in dataset: \n") 
print(df.isnull().sum(), "\n")
print("Removing null values (if any)...\n")

# Fill missing cases and death values with 0 
df['CASES_NEW'] = df['CASES_NEW'].fillna(0)
df['DEATHS_NEW'] = df['DEATHS_NEW'].fillna(0)

# Remove rows with negative case or death numbers to remove any outliers
print("Removing negative values (if any) to remove outliers...\n")
df = df[df['CASES_NEW'] >= 0]
df = df[df['DEATHS_NEW'] >= 0]

# Convert the Date Column to the proper Date format
print("Convert Date Column to Date format\n")
df['DATE'] = pandas_data.to_datetime(df['DATE'])

#Display the top 10 countries sorted by new case count for the date of 2020-08-09
print("Top 10 Countries sorted by case count for the date of 2020-08-09\n")
top_countries = df[df['DATE'] == '2020-08-09'].sort_values(by='CASES_NEW', ascending=False).head(10)

#Convert Date field back to Snowflake compatible Date
top_countries['DATE'] = top_countries['DATE'].dt.date
#print(top_countries, "\n")

#Sum all the new cases and new deaths grouped by country
print("Sum of all New Cases and New Deaths Grouped by Country...\n")
total_by_country = df.groupby('COUNTRY')[['CASES_NEW', 'DEATHS_NEW']].sum().reset_index()
#print(total_by_country, "\n")

#Create New Tables
print("Creating new tables...\n")
create_table_query0 = """
CREATE OR REPLACE TABLE TOP_10_COUNTRIES_20200809 (
    COUNTRY VARCHAR(16777216),
    CASES_NEW NUMBER(38,0),
    DEATHS_NEW NUMBER(38,0),
    DATE DATE
);
"""
create_table_query1 = """
CREATE OR REPLACE TABLE COUNTRY_CASE_DEATH_COUNT (
    COUNTRY VARCHAR(16777216),
    CASES_NEW NUMBER(38,0),
    DEATHS_NEW NUMBER(38,0)
);
"""
cursor.execute(create_table_query0)
cursor.execute(create_table_query1)

#Write Data to Snowflake DB
success, nchunks, nrows, _ = write_pandas(conn, top_countries, 'TOP_10_COUNTRIES_20200809')

#Verify the result
if success:
    print("Successfully inserted ", nrows, " rows into 'TOP_10_COUNTRIES_20200809'.")
else:
    print("Data insertion failed.")
    
#Write Data to Snowflake DB    
success, nchunks, nrows, _ = write_pandas(conn, total_by_country, 'COUNTRY_CASE_DEATH_COUNT')

#Verify the result
if success:
    print("Successfully inserted ", nrows, " rows into 'COUNTRY_CASE_DEATH_COUNT'.")
else:
    print("Data insertion failed.")

# Close the Snowflake connection
conn.close()