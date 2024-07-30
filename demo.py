import configparser
from sqlalchemy import text
from organisedcode.database_handler import *
from organisedcode.api_handler import get_long_lat, extract_long_lat

# read the configuration file
config = configparser.ConfigParser()
config.read('my_config.ini')

# your supabase parameters
POSTGRES_HOST = config.get('LOCAL_DB','host')
POSTGRES_USER = config.get('LOCAL_DB','username')
POSTGRES_PASSWORD = config.get('LOCAL_DB','password')
POSTGRES_DATABASE = config.get('LOCAL_DB','db')

# this ensures the table exists in the database before we try and write to it
engine = get_sql_engine(POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_DATABASE)

# create table sql query
create_table_sql = text("""
    CREATE TABLE IF NOT EXISTS customer_data (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255),
        postcode VARCHAR(9)
    );
    """)
connection_manager(engine, create_table_sql)

# insert the data into the table
connection_manager(engine,
                   insert_data('customer_data', engine),
                   get_data('customer_data.csv')
                  )

# making sure our target table is ready
create_table_sql = text("""
    CREATE TABLE IF NOT EXISTS location_data (
        postcode VARCHAR(9) PRIMARY KEY,
        longitude FLOAT,
        latitude FLOAT
    );
    """)
connection_manager(engine, create_table_sql)

# We grab our postcodes
result = connection_manager(engine,
                   get_10_postcodes(engine)
                  ).fetchall()
postcodes = [i[0] for i in result]

# We extract the data from the API
data = get_long_lat(postcodes)

# We filter out all the unnecessary additional information
df = extract_long_lat(data)

# We load the data into our database
connection_manager(engine,
                   insert_data('location_data',engine),
                   df.to_dict('records')
                  )