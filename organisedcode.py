import pandas as pd
import requests
from sqlalchemy import create_engine, Table, MetaData, insert, select

def get_data(filename_csv):
    data = pd.read_csv(filename_csv)
    return data.to_dict('records')

def get_sql_engine(user, passwd, host, database):
    conn_string = f'postgresql+psycopg2://{user}:{passwd}@{host}:5432/{database}'
    return create_engine(conn_string)

def connection_manager(sql_engine, query, data =[]):
    result = None
    with sql_engine.connect() as conn:
        try:
            result = conn.execute(query, data)
            conn.commit()
        except Exception as e:
            print(e)
            conn.rollback()
    return result

def insert_data(table_name, engine):
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)
    return insert(table)

def get_10_postcodes(engine):
    metadata = MetaData()
    customers = Table('customer_data', metadata, autoload_with=engine)
    location = Table('location_data', metadata, autoload_with=engine)
    result = select(customers.c.postcode).where(
        customers.c.postcode.notin_( # excluding ones we've already got geolocation for
            select(location.c.postcode)
        )).limit(10) # only 10 postcodes at a time
    return result

def get_long_lat(postcode_list):
    url = "https://api.postcodes.io/postcodes"
    params = {
    	"postcodes": postcode_list
    }
    response = requests.post(url, json = params)
    return response.json()

# Then we need to format it so we only get the bits we're interested in.  
def extract_long_lat(data):
    extracted_data = []
    for item in data['result']:
        if 'result' in item and item['result']:
            extracted_data.append({
                'postcode': item['result'].get('postcode'),
                'longitude': item['result'].get('longitude'),
                'latitude': item['result'].get('latitude')
            })
    return pd.DataFrame(extracted_data)