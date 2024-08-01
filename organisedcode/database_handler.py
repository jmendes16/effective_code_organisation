import pandas as pd
from sqlalchemy import create_engine, Table, MetaData, insert, select, URL

def get_data(filename_csv):
    data = pd.read_csv(filename_csv)
    return data.to_dict('records')

def get_sql_engine(user, passwd, host, database):
    conn_string = URL.create(
        "postgresql+psycopg2",
        username=user,
        password=passwd,
        host=host,
        database=database,
    )
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