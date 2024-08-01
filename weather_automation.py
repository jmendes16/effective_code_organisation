import psycopg2
import select
import configparser
import requests
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from organisedcode.database_handler import get_sql_engine, connection_manager

# read the configuration file
config = configparser.ConfigParser()
config.read('my_config.ini')

# your supabase parameters
POSTGRES_HOST = config.get('LOCAL_DB','host')
POSTGRES_USER = config.get('LOCAL_DB','username')
POSTGRES_PASSWORD = config.get('LOCAL_DB','password')
POSTGRES_DATABASE = config.get('LOCAL_DB','db')

# Create an SQLAlchemy engine
engine = get_sql_engine(POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_HOST, POSTGRES_DATABASE)

# Create a sessionmaker factory using the engine
Session = sessionmaker(bind=engine)

def listen_to_db():
    try:
        # Use SQLAlchemy engine to get the raw connection
        conn = engine.raw_connection()

        # Set isolation level to autocommit for listening to notifications
        conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        # Create a cursor using the raw connection
        cur = conn.cursor()

        # Listen to the specified channel
        cur.execute("LISTEN weather_channel;")
        print("Waiting for notifications on channel 'weather_channel'...")

        while True:
            if select.select([conn], [], [], 5) == ([], [], []):
                print("Timeout, no notifications received.")
            else:
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    print(f"Got NOTIFY: {notify.payload}")
                    # Call the function to handle the notification
                    handle_event(notify)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        if conn:
            cur.close()
            conn.close()

def handle_event(notify):
    print(f"Handling event: {notify.payload}")

    try:
        # clean the database notification for processing using SQL
        event_string = notify.payload.strip('()').replace('"',"'")
        tuple_value = tuple(event_string.split(','))
        # Ensure it's actually a tuple before proceeding
        if isinstance(tuple_value, tuple):
            customer_id = int(tuple_value[0])
            time = tuple_value[1]
            # retrieve geolocation data
            query = text(f'''
                SELECT longitude, latitude
                FROM location_data
                JOIN customer_data USING(postcode)
                WHERE id = {customer_id}
            ''')
            result = connection_manager(engine,
                   query
                  ).fetchone()
            
            # get weather data from API
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                "latitude": result[0],
                "longitude": result[1],
                "daily": ["temperature_2m_max", "precipitation_sum"]
            }
            response = requests.get(url, params)
            temp = response.json()["daily"]["temperature_2m_max"][0]
            precipitation = response.json()["daily"]["precipitation_sum"][0]
            
            # update transactions table to include this weather data
            query = text(f'''
                UPDATE transaction_data
                SET temprature = {temp}, percipitation = {precipitation}
                WHERE id = {customer_id} AND order_time = {time}
            ''')
            result = connection_manager(engine,query)

        else:
            print("Data extraction issue, unable to retrieve customer_id")
    except (ValueError, SyntaxError) as e:
        print(f"Error occurred: {e}")

if __name__ == "__main__":
    listen_to_db()
