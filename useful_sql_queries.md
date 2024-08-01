## SQL - transaction table and notification

### Access

  CREATE ROLE engineer WITH ENCRYPTED PASSWORD your_password_here;

  GRANT ALL PRIVILEGES ON DATABASE postgres TO engineer;
GRANT ALL PRIVILEGES ON SCHEMA public TO engineer;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO engineer;



### Transaction table (may need to rerun permissions after creation of this table, note no references defined)

  CREATE TABLE IF NOT EXISTS transaction_data (
        id INTEGER NOT NULL,
        order_time TIMESTAMP NOT NULL,
        temprature FLOAT,
        percipitation FLOAT,
        PRIMARY KEY(id, order_time)
    );



### Notification and trigger created

  -- Create a function that sends a NOTIFY message
CREATE OR REPLACE FUNCTION notify_event() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('weather_channel', NEW::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

  -- Create a trigger that calls the function after an insert occurs
CREATE TRIGGER weather_trigger
AFTER INSERT ON transaction_data
FOR EACH ROW EXECUTE FUNCTION notify_event();

### INSERTION OF DATA TO TRIGGER AUTOMATION

INSERT INTO transaction_data (id,order_time) 

VALUES (1,'2024-08-01 13:30:00');
