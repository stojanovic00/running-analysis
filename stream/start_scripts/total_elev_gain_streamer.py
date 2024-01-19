import os
import time

import psycopg2
from psycopg2 import sql
from datetime import datetime

# PostgreSQL connection parameters
jdbc_url = "jdbc:postgresql://localhost:5000/running-analytics"
pg_properties = {
    "host": "localhost",
    "port": "5000",
    "database": "running-analytics",
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

def query_last_entry(table_name):
    # Establish a connection to the PostgreSQL database
    connection = psycopg2.connect(
        host=pg_properties["host"],
        port=pg_properties["port"],
        user=pg_properties["user"],
        password=pg_properties["password"],
        database=pg_properties["database"]
    )

    try:
        # Create a cursor object to interact with the database
        cursor = connection.cursor()

        # Build the SQL query to get the last entry from the specified table
        query = sql.SQL("SELECT * FROM {} ORDER BY time DESC LIMIT 1").format(sql.Identifier(table_name))

        # Execute the query
        cursor.execute(query)

        # Fetch the result
        result = cursor.fetchone()

        # Clear the terminal screen
        os.system('clear')

        if result:
            # Extract and format the relevant information
            time, elevation_gain = result[0], result[1]
            formatted_time = datetime.strftime(time, "%Y-%m-%d %H:%M:%S")

            print(f"({formatted_time}) Total elevation gain: {round(elevation_gain, 4)}")
        else:
            print("Runner didn't start yet.")

    finally:
        # Close the cursor and the connection
        cursor.close()
        connection.close()

if __name__ == "__main__":
    # Specify the table name to query
    table_name_to_query = "elev_gain_total"

    while True:
        # Call the function to query the last entry
        query_last_entry(table_name_to_query)

        # Sleep for 5 seconds
        time.sleep(2)