from airflow.models import BaseOperator
import psycopg2
import pandas as pd

# RDS credentials
RDS_HOST = 'spotify-data-store.cl8280ymitkt.eu-west-2.rds.amazonaws.com'
RDS_USER = 'postgres'
RDS_PASSWORD = 'x3bmkisG7lRZYorWvQK6'
RDS_DB_NAME = 'spotify-data-store' 
PORT=5432

class SpotifyToRDSOperator(BaseOperator):
    def insert_into_rds(track_df):
        # Connect to RDS database     
        conn = psycopg2.connect(host=RDS_HOST, user=RDS_USER, password=RDS_PASSWORD, database=RDS_DB_NAME)
        cursor = conn.cursor()

        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS spotify_data (
            id SERIAL PRIMARY KEY,
            artist VARCHAR(255),
            album VARCHAR(255),
            track_name VARCHAR(255),
            track_id VARCHAR(255),
            timestamp DOUBLE PRECISION
        );
        """
        cursor.execute(create_table_query)
        conn.commit()


        # Insert data into the table from the csv (need to complete this)
        for _, row in track_df.iterrows():
            insert_query = f"""
            INSERT INTO spotify_data (artist, album, track_name, track_id, timestamp)
            VALUES ('{row["artist"]}', '{row["album"]}', '{row["track_name"]}', '{row["track_id"]}', {row["timestamp"]});
            """
            cursor.execute(insert_query)
            conn.commit()
        
        # if successful delete temp file

        # Close the connection
        cursor.close()
        conn.close()
        


