import streamlit as st
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from utils import generate_unique_id
import json
import os

class Database:
    """MySQL database for storing scraping results and cron schedules"""

    def __init__(self, mysql_uri):
        """Initialize the MySQL connection and create tables if needed."""
        self.mysql_uri = mysql_uri
        self.connection = self.connect_db()

        if self.connection:
            self.create_tables()

    def connect_db(self):
        """Establish a connection to MySQL database."""
        try:
            conn = mysql.connector.connect(
                host="mysql-e613014-moviefordsandhu-195f.c.aivencloud.com",#
                user="avnadmin",#"scrapper"
                password="AVNS_J8TyM_YEio9Cfp3rYck", #"nDbyL3jrSwmdkakn" , 
                database="defaultdb", #"scrapper",
                port=21010
                
            )
            if conn.is_connected():
                print("Connected to MySQL database!")
                return conn
        except Error as e:
            print(f"Database connection error: {e}")
            return None

    def create_tables(self):
        """Create necessary tables if they do not exist."""
        create_raw_html_table = """
        CREATE TABLE IF NOT EXISTS raw_html (
            id VARCHAR(50) PRIMARY KEY,
            url TEXT NOT NULL,
            html LONGTEXT NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            scrape_id VARCHAR(50),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        );
        """

        create_extracted_data_table = """
        CREATE TABLE IF NOT EXISTS extracted_data (
            id VARCHAR(50) PRIMARY KEY,
            raw_id VARCHAR(50),
            url TEXT NOT NULL,
            data JSON NOT NULL,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            scrape_id VARCHAR(50),
            FOREIGN KEY (raw_id) REFERENCES raw_html(id) ON DELETE CASCADE
        );
        """
        create_api_key_table= """
                            CREATE TABLE IF NOT EXISTS api_keys (
                        id VARCHAR(50) PRIMARY KEY,
                        provider VARCHAR(50) UNIQUE NOT NULL,
                        api_key TEXT NOT NULL,
                        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                    );"""
        create_cron_schedule_table = """
        CREATE TABLE IF NOT EXISTS cron_schedule (
            id VARCHAR(50) PRIMARY KEY,
            job_name VARCHAR(255) NOT NULL,
            schedule_time VARCHAR(50) NOT NULL,
            website VARCHAR(50) NOT NULL,
            data VARCHAR(50) NOT NULL,
            status ENUM('active', 'inactive') DEFAULT 'active',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """

        with self.connection.cursor() as cursor:
            cursor.execute(create_api_key_table)
            cursor.execute(create_raw_html_table)
            cursor.execute(create_extracted_data_table)
            cursor.execute(create_cron_schedule_table)
            self.connection.commit()
    def save_or_update_api_key(self, provider, key):
        """Insert or update an API key for a provider."""
        query = """
        INSERT INTO api_keys (id, provider, api_key)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE api_key = VALUES(api_key)
        """
        key_id = generate_unique_id()
        values = (key_id, provider, key)
        with self.connection.cursor() as cursor:
            cursor.execute(query, values)
            self.connection.commit()

    def save_api_key(self, provider, key):
        """Store or update an API key."""
        query = """
        INSERT INTO api_keys (id, provider, api_key)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE api_key = VALUES(api_key)
        """
        key_id = generate_unique_id()
        values = (key_id, provider, key)
        with self.connection.cursor() as cursor:
            cursor.execute(query, values)
            self.connection.commit()
    def get_api_key(self, provider):
        """Retrieve an API key for a provider."""
        query = "SELECT api_key FROM api_keys WHERE provider = %s"
        with self.connection.cursor() as cursor:
            cursor.execute(query, (provider,))
            result = cursor.fetchone()
        return result[0] if result else None


    def save_raw_html(self, url, html_content):
        """Save raw HTML content from a URL into MySQL."""
        item_id = generate_unique_id()
        scrape_id = st.session_state.get('scrape_id', 'unknown')

        query = """
        INSERT INTO raw_html (id, url, html, scrape_id)
        VALUES (%s, %s, %s, %s)
        """
        values = (item_id, url, html_content, scrape_id)

        with self.connection.cursor() as cursor:
            cursor.execute(query, values)
            self.connection.commit()

        return item_id

    def get_most_recent_updated_id(self):
        """Get the ID of the most recently updated row from the raw_html table."""
        query = """
        SELECT id
        FROM raw_html
        ORDER BY updated_at DESC
        LIMIT 1
        """
        
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()

        # Return the ID if there's a result, otherwise return None
        return result[0] if result else None
    def clear_all_data(self):
        """Remove all data from the extracted_data table."""
        query = "TRUNCATE TABLE extracted_data"  # or "DELETE FROM extracted_data"
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
            return True
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Failed to clear data: {str(e)}")
        return False

    from datetime import datetime

    def save_extracted_data(self, raw_id, url, extracted_data):
        """Save extracted data linked to raw HTML in MySQL with explicit timestamp."""
        item_id = generate_unique_id()
        scrape_id = st.session_state.get('scrape_id', 'unknown')
        json_data = json.dumps(extracted_data)
        current_timestamp = datetime.now()  # Explicitly set timestamp
        query = """
        INSERT INTO extracted_data (id, raw_id, url, data, scrape_id, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        values = (item_id, raw_id, url, json_data, scrape_id, current_timestamp)

        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, values)
                self.connection.commit()
        except Exception as e:
            self.connection.rollback()
            raise Exception(f"Failed to save data: {str(e)}")

        return item_id
    def get_all_data(self):
        """Retrieve all extracted data from MySQL, sorted by timestamp descending."""
        query = "SELECT url, data, timestamp FROM extracted_data ORDER BY timestamp DESC"
        results = []

        try:
            with self.connection.cursor(dictionary=True) as cursor:
                cursor.execute(query)
                rows = cursor.fetchall()

                for row in rows:
                    base_info = {
                        'URL': row['url'],
                        'Timestamp': row['timestamp']  # Should be a datetime object from MySQL
                    }

                    # Safely parse JSON data
                    extracted_data = json.loads(row['data']) if isinstance(row['data'], str) else row['data']
                    for key, value in extracted_data.items():
                        base_info[key] = ', '.join(value) if isinstance(value, list) else value

                    results.append(base_info)

        except Exception as e:
            raise Exception(f"Failed to fetch data: {str(e)}")

        return results
    def delete_data(self, url):
        """Delete a record from the database based on URL."""
        query = "DELETE FROM extracted_data WHERE url = %s"
        with self.connection.cursor() as cursor:
            cursor.execute(query, (url,))
            self.connection.commit()
    def save_cron_schedule(self, job_name, website,schedule_time, status='active',data=""):
        """Save a cron schedule into MySQL."""
        job_id = generate_unique_id()
        query = """
        INSERT INTO cron_schedule (id, job_name, schedule_time, status)
        VALUES (%s, %s, %s, %s)
        """
        values = (job_id, job_name, schedule_time, status)

        with self.connection.cursor() as cursor:
            cursor.execute(query, values)
            self.connection.commit()
        return job_id

    def get_cron_schedules(self):
        """Retrieve all cron schedules."""
        query = "SELECT * FROM cron_schedule"
        with self.connection.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def update_cron_schedule(self, job_id, job_name=None, schedule_time=None, status=None):
        """Update an existing cron schedule."""
        updates = []
        values = []
        
        if job_name:
            updates.append("job_name = %s")
            values.append(job_name)
        if schedule_time:
            updates.append("schedule_time = %s")
            values.append(schedule_time)
        if status:
            updates.append("status = %s")
            values.append(status)
        
        if not updates:
            return False
        
        values.append(job_id)
        query = f"UPDATE cron_schedule SET {', '.join(updates)} WHERE id = %s"
        
        with self.connection.cursor() as cursor:
            cursor.execute(query, values)
            self.connection.commit()
        return True

    def delete_cron_schedule(self, job_id):
        """Delete a cron schedule from MySQL."""
        query = "DELETE FROM cron_schedule WHERE id = %s"
        with self.connection.cursor() as cursor:
            cursor.execute(query, (job_id,))
            self.connection.commit()
        return True

    def close_connection(self):
        """Close the database connection."""
        if self.connection.is_connected():
            self.connection.close()
            st.info("Database connection closed.")

db= Database("fd")
# db=""