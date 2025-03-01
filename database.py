import streamlit as st
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from utils import generate_unique_id
import os
class Database:
    """MySQL database for storing scraping results"""

    def __init__(self, mysql_uri):
        """Initialize the MySQL connection and create tables if needed."""
        self.mysql_uri = mysql_uri
        # print(os.environ)
        self.connection = self.connect_db()

        if self.connection:
            self.create_tables()

    def connect_db(self):
        """Establish a connection to MySQL database."""
        try:
            conn = mysql.connector.connect(
                host="mysql-e613014-moviefordsandhu-195f.c.aivencloud.com",
                user="avnadmin",
                password="AVNS_J8TyM_YEio9Cfp3rYck",
                database="defaultdb",
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
            scrape_id VARCHAR(50)
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

        with self.connection.cursor() as cursor:
            cursor.execute(create_raw_html_table)
            cursor.execute(create_extracted_data_table)
            self.connection.commit()
    
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

    def save_extracted_data(self, raw_id, url, extracted_data):
        """Save extracted data linked to raw HTML in MySQL."""
        item_id = generate_unique_id()
        scrape_id = st.session_state.get('scrape_id', 'unknown')

        query = """
        INSERT INTO extracted_data (id, raw_id, url, data, scrape_id)
        VALUES (%s, %s, %s, %s, %s)
        """
        values = (item_id, raw_id, url, str(extracted_data), scrape_id)

        with self.connection.cursor() as cursor:
            cursor.execute(query, values)
            self.connection.commit()

        return item_id

    def get_all_data(self):
        """Retrieve all extracted data from MySQL."""
        query = "SELECT url, data, timestamp FROM extracted_data"
        results = []

        with self.connection.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()

            for row in rows:
                base_info = {
                    'URL': row['url'],
                    'Timestamp': row['timestamp']
                }

                # Parse JSON string if needed
                extracted_data = eval(row['data']) if isinstance(row['data'], str) else row['data']
                for key, value in extracted_data.items():
                    base_info[key] = ', '.join(value) if isinstance(value, list) else value

                results.append(base_info)

        return results

    def clear(self):
        """Clear all stored data in MySQL."""
        with self.connection.cursor() as cursor:
            cursor.execute("DELETE FROM raw_html")
            cursor.execute("DELETE FROM extracted_data")
            self.connection.commit()
    
    def close_connection(self):
        """Close the database connection."""
        if self.connection.is_connected():
            self.connection.close()
            st.info("Database connection closed.")

db = Database(os.getenv("DB_URI"))