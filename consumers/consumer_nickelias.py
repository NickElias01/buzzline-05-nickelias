"""
Elias Analytics

file_consumer_nickelias.py

This script consumes JSON-formatted messages from a live data file,
processes them, and inserts the processed data into a SQLite database.
Additionally, processed messages are logged into a CSV file for tracking.

Example JSON message:
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Database-related functions are defined in consumers/db_sqlite_case.py.
Environment variables are managed via the utils/utils_config module.
"""

#####################################
# Import Modules
#####################################

# Import standard library modules
import json
import pathlib
import sys
import time
import csv
import sqlite3
import os

# Import local utility modules
import utils.utils_config as config
from utils.utils_logger import logger


#####################################
# Define File Path for CSV Logging
#####################################

# Define the path for the CSV file inside the "data" folder.
# This will store processed messages in CSV format.
CSV_FILE_PATH = pathlib.Path("data") / "live_data.csv"


#####################################
# Function to Process a Single Message
#####################################

def process_message(message: dict) -> None:
    """
    Processes a single JSON message, extracting relevant fields and converting them to appropriate data types.

    Args:
        message (dict): A dictionary containing the message's data, including message, author, timestamp,
                        category, sentiment, and keyword mentioned.

    Returns:
        dict: Processed message with proper data types (e.g., converting sentiment to a float).
    """
    try:
        processed_message = {
            "message": message.get("message"),
            "author": message.get("author"),
            "timestamp": message.get("timestamp"),
            "category": message.get("category"),
            "sentiment": float(message.get("sentiment", 0.0)),
            "keyword_mentioned": message.get("keyword_mentioned"),
        }
        logger.info(f"Processed message: {processed_message}")
        return processed_message
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        return None


#####################################
# Function to Append Data to CSV
#####################################

def append_to_csv(processed_message):
    """
    Appends a processed message to a CSV file for logging purposes.
    If the CSV file doesn't exist, it writes a header row.

    Args:
        processed_message (dict): The message data to be written to the CSV file.
    """
    file_exists = os.path.exists(CSV_FILE_PATH)

    try:
        # Open the CSV file in append mode, create it if it doesn't exist
        with open(CSV_FILE_PATH, mode="a", newline="") as file:
            writer = csv.writer(file)
            # Write header only if the file doesn't already exist
            if not file_exists:
                writer.writerow(["message", "author", "timestamp", "category", "sentiment", "keyword_mentioned"])
            writer.writerow([
                processed_message["message"],
                processed_message["author"],
                processed_message["timestamp"],
                processed_message["category"],
                processed_message["sentiment"],
                processed_message["keyword_mentioned"],
            ])
    except Exception as e:
        logger.error(f"ERROR: Failed to write to CSV file: {e}")


#####################################
# Function to Initialize the Database
#####################################

def init_db(db_path: pathlib.Path):
    """
    Initializes the SQLite database, setting up tables for storing messages and sentiment insights.

    Args:
        db_path (pathlib.Path): Path to the SQLite database file.

    Raises:
        Exception: If there's an error in initializing the database.
    """
    try:
        # Create directory for database if it doesn't exist
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Drop existing table if it exists, and create new tables
            cursor.execute("DROP TABLE IF EXISTS streamed_messages;")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS streamed_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message TEXT,
                    author TEXT,
                    timestamp TEXT,
                    category TEXT,
                    sentiment REAL,
                    keyword_mentioned TEXT                   
                )
            """
            )
            
            # Create a new table for sentiment insights
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS sentiment_insights (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    average_sentiment REAL,
                    total_messages INTEGER,
                    last_updated TEXT
                )
            """
            )

            # Ensure at least one row exists for sentiment insights tracking
            cursor.execute("SELECT COUNT(*) FROM sentiment_insights")
            if cursor.fetchone()[0] == 0:
                cursor.execute(
                    "INSERT INTO sentiment_insights (average_sentiment, total_messages, last_updated) VALUES (0.0, 0, datetime('now'))"
                )

            conn.commit()
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize database: {e}")


#####################################
# Function to Insert Processed Message into the Database
#####################################

def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Inserts a processed message into the SQLite database and updates sentiment insights.

    Args:
        message (dict): Processed message data to insert into the database.
        db_path (pathlib.Path): Path to the SQLite database file.

    Raises:
        Exception: If there's an error in inserting the message or updating sentiment insights.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO streamed_messages (
                    message, author, timestamp, category, sentiment, keyword_mentioned
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    message["message"],
                    message["author"],
                    message["timestamp"],
                    message["category"],
                    message["sentiment"],
                    message["keyword_mentioned"],
                ),
            )

            # Update sentiment insights based on total messages and average sentiment
            cursor.execute("SELECT COUNT(*), AVG(sentiment) FROM streamed_messages")
            total_messages, avg_sentiment = cursor.fetchone()

            cursor.execute(
                """
                UPDATE sentiment_insights 
                SET average_sentiment = ?, total_messages = ?, last_updated = datetime('now')
                WHERE id = (SELECT id FROM sentiment_insights ORDER BY last_updated DESC LIMIT 1)
                """,
                (avg_sentiment, total_messages),
            )

            conn.commit()
    except Exception as e:
        logger.error(f"ERROR: Failed to insert message and update sentiment insights: {e}")


#####################################
# Function to Consume Messages from Live Data File
#####################################

def consume_messages_from_file(live_data_path, sql_path, interval_secs, last_position):
    """
    Continuously consumes new messages from a live data file, processes them, and stores them
    in both the database and CSV file.

    Args:
        live_data_path (pathlib.Path): Path to the live data file (e.g., JSON messages).
        sql_path (pathlib.Path): Path to the SQLite database file.
        interval_secs (int): Time interval in seconds to check for new messages.
        last_position (int): The last position in the file that was read.

    Raises:
        Exception: If an error occurs during file reading or message processing.
    """
    logger.info("Started consuming messages from file.")

    # Initialize the database
    init_db(sql_path)

    # Start reading from the file from the last read position
    while True:
        try:
            with open(live_data_path, "r") as file:
                file.seek(last_position)
                new_data_found = False  # Flag to track if new messages are read

                for line in file:
                    if line.strip():  # Only process non-empty lines
                        new_data_found = True
                        message = json.loads(line.strip())  # Parse the JSON message

                        # Process the message and insert it into the database and CSV
                        processed_message = process_message(message)
                        if processed_message:
                            insert_message(processed_message, sql_path)
                            append_to_csv(processed_message)

                # Update the last read position in the file
                last_position = file.tell()

            if not new_data_found:
                logger.info("No new messages found, sleeping...")
                time.sleep(interval_secs)

        except FileNotFoundError:
            logger.error(f"ERROR: Live data file not found. Retrying in {interval_secs} seconds.")
            time.sleep(interval_secs)
        except Exception as e:
            logger.error(f"ERROR: Unexpected error occurred: {e}")
            time.sleep(interval_secs)


#####################################
# Define Main Function
#####################################

def main():
    """
    The main function to run the message consumer process.

    Reads environment variables, initializes the database, and starts consuming messages from the live data file.
    """
    logger.info("Starting message consumer process.")

    try:
        # Read environment variables
        interval_secs = config.get_message_interval_seconds_as_int()
        live_data_path = config.get_live_data_path()
        sqlite_path = config.get_sqlite_path()
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    # Delete previous database file if it exists
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
        except Exception as e:
            logger.error(f"ERROR: Failed to delete old DB file: {e}")
            sys.exit(2)

    # Initialize a fresh database
    init_db(sqlite_path)

    # Start consuming and processing messages
    consume_messages_from_file(live_data_path, sqlite_path, interval_secs, 0)


#####################################
# Conditional Execution Block
#####################################

if __name__ == "__main__":
    main()