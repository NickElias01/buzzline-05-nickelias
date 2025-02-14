"""
file_consumer_nickelias.py

Consume json messages from a live data file. 
Insert the processed messages into a database.

Example JSON message
{
    "message": "I just shared a meme! It was amazing.",
    "author": "Charlie",
    "timestamp": "2025-01-29 14:35:20",
    "category": "humor",
    "sentiment": 0.87,
    "keyword_mentioned": "meme",
    "message_length": 42
}

Database functions are in consumers/db_sqlite_case.py.
Environment variables are in utils/utils_config module. 
"""

#####################################
# Import Modules
#####################################

# import from standard library
import json
import pathlib
import sys
import time
import csv
import sqlite3
import os

# import from local modules
import utils.utils_config as config
from utils.utils_logger import logger


#####################################
# Defining file path for CSV
# #####################################

# Define the path for the CSV file inside the "data" folder
CSV_FILE_PATH = pathlib.Path("data") / "live_data.csv"

#####################################
# Function to process a single message
# #####################################

def process_message(message: dict) -> None:
    """
    Process and transform a single JSON message.
    Converts message fields to appropriate data types.

    Args:
        message (str): The JSON message as a string.
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
# Function to write data to CSV
# #####################################

def append_to_csv(processed_message):
    """
    Append processed message to a CSV file for logging.
    """
    file_exists = os.path.exists(CSV_FILE_PATH)
    
    try:
        with open(CSV_FILE_PATH, mode="a", newline="") as file:
            writer = csv.writer(file)
            # Write header only if file doesn't exist
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
# Function to initialize the database
#####################################

def init_db(db_path: pathlib.Path):
    """
    Initialize the SQLite database with message storage and sentiment insights.
    """
    try:
        os.makedirs(os.path.dirname(db_path), exist_ok=True)

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
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
            
            # New table for sentiment insights
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

            # Ensure at least one row exists for tracking insights
            cursor.execute("SELECT COUNT(*) FROM sentiment_insights")
            if cursor.fetchone()[0] == 0:
                cursor.execute(
                    "INSERT INTO sentiment_insights (average_sentiment, total_messages, last_updated) VALUES (0.0, 0, datetime('now'))"
                )

            conn.commit()
    except Exception as e:
        logger.error(f"ERROR: Failed to initialize database: {e}")


#####################################
# Function to insert a processed message into the database
#####################################

def insert_message(message: dict, db_path: pathlib.Path) -> None:
    """
    Insert a single processed message into the database and update sentiment insights.
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

            # Update sentiment insights
            cursor.execute("SELECT COUNT(*), AVG(sentiment) FROM streamed_messages")
            total_messages, avg_sentiment = cursor.fetchone()

            # Update sentiment insights
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
# Consume Messages from Live Data File
#####################################

def consume_messages_from_file(live_data_path, sql_path, interval_secs, last_position):
    """
    Consume new messages from a file and process them.
    Each message is expected to be JSON-formatted.

    Args:
    - live_data_path (pathlib.Path): Path to the live data file.
    - sql_path (pathlib.Path): Path to the SQLite database file.
    - interval_secs (int): Interval in seconds to check for new messages.
    - last_position (int): Last read position in the file.
    """
    logger.info("Called consume_messages_from_file() with:")
    logger.info(f"   {live_data_path=}")
    logger.info(f"   {sql_path=}")
    logger.info(f"   {interval_secs=}")
    logger.info(f"   {last_position=}")

    logger.info("1. Initialize the database.")
    init_db(sql_path)

    logger.info("2. Set the last position to 0 to start at the beginning of the file.")
    last_position = 0

    while True:
        try:
            logger.info(f"3. Read from live data file at position {last_position}.")
            with open(live_data_path, "r") as file:
                # Move to the last read position
                file.seek(last_position)
                new_data_found = False  # Track if new messages were read

                for line in file:
                    if line.strip():  # If there's a non-empty line
                        new_data_found = True  # New message detected
                        message = json.loads(line.strip())

                        # Call our process_message function
                        processed_message = process_message(message)

                        # If we have a processed message, insert it into the database
                        if processed_message:
                            insert_message(processed_message, sql_path)
                            append_to_csv(processed_message)  # Append to CSV

                # Update the last position that's been read to the current file position
                last_position = file.tell()

            if not new_data_found:
                logger.info("No new messages found, sleeping...")
                time.sleep(interval_secs)  # Avoids CPU overload

        except FileNotFoundError:
            logger.error(f"ERROR: Live data file not found at {live_data_path}. Retrying in {interval_secs} seconds.")
            time.sleep(interval_secs)  # Prevents tight error loop
        except Exception as e:
            logger.error(f"ERROR: Error reading from live data file: {e}")
            time.sleep(interval_secs)  # Ensures a delay even on failure



#####################################
# Define Main Function
#####################################


def main():
    """
    Main function to run the consumer process.

    Reads configuration, initializes the database, and starts consumption.

    """
    logger.info("Starting Consumer to run continuously.")
    logger.info("Things can fail or get interrupted, so use a try block.")
    logger.info("Moved .env variables into a utils config module.")

    logger.info("STEP 1. Read environment variables using new config functions.")
    try:
        interval_secs: int = config.get_message_interval_seconds_as_int()
        live_data_path: pathlib.Path = config.get_live_data_path()
        sqlite_path: pathlib.Path = config.get_sqlite_path()
        logger.info("SUCCESS: Read environment variables.")
    except Exception as e:
        logger.error(f"ERROR: Failed to read environment variables: {e}")
        sys.exit(1)

    logger.info("STEP 2. Delete any prior database file for a fresh start.")
    if sqlite_path.exists():
        try:
            sqlite_path.unlink()
            logger.info("SUCCESS: Deleted database file.")
        except Exception as e:
            logger.error(f"ERROR: Failed to delete DB file: {e}")
            sys.exit(2)

    logger.info("STEP 3. Initialize a new database with an empty table.")
    try:
        init_db(sqlite_path)
    except Exception as e:
        logger.error(f"ERROR: Failed to create db table: {e}")
        sys.exit(3)

    logger.info("STEP 4. Begin consuming and storing messages.")
    try:
        consume_messages_from_file(live_data_path, sqlite_path, interval_secs, 0)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"ERROR: Unexpected error: {e}")
    finally:
        logger.info("TRY/FINALLY: Consumer shutting down.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()