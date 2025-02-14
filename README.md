# Elias Analytics - Live Data Consumer

## Description

This **Live Data Consumer** is a Python-based application designed to consume, process, and store JSON messages from a live data source. The application reads messages from a file, processes them, and stores the processed data into both a SQLite database and a CSV file for tracking purposes. The application also tracks sentiment insights based on the processed messages, making it ideal for analyzing the emotional tone and content of incoming data streams.

This tool is useful for processing and analyzing data in real-time, with a focus on user-generated content, such as messages from social media, forums, or other messaging platforms.

## Installation

To get started with the **Live Data Consumer**, follow the steps below:

### Prerequisites

Ensure that you have the following installed:

- Python 3.7+ (Recommended: Use a virtual environment)
- SQLite (for local database storage)
- Git (for cloning the repository)

### Steps to Install

1. **Clone the repository:**

   ```bash
   git clone https://github.com/your-username/file-consumer-nickelias.git

2. **Navigate to the project directory**:

3. **Set up a virtual environment**

   ```bash
   python -m venv .venv
   source venv/bin/activate  # On Windows, use `.venv\Scripts\activate`

4. **Install the required dependencies:**
   ```bash
   pip install -r requirements.txt

5. **Set up environment variables**
   (Ensure the .env file is configured properly, or use the utils/utils_config module to define paths and other variables)

6. **Run the application. Requires 2 terminals: One runs producer_case.py, the other runs consumer_nickelias.py**


## Configuration

Set the following environment variables or update `utils/utils_config.py`:

1. **Live Data Path** (`LIVE_DATA_PATH`):  
   Path to the JSON file with messages.

2. **SQLite Database Path** (`SQLITE_DB_PATH`):  
   Path to store the SQLite database.

3. **Message Interval** (`MESSAGE_INTERVAL_SECONDS`):  
   Interval (seconds) to check for new messages.

4. **CSV File Path**:  
   Path for logging processed messages (default: `data/live_data.csv`).

Ensure all variables are properly configured before running.




## License
This project is licensed under the MIT License as an example project. 
You are encouraged to fork, copy, explore, and modify the code as you like. 
See the [LICENSE](LICENSE.txt) file for more.
