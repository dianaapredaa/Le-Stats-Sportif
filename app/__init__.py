"""
Initializes the Flask application and imports the routes module.
"""

import logging
import logging.handlers
import time
from threading import Lock
from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool

# Initialize the Flask application
webserver = Flask(__name__)
# Set the webserver configuration
webserver.tasks_runner = ThreadPool()
# Set the data ingestor
webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")
# Set the lock
webserver.lock = Lock()
# Set the job counter
webserver.job_counter = 1

# Logger configuration
logging.basicConfig(level=logging.INFO)
# Create a logger object
logger = logging.getLogger(__name__)
# Set logger level to INFO
logger.setLevel(logging.INFO)
# Create a file handler
handler = logging.handlers.RotatingFileHandler("webserver.log", maxBytes=1000000, backupCount=5)
# Create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Set the formatter
handler.setFormatter(formatter)
# Add the handler to the logger
logger.addHandler(handler)
# Tie logger to the webserver
webserver.logger = logger
# Set logger time to UTC/GMT
logging.Formatter.converter = time.gmtime

from app import routes
