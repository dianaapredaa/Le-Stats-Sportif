"""
Initializes the Flask application and imports the routes module.
"""

from threading import Lock
from flask import Flask
from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool

webserver = Flask(__name__)
webserver.tasks_runner = ThreadPool()

webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")
webserver.lock = Lock()
webserver.job_counter = 1

from app import routes
