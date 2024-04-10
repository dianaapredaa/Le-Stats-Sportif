"""
task_runner.py module contains the ThreadPool and TaskRunner classes that are used to manage
the execution of tasks in the application.
"""
from threading import Thread, Event
import multiprocessing
import os
import json
from queue import Queue

class ThreadPool:
    """
    ThreadPool class is used to manage the execution of tasks in the application.
    """
    def __init__(self):
        """
        Initialize the thread pool with the number of threads specified in the environment variable
        TP_NUM_OF_THREADS. If the environment variable is not set, the number of threads will be set
        to the number of CPUs on the system.
        """
        self.num_threads = int(os.getenv('TP_NUM_OF_THREADS', multiprocessing.cpu_count()))
        self.task_queue = Queue()
        self.shutdown_event = Event()
        self.threads = []
        self.task_done = {}

        self.shutdown_event.clear()

        # Initialize and start threads
        for _ in range(self.num_threads):
            thread = TaskRunner(self.task_queue, self.shutdown_event, self.task_done)
            thread.start()
            self.threads.append(thread)

    def add_task(self, task):
        """
        Add a task to the task queue.
        """
        if not self.shutdown_event.is_set():
            self.task_queue.put(task)

    def shutdown(self):
        """ 
        Shutdown the thread pool.
        """
        self.shutdown_event.set()
        for _ in self.threads:
            self.task_queue.put(None)
        for thread in self.threads:
            thread.join()

class TaskRunner(Thread):
    """
    TaskRunner class is used to execute tasks in a separate thread.
    """
    def __init__(self, task_queue, shutdown_event, task_done):
        """
        Initialize the TaskRunner with the task queue, shutdown event, and task_done dictionary.
        """
        super().__init__()
        self.task_queue = task_queue
        self.shutdown_event = shutdown_event
        self.task_done = task_done
        self.has_task = False

    def run(self):
        """
        Run the TaskRunner thread.
        """
        while not self.shutdown_event.is_set():
            task = self.task_queue.get()
            if task is None:  # Allow exiting the thread
                break
            try:
                self.has_task = True
                self.task_done[task.job_id] = False
                task.execute()
                task.save_result()
                self.task_done[task.job_id] = True
            except (IOError, ValueError) as e:
                print(f"Error processing task {task.job_id}: {e}")
            finally:
                self.has_task = False


class Task:
    """
    Task class is used to represent a task that needs to be executed.
    """
    def __init__(self, job_id, data, data_ingestor, request_type):
        self.job_id = job_id
        self.data = data
        self.data_ingestor = data_ingestor
        self.request_type = request_type
        self.result = None

    def execute(self):
        """
        Execute the task.
        """
        # Process the question with given data and request type
        self.result = self.data_ingestor.process_question(self.data, self.request_type)

    def save_result(self):
        """
        Save the result to a JSON file.
        """
        # Save the result to a JSON file
        result_dir = "results"
        os.makedirs(result_dir, exist_ok=True)
        result_path = os.path.join(result_dir, f"{self.job_id}.json")
        with open(result_path, 'w', encoding='utf-8') as file:
            json.dump(self.result, file)
