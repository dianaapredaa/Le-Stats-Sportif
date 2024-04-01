from copy import deepcopy
from queue import Queue
from threading import Thread, Event
import time
import os
import multiprocessing
import json

class ThreadPool:
    def __init__(self):
        # You must implement a ThreadPool of TaskRunners
        # Your ThreadPool should check if an environment variable TP_NUM_OF_THREADS is defined
        # If the env var is defined, that is the number of threads to be used by the thread pool
        # Otherwise, you are to use what the hardware concurrency allows
        # You are free to write your implementation as you see fit, but
        # You must NOT:
        #   * create more threads than the hardware concurrency allows
        #   * recreate threads for each task

        # Get the number of threads from environment variable if defined, else use hardware concurrency
        self.num_threads = int(os.getenv('TP_NUM_OF_THREADS', multiprocessing.cpu_count()))
        self.task_queue = Queue()
        self.threads = []
        self.condition = Event()
        # Dictionary to keep track of completed tasks
        self.task_done = {}  # job_id: Task

        # Create and start threads
        for _ in range(self.num_threads):
            thread = TaskRunner(self.task_queue, self.condition, self.task_done)
            thread.start()
            self.threads.append(thread)
        
    def add_task(self, task):
        # Add task to the queue
        print(f"Adding task {task.job_id} to queue")
        self.task_queue.put(task)
        print(f"Task {task.job_id} added to queue")

    def wait_completion(self):
        # Block until all tasks are done
        self.task_queue.join()

    def shutdown(self):
        # Signal to all threads to gracefully shutdown
        for _ in range(self.num_threads):
            self.task_queue.put(None)
        for thread in self.threads:
            thread.join()

class TaskRunner(Thread):
    def __init__(self, task_queue: Queue, event: Event, task_done: dict):
        # TODO: init necessary data structures
        super().__init__()
        self.task_queue = task_queue
        self.event = event
        self.task_done = task_done

    def run(self):
        while True:
            # TODO
            # Get pending job
            # Execute the job and save the result to disk
            # Repeat until graceful_shutdown
            task = self.task_queue.get()
            # Check if it's time to shutdown
            if self.event.is_set():
                break

            # Execute the job and save the result to disk
            try:
                task.execute()
            except Exception as e:
                print(f"Error processing task {task.job_id}: {e}")
            finally:
                self.task_done[task.job_id] = task

class Task:
    def __init__(self, job_id, data, data_ingestor, request_type):
        self.job_id = job_id
        self.data = data
        self.data_ingestor = data_ingestor
        self.request_type = request_type
        self.result = None

    def execute(self):
        self.result = self.data_ingestor.process_question(self.data, self.request_type)
        
    def save_result(self):
        # Save result to disk
        result_dir = "results"
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        result_path = f"{result_dir}/{self.job_id}.json"
        with open(result_path, 'w') as file:
            json.dump({'job_id': self.job_id, 'question': self.question, 'state': self.state, 'result': self.result}, file)
