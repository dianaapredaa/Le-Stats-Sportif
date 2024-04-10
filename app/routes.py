"""
routes.py is a module that defines the endpoints for the webserver.
"""
import os
import json

from flask import request, jsonify
from app import webserver
from app.task_runner import Task

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    """
    post_endpoint method is a POST endpoint that receives JSON data in the request body
    and returns a JSON response.
    """
    # Log the request
    webserver.logger.info("Received request for post_endpoint")

    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)

    # Log the response
    webserver.logger.info("Returning response for post_endpoint")

    return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    """
    get_response is a GET endpoint that returns the result of a given job_id.
    """
    # Log the request
    webserver.logger.info("Received request for job_id: {job_id}")

    # Check if job_id is convertible to an integer
    try:
        job_id = int(job_id)
    except ValueError:
        return jsonify({'status': 'error', 'message': 'Invalid job_id'}), 400

    # Check if the result file exists
    result_path = f"results/{job_id}.json"
    if (os.path.exists(result_path)
        and job_id in webserver.tasks_runner.task_done
        and webserver.tasks_runner.task_done[job_id]):
        # Return the result if it exists
        with open(result_path, "r", encoding='utf-8') as f:
            try:
                result = json.load(f)
            except json.JSONDecodeError:
                # Log the error
                webserver.logger.error("Invalid JSON in result file: {result_path}")
                return jsonify({'status': 'error', 'message': 'Invalid JSON in result file'}), 500
            # Log the successful response
            webserver.logger.info("Returning result for job_id: {job_id}")
            return jsonify({
                'status': 'done',
                'data': result
            })
    else:
        # Log the response
        webserver.logger.info("Job_id: {job_id} is still running")

        return jsonify({'status': 'running'})

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    """
    states_mean_request is a POST endpoint that receives JSON data in the request body
    and returns a job_id that can be used to check the status of the job.
    """
    # Log the request
    webserver.logger.info("Received states_mean request")

    # Get request data
    data = request.json

    # Check if server is in drain mode
    if webserver.tasks_runner.shutdown_event.is_set():
        # Log the response
        webserver.logger.info("Server is shutting down.")
        return jsonify({"status": "Server is shutting down."}), 503

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    with webserver.lock:
        job_id = webserver.job_counter
        webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'states_mean_request')

    # Return associated job_id
    webserver.tasks_runner.add_task(task)

    # Log the response
    webserver.logger.info("Job_id: {job_id} is running")

    return jsonify({"job_id": job_id})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    """
    state_mean_request is a POST endpoint that receives JSON data in the request body
    and returns a job_id that can be used to check the status of the job.
    """
    # Log the request
    webserver.logger.info("Received state_mean request")

    # Get request data
    data = request.json

    # Check if server is in drain mode
    if webserver.tasks_runner.shutdown_event.is_set():
        # Log the response
        webserver.logger.info("Server is shutting down.")
        return jsonify({"status": "Server is shutting down."}), 503

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    with webserver.lock:
        job_id = webserver.job_counter
        webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'state_mean_request')

    # Return associated job_id
    webserver.tasks_runner.add_task(task)

    # Log the response
    webserver.logger.info("Job_id: {job_id} is running")

    return jsonify({"job_id": job_id})

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    """
    best5_request is a POST endpoint that receives JSON data in the request body
    and returns a job_id that can be used to check the status of the job.
    """
    # Log the request
    webserver.logger.info("Received best5 request")

    # Get request data
    data = request.json

    # Check if server is in drain mode
    if webserver.tasks_runner.shutdown_event.is_set():
        # Log the response
        webserver.logger.info("Server is shutting down.")
        return jsonify({"status": "Server is shutting down."}), 503

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    with webserver.lock:
        job_id = webserver.job_counter
        webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'best5_request')

    # Return associated job_id
    webserver.tasks_runner.add_task(task)

    # Log the response
    webserver.logger.info("Job_id: {job_id} is running")

    return jsonify({"job_id": job_id})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    """
    worst5_request is a POST endpoint that receives JSON data in the request body
    and returns a job_id that can be used to check the status of the job.
    """
    # Log the request
    webserver.logger.info("Received worst5 request")

    # Get request data
    data = request.json

    # Check if server is in drain mode
    if webserver.tasks_runner.shutdown_event.is_set():
        # Log the response
        webserver.logger.info("Server is shutting down.")
        return jsonify({"status": "Server is shutting down."}), 503

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    with webserver.lock:
        job_id = webserver.job_counter
        webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'worst5_request')

    # Return associated job_id
    webserver.tasks_runner.add_task(task)

    # Log the response
    webserver.logger.info("Job_id: {job_id} is running")

    return jsonify({"job_id": job_id})

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    """
    global_mean_request is a POST endpoint that receives JSON data in the request body
    and returns a job_id that can be used to check the status of the job.
    """
    # Log the request
    webserver.logger.info("Received global_mean request")

    # Get request data
    data = request.json

    # Check if server is in drain mode
    if webserver.tasks_runner.shutdown_event.is_set():
        return jsonify({"status": "Server is shutting down."}), 503

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    with webserver.lock:
        job_id = webserver.job_counter
        webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'global_mean_request')

    # Return associated job_id
    webserver.tasks_runner.add_task(task)

    # Log the response
    webserver.logger.info("Job_id: {job_id} is running")

    return jsonify({"job_id": job_id})

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    """
    diff_from_mean_request is a POST endpoint that receives JSON data in the request body
    and returns a job_id that can be used to check the status of the job.
    """
    # Log the request
    webserver.logger.info("Received diff_from_mean request")

    # Get request data
    data = request.json

    # Check if server is in drain mode
    if webserver.tasks_runner.shutdown_event.is_set():
        return jsonify({"status": "Server is shutting down."}), 503

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    with webserver.lock:
        job_id = webserver.job_counter
        webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'diff_from_mean_request')

    # Return associated job_id
    webserver.tasks_runner.add_task(task)

    # Log the response
    webserver.logger.info("Job_id: {job_id} is running")

    return jsonify({"job_id": job_id})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    """
    state_diff_from_mean_request is a POST endpoint that receives JSON data in the request body
    and returns a job_id that can be used to check the status of the job.
    """
    # Log the request
    webserver.logger.info("Received state_diff_from_mean request")

    # Get request data
    data = request.json

    # Check if server is in drain mode
    if webserver.tasks_runner.shutdown_event.is_set():
        # Log the response
        webserver.logger.info("Server is shutting down.")
        return jsonify({"status": "Server is shutting down."}), 503

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    with webserver.lock:
        job_id = webserver.job_counter
        webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'state_diff_from_mean_request')

    # Return associated job_id
    webserver.tasks_runner.add_task(task)

    # Log the response
    webserver.logger.info("Job_id: {job_id} is running")

    return jsonify({"job_id": job_id})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    """
    mean_by_category_request is a POST endpoint that receives JSON data in the request body
    and returns a job_id that can be used to check the status of the job.
    """
    # Log the request
    webserver.logger.info("Received mean_by_category request")

    # Get request data
    data = request.json

    # Check if server is in drain mode
    if webserver.tasks_runner.shutdown_event.is_set():
        # Log the response
        webserver.logger.info("Server is shutting down.")
        return jsonify({"status": "Server is shutting down."}), 503

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    with webserver.lock:
        job_id = webserver.job_counter
        webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'mean_by_category_request')

    # Return associated job_id
    webserver.tasks_runner.add_task(task)

    # Log the response
    webserver.logger.info("Job_id: {job_id} is running")

    return jsonify({"job_id": job_id})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    """
    state_mean_by_category_request is a POST endpoint that receives JSON data in the request body
    and returns a job_id that can be used to check the status of the job.
    """
    # Log the request
    webserver.logger.info("Received state_mean_by_category request")

    # Get request data
    data = request.json

    # Check if server is in drain mode
    if webserver.tasks_runner.shutdown_event.is_set():
        return jsonify({"status": "Server is shutting down."}), 503

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    with webserver.lock:
        job_id = webserver.job_counter
        webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'state_mean_by_category_request')

    # Return associated job_id
    webserver.tasks_runner.add_task(task)

    # Log the response
    webserver.logger.info("Job_id: {job_id} is running")

    return jsonify({"job_id": job_id})

@webserver.route('/api/jobs', methods=['GET'])
def jobs():
    """
    jobs method is a GET endpoint that returns the status of all jobs.
    """
    # Log the request
    webserver.logger.info("Received request for all jobs")

    jobs_status = []
    with webserver.lock:
        for job_id in range(webserver.job_counter):
            if (os.path.exists(f"results/{job_id}.json")
                and job_id in webserver.tasks_runner.task_done
                and webserver.tasks_runner.task_done[job_id]):
                jobs_status.append({job_id: 'done'})
            else:
                jobs_status.append({job_id: 'running'})

    # Log the response
    webserver.logger.info("Returning status of all jobs")

    return jsonify({"status": "done", "data": jobs_status})

@webserver.route('/api/num_jobs', methods=['GET'])
def num_jobs():
    """
    num_jobs method is a GET endpoint that returns the number of jobs in the queue.
    """
    # Log the request
    webserver.logger.info("Received request for number of jobs")

    # Return the number of jobs
    jobs_counter = webserver.tasks_runner.task_queue.qsize()
    thread_pool = webserver.tasks_runner
    for thread in thread_pool.threads:
        if thread.has_task:
            jobs_counter += 1

    # Log the response
    webserver.logger.info("Returning number of jobs: {jobs_counter}")

    return jsonify({"num_jobs": jobs_counter})

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    """
    graceful_shutdown method is a GET endpoint that initiates a graceful shutdown of the server.
    """
    # Log the request
    webserver.logger.info("Received request for graceful shutdown")

    # Call function to initiate shutdown
    webserver.tasks_runner.shutdown()

    # Log the response
    webserver.logger.info("Shutting down gracefully")

    # Delete all files in the results directory
    for file in os.listdir("results"):
        os.remove(os.path.join("results", file))

    return jsonify({"status": "Shutting down gracefully"})

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    """
    index method is a GET endpoint that returns a welcome message and the defined routes.
    """
    # Log the request
    webserver.logger.info("Received request for index")

    routes = get_defined_routes()
    msg = "Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    paragraphs = ''.join(f"<p>{route}</p>" for route in routes)

    msg += paragraphs

    # Log the response
    webserver.logger.info("Returning index")

    return msg

def get_defined_routes():
    """
    get_defined_routes method returns a list of all defined routes in the webserver.
    """
    # Log the request
    webserver.logger.info("Received request for defined routes")

    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")

    # Log the response
    webserver.logger.info("Returning defined routes")

    return routes
