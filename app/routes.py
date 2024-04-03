from app import webserver
from flask import request, jsonify
from app.task_runner import Task

import os
import json

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])
def post_endpoint():
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)
    else:
        # Method Not Allowed
        return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    job_id = int(job_id)
    # TODO
    # Check if job_id is valid
    if not job_id or int(job_id) < 0 or int(job_id) >= webserver.job_counter:
        return jsonify({'status': 'error', 'message': 'Invalid job_id'})
    
    # Check if job_id is done and return the result
    if is_done(job_id):
        res = res_for(job_id)
        return jsonify({
            'status': 'done',
            'data': res
        })

    # If not, return running status
    return jsonify({'status': 'running'})

def is_done(job_id):
    # TODO
    if webserver.tasks_runner.task_done.get(job_id):
        return True
    return False

def res_for(job_id):
    # TODO
    # Get result for job_id
    if webserver.tasks_runner.task_done.get(job_id):
        task = webserver.tasks_runner.task_done[job_id]
        return task.result
    return None

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    # TODO
    # Get request data
    data = request.json

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = webserver.job_counter
    webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'states_mean_request')
        
    # Return associated job_id
    webserver.tasks_runner.add_task(task)
    return jsonify({"job_id": job_id})

@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    # TODO
    # Get request data
    data = request.json

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = webserver.job_counter
    webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'state_mean_request')
        
    # Return associated job_id
    webserver.tasks_runner.add_task(task)
    return jsonify({"job_id": job_id})

@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    # TODO
    # Get request data
    data = request.json

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = webserver.job_counter
    webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'best5_request')
        
    # Return associated job_id
    webserver.tasks_runner.add_task(task)
    return jsonify({"job_id": job_id})

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    # TODO
    # Get request data
    data = request.json

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = webserver.job_counter
    webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'worst5_request')
        
    # Return associated job_id
    webserver.tasks_runner.add_task(task)
    return jsonify({"job_id": job_id})

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    # TODO
    # Get request data
    data = request.json

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = webserver.job_counter
    webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'global_mean_request')
        
    # Return associated job_id
    webserver.tasks_runner.add_task(task)
    return jsonify({"job_id": job_id})

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    # TODO
    # Get request data
    data = request.json

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = webserver.job_counter
    webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'diff_from_mean_request')
        
    # Return associated job_id
    webserver.tasks_runner.add_task(task)
    return jsonify({"job_id": job_id})

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    # TODO
    # Get request data
    data = request.json

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = webserver.job_counter
    webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'state_diff_from_mean_request')
        
    # Return associated job_id
    webserver.tasks_runner.add_task(task)
    return jsonify({"job_id": job_id})

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    # TODO
    # Get request data
    data = request.json

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = webserver.job_counter
    webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'mean_by_category_request')
        
    # Return associated job_id
    webserver.tasks_runner.add_task(task)
    return jsonify({"job_id": job_id})

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    # TODO
    # Get request data
    data = request.json

    # Register job. Don't wait for task to finish
    # Increment job_id counter
    job_id = webserver.job_counter
    webserver.job_counter += 1

    task = Task(job_id, data, webserver.data_ingestor, 'state_mean_by_category_request')
        
    # Return associated job_id
    webserver.tasks_runner.add_task(task)
    return jsonify({"job_id": job_id})

# You can check localhost in your browser to see what this displays
@webserver.route('/')
@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def graceful_shutdown():
    # Call function to initiate shutdown
    webserver.tasks_runner.shutdown()
    return jsonify({"message": "Shutting down gracefully"})
