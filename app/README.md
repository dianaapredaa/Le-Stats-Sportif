### Le Stats Sportif

    Student: Diana Preda  
    Group: 334CA

### Description
    This is a simple web application that allows users to upload a CSV file containing data, analyze the data, and view the results of the analysis. The application also provides a RESTful API for users to submit jobs, check the status of jobs, and get the results of jobs. The application uses a background task runner to execute tasks asynchronously and a job queue to manage the tasks that need to be executed.

### Implementation
    The application is implemented using the Flask framework in Python. The application consists of several modules, each responsible for a specific aspect of the application. The modules are as follows:

***data_ingestor.py***

    This file contains the DataIngestor class, which is responsible for reading a CSV file containing data, processing this data into a useful format for analysis, identifying metrics where lower or higher values are preferable, and implementing methods to analyze the data based on various criteria.

***routes.py***

    This file defines the web application's routes, which form the primary interface for interacting with your web service. Every route is linked to a distinct endpoint, catering to specific tasks including: conducting data analyses, managing job submissions and their outcomes through API Endpoints; overseeing job statuses, enumerating all tasks, and handling job queues; and facilitating a Graceful Shutdown process to carefully close down the server after ensuring the completion of all active tasks.

***task_runner.py***

    This file contains the TaskRunner class, which is responsible for managing the execution of tasks in the background. The TaskRunner class uses a queue to manage the tasks that need to be executed and a thread pool to execute the tasks concurrently. The TaskRunner class also provides methods to submit tasks to the queue, get the status of a task, and get the results of a task.

    The Task class represents a task that needs to be executed. Each task has a unique ID, a status (pending, running, or completed), and a result. The Task class also has a run method that executes the task and sets the result.

### Improvements
* Add more routes to the web application to support additional functionality.
* Add more error handling to the web application to improve robustness.
* Add more unit tests to the application to ensure the correctness of the code.

### Conclusion
    The Flask application demonstrates an efficient approach to asynchronous data analysis, leveraging a modular design and a ThreadPool for background processing. It offers a RESTful API for user interaction and ensures reliability through a graceful shutdown mechanism, making it an effective solution for web-based data analysis.
