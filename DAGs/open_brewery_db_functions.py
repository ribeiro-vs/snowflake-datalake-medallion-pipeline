from s3_shared_functions import upload_str_data_to_s3
from logging_config import logger
import requests
import time
import json

def fetch_api_data(url,endpoint):
    """
    Fetches data from a specified API endpoint.

    Parameters:
    - url (str): Base URL of the API.
    - endpoint (str): Specific API endpoint to append to the base URL.

    Returns:
    - dict: The JSON response parsed into a dictionary if the request is successful.

    Raises:
    - Exception: If the API request fails (non-200 status code), an exception is raised.
    """
    
    response = requests.get(url+endpoint)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve API data. Status code: {response.status_code}")
    return response.json()

def api_data_load_to_s3(url, endpoint, s3_bucket, s3_target_path, file_name, ext, retries=3, delay=5):
    """
    Fetches data from an API and uploads it to an S3 bucket, with retry logic for empty or failed responses.

    Parameters:
    - url (str): Base URL of the API.
    - endpoint (str): Specific API endpoint to retrieve data from.
    - s3_bucket (str): Name of the S3 bucket where the data will be uploaded.
    - s3_target_path (str): S3 target path (directory structure) where the file will be stored.
    - file_name (str): The name of the file to be uploaded.
    - ext (str): File extension (e.g., `.json`, `.csv`) for the uploaded file.
    - retries (int, optional): Number of retries allowed for fetching data from the API (default is 3).
    - delay (int, optional): Time delay (in seconds) between retry attempts (default is 5 seconds).

    Raises:
    - ValueError: If no data is retrieved after the specified number of retry attempts, an error is raised.
    """

    attempts = 0
    json_response = None
    
    while attempts < retries:
        json_response = fetch_api_data(url, endpoint)
        
        # If valid data is retrieved, break the loop
        if json_response and len(json_response) > 0:
            break
        
        # Increment the attempt counter
        attempts += 1        
        # If the maximum number of attempts is reached, raise an error
        if attempts == retries:
            raise ValueError("No data retrieved from the API after multiple attempts, aborting upload to S3.")
        # Wait before retrying
        time.sleep(delay)

    # Continue if data was successfully retrieved
    json_str_data = json.dumps(json_response)
    upload_str_data_to_s3(json_str_data, s3_bucket, s3_target_path, file_name, ext)
    
def validate_insert(**kwargs):
    """
    Validates the number of rows inserted into a target database to ensure data quality.

    Parameters:
    kwargs (dict): Dictionary of keyword arguments. It must include:
        - task_id (str): Task ID for which the row insertion needs to be validated.
        - min_rows (int, optional): Minimum number of rows expected to be inserted (default is 1).
        - ti (TaskInstance): The Airflow TaskInstance object that allows pulling of XCom values.

    Raises:
    ValueError: If the number of rows inserted is less than the expected minimum, an error is raised.
    """

    task_id = kwargs['task_id']
    min_rows = kwargs.get('min_rows', 1)

    # Fetching the context
    ti = kwargs['ti']
    rows_inserted = ti.xcom_pull(task_ids=task_id)

    print(f'this is rows_inserted!!: {str(rows_inserted)}')

    if rows_inserted is None or len(rows_inserted) == 0 or rows_inserted[0]['ROWS_INSERTED'] < min_rows:
        raise ValueError(f"Validation failed: less than {min_rows} rows inserted for task {task_id}.")
    else:
        logger.info(f"Validation passed: {rows_inserted[0]['ROWS_INSERTED']} rows inserted for task {task_id}.")