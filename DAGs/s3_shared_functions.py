from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from logging_config import logger

def upload_str_data_to_s3(
    data: str,
    s3_bucket: str,
    s3_target_path: str,
    file_name: str,
    ext: str,
) -> None:
    """
    Uploads a string of data to a specified AWS S3 bucket.
    
    Parameters:
    - data (str): The data to be uploaded, provided as a string. 
                  This is not a pandas DataFrame, but rather a raw string.
    - s3_bucket (str): The name of the S3 bucket where the data will be uploaded.
    - s3_target_path (str): The directory path inside the S3 bucket where the file will be saved.
                            This should end with a slash ('/').
    - file_name (str): The name of the file (without extension) to be saved in S3.
    - ext (str): The file extension (e.g., 'json', 'csv') for the file to be uploaded.

    This function prepares the S3 file path using the provided target path, file name, and extension.
    It then uploads the string data to the specified S3 location, using the AWS S3 connection 
    configured in Airflow (`s3_conn`). Upon successful upload, a log entry is generated.

    Raises:
    Exception: If an error occurs during the upload process, it logs the error and re-raises the exception.
    """

    try:        
        logger.info("Processing path")
        file_key = f"{s3_target_path}{file_name}.{ext}"
        logger.info("Uploading to s3 and connecting on method call.")
        s3_conn = S3Hook("s3_conn")
        s3_conn.load_string(
            string_data=data, key=file_key, bucket_name=s3_bucket, replace=True
        )
        logger.info("Upload successful")

    except Exception as e:
        logger.error(e)
        logger.error(f"Cause: {e.__cause__}")
        raise e
    
def read_s3_object(
        s3_bucket_name: str,
        s3_object_path: str, 
        object_file_name: str
    ):
    """
    Reads an object from an S3 bucket and returns its content as a string.

    Parameters:
    - s3_bucket_name (str): The name of the S3 bucket containing the object.
    - s3_object_path (str): The path to the object in the bucket. This should end with a slash ('/').
    - object_file_name (str): The name of the file (including its extension) to be read from the S3 bucket.

    Returns:
    - str: The content of the object as a decoded UTF-8 string.

    This function connects to the S3 bucket using the Airflow S3Hook and retrieves the specified object.
    The content of the object is read and decoded from bytes to a UTF-8 string before being returned.

    Raises:
    Exception: If there is an error retrieving or reading the object, the error is logged and re-raised.
    """

    try:
        logger.info("Reading the object...")
        s3_conn = S3Hook("s3_conn")
        logger.info("Getting object from s3 and connecting on method call.")
        return (
            s3_conn.get_key(
                bucket_name=s3_bucket_name, key=f"{s3_object_path}{object_file_name}"
            )
            .get()["Body"]
            .read()
            .decode("utf-8")
        )
    except Exception as e:
        logger.error(e)
        logger.error(f"Cause: {e.__cause__}")
        raise e
