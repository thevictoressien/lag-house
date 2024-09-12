"""
Manage sone GCP resources
"""

from google.cloud import storage
from google.cloud import bigquery
from google.cloud.exceptions import Conflict, NotFound, GoogleCloudError
from typing import Optional, List
from dags.scripts.bq_utils import fetch_job_config
import logging
from dotenv import load_dotenv
import time
# from uuid

load_dotenv()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class GCSManager:
    def __init__(self, project_id: str, credentials: Optional[str] = None):
        """
        Initialize the GCSManager class.

        Args:
            project_id (str): The Google Cloud project ID.
            bucket_name (str): The name of the Google Cloud Storage bucket.
            credentials (Optional[str], optional): The path to the Google Cloud credentials file. Defaults to None.
        """
        self.project_id = project_id
        self.credentials = credentials
        self.storage_client = self._create_storage_client()


    def _create_storage_client(self) -> storage.Client:
        """
        Create a Google Cloud Storage client.

        Returns:
            storage.Client: A Google Cloud Storage client instance.
        """
        if self.credentials:
            return storage.Client.from_service_account_json(self.credentials)
        else:
            return storage.Client(project=self.project_id)
        

    def get_bucket(self, bucket_name: str) -> storage.Bucket:
        """
        Get a bucket object by name.

        Args:
            bucket_name (str): The name of the bucket.

        Returns:
            storage.Bucket: A Google Cloud Storage bucket instance.
        """
        return self.storage_client.bucket(bucket_name)


    def create_bucket(
        self, bucket_name: str, location: str, storage_class: str
    ) -> storage.Bucket:
        """
        Create a Google Cloud Storage bucket.

        Args:
        bucket_name (str): The name of the bucket to create.
        location (str): The location for the bucket.
        storage_class (str): The storage class for the bucket.

        Returns:
        storage.Bucket: A Google Cloud Storage bucket instance.
        """
        try:
            bucket = self.storage_client.get_bucket(bucket_name)
            logging.info(f"Bucket {bucket_name} already exists.")
            return bucket
        except NotFound:
            bucket = self.storage_client.bucket(bucket_name)
            bucket.storage_class = storage_class
            new_bucket = self.storage_client.create_bucket(bucket, location=location)
            logging.info(f"Bucket {bucket_name} created successfully.")
            return new_bucket
        except Exception as e:
            logging.error(f"Error creating bucket {bucket_name}: {e}")
            raise e


    def delete_bucket(self, bucket_name: str) -> None:
        """
        Delete a Google Cloud Storage bucket.
        """
        try:
            bucket = self.storage_client.get_bucket(bucket_name)
            bucket.delete(force=True)
            logging.info(f"Bucket {bucket_name} deleted successfully.")
        except NotFound:
            logging.warning(f"Bucket {bucket_name} does not exist.")
        except GoogleCloudError as e:
            logging.error(f"Error deleting bucket '{bucket_name}': {e}")



    def list_buckets(self) -> list:
        """
        List all Google Cloud Storage buckets.
        """
        buckets = [bucket.name for bucket in self.storage_client.list_buckets()]
        logging.info(f"Buckets in storage: {', '.join(buckets)}")
        return buckets


    def count_buckets(self) -> list:
        """
        Count number of buckets in Google Cloud Storage.
        """
        buckets = self.list_buckets()
        logging.info(f"Number of buckets in storage: {len(buckets)}")


    def upload_file_from_local(
        self, bucket_name: str, source_file_path: str, destination_blob_name: str
    ) -> None:
        """
        Upload a file to Google Cloud Storage from local storage.

        Args:
            bucket_name (str): The Google Cloud Storage bucket to be uploaded to.
            source_file_path (str): The path to the local file to upload.
            destination_blob_name (str): The name of the destination blob in Google Cloud Storage.
        """
        try:
            bucket = self.storage_client.get_bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_filename(source_file_path)
            logging.info(f"File {source_file_path} uploaded to {destination_blob_name} successfully.")        
            return f"gs://{bucket_name}/{destination_blob_name}"
        except NotFound:
            logging.error(f"{bucket_name} does not exist")
        except Exception as e:
            logging.error(f"Error uploading file to {destination_blob_name}: {str(e)}")
            return None

    def upload_file_from_string(
        self,
        bucket_name: str,
        buffer: str,
        destination_blob_name: str,
        content_type: str = "application/json",
    ) -> None:
        """
        Upload a file to Google Cloud Storage.
        """
        try:
            bucket = self.storage_client.get_bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)
            data_bytes = buffer.encode("utf-8")
            blob.upload_from_string(data_bytes, content_type=content_type)
            logging.info(f"File uploaded to {destination_blob_name} successfully.")
            return f"gs://{bucket_name}/{destination_blob_name}"
        except NotFound:
            logging.error(f"{bucket_name} does not exist")
        except Exception as e:
            logging.error(f"Error uploading file to {destination_blob_name}: {str(e)}")
            return None


    def download_file(
        self, bucket_name: str, source_blob_name: str, destination_file_path: str
    ) -> None:
        """
        Download a file from Google Cloud Storage.

        Args:
            bucket_name (str): The Google Cloud Storage bucket to be downloaed from
            source_blob_name (str): The name of the source blob in Google Cloud Storage.
            destination_file_path (str): The path to the local file to download to.
        """
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_path)
        logging.info(
            f"File {source_blob_name} downloaded to {destination_file_path} successfully."
        )

    def list_files(self, bucket_name: str) -> None:
        """
        List all files in the Google Cloud Storage bucket.
        """
        bucket = self.get_bucket(bucket_name)
        blobs = bucket.list_blobs()
        blob_names = [blob.name for blob in blobs]
        logging.info(f"Files in bucket: {', '.join(blob_names)}")


class BigQueryManager:
    """
    A class to manage BigQuery datasets, tables, and operations.
    """

    def __init__(self, project_id: str, location: str = "US"):
        """
        Initialize the BigQueryManager instance.

        Args:
            project_id (str): The Google Cloud project ID.
            location (str, optional): The BigQuery location. Defaults to "US".
        """
        self.client = bigquery.Client(project=project_id, location=location)

    def create_dataset(self, dataset_id: str, data_location: str = "US") -> None:
        """
        Create a new BigQuery dataset.

        Args:
            dataset_id (str): The ID of the dataset to create.
            data_location (str, optional): The location where the dataset should reside. Defaults to "US".

        Returns:
            bigquery.Dataset: The created dataset.
        """
        try:
            dataset_ref = f"{self.client.project}.{dataset_id}"
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = data_location
            dataset = self.client.create_dataset(dataset, timeout=30)
            logging.info(f"Created dataset {dataset.dataset_id}")
        except Conflict:
            logging.warning(f"Dataset {dataset_id} already exists")
        except Exception as e:
            logging.error(f"Error creating dataset {dataset_id}: {str(e)}")


    def create_table(
        self, dataset_id: str, table_id: str, schema: List[bigquery.SchemaField]
    ) -> None:
        """
        Create a new BigQuery table.

        Args:
            dataset_id (str): The ID of the dataset where the table should be created.
            table_id (str): The ID of the table to create.
            schema (List[bigquery.SchemaField]): The schema of the table.

        Returns:
            bigquery.Table: The created table.
        """
        try:
            table_ref = f"{self.client.project}.{dataset_id}.{table_id}"
            table = bigquery.Table(table_ref, schema=schema)
            table = self.client.create_table(table, timeout=30)
            logging.info(f"Created table {table.table_id}")
        except Conflict:
            logging.warning(f"Table {table_id} already exists")
        except Exception as e:
            logging.error(f"Error creating table {table_id}: {str(e)}")


    def load_from_gcs(
        self,
        dataset_id: str,
        table_id: str,
        source_uris: List[str],
        source_format: str = "CSV",
        schema: List[bigquery.SchemaField] = None,
        field_delimiter: str = ",",
        create_disposition: str = "CREATE_IF_NEEDED",
        write_disposition: str = "WRITE_APPEND"
    ) -> None:
        """
        Load data from Google Cloud Storage into a BigQuery table.

        Args:
            dataset_id (str): The ID of the dataset where the table resides.
            table_id (str): The ID of the table to load data into.
            source_uris (List[str]): The URIs of the data files in Google Cloud Storage.
            source_format (str, optional): The format of the data files. Defaults to "CSV".
            schema (List[bigquery.SchemaField], optional): The schema of the table. Defaults to None.
            field_delimiter (str, optional): The field delimiter for CSV files. Defaults to ','.
            create_disposition (str, optional): The create disposition for the load job. Defaults to "CREATE_IF_NEEDED".
            write_disposition (str, optional): The write disposition for the load job. Defaults to "WRITE_TRUNCATE".
        """
        # job_id =
        table_ref = self.client.dataset(dataset_id).table(table_id)
        job_config = fetch_job_config(
            source_format,
            schema,
            field_delimiter,
            create_disposition,
            write_disposition,
        )

        load_job = self.client.load_table_from_uri(
            source_uris, table_ref, job_config=job_config
        )
        load_job.result()
        logging.info(f"Loaded data into table {table_ref.table_id}")


    def load_from_local(
        self,
        dataset_id: str,
        table_id: str,
        source_file: str,
        file_format: str,
        schema: dict,
        field_delimiter: str,
        create_disposition: str = "CREATE_IF_NEEDED",
        write_disposition: str = "WRITE_APPEND",
    ) -> None:
        """
        Load data from a local file into a BigQuery table.

        Args:
            dataset_id (str): The ID of the dataset where the table resides.
            table_id (str): The ID of the table to load data into.
            source_file (str): The path to the local data file.
            file_format (str): The format of the data file (e.g., "json", "avro", "csv", "parquet").
            schema (dict): The schema of the table.
            field_delimiter (str, optional): The field delimiter for CSV files. Defaults to ','.
            create_disposition (str, optional): The create disposition for the load job. Defaults to "CREATE_IF_NEEDED".
            write_disposition (str, optional): The write disposition for the load job. Defaults to "WRITE_APPEND".
        """
        table_ref = self.client.dataset(dataset_id).table(table_id)
        job_config = fetch_job_config(
            file_format, schema, field_delimiter, create_disposition, write_disposition
        )

        with open(source_file, "rb") as source_file_obj:
            load_job = self.client.load_table_from_file(
                source_file_obj, table_ref, job_config=job_config
            )
            load_job.result()
            logging.info(f"Loaded data into table {table_ref.table_id}")