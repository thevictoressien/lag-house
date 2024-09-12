import json
from google.cloud import bigquery, storage



def load_schema(schema_path):
    if schema_path.startswith("gs://"):
        # If the path is a GCS URI, use the Google Cloud Storage client to fetch the file
        storage_client = storage.Client()

        # Extract the bucket name and the blob (file) name from the GCS URI
        bucket_name = schema_path.split("/")[2]
        blob_name = "/".join(schema_path.split("/")[3:])

        # Get the bucket and the blob
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)

        # Download the contents of the blob as a string and parse it as JSON
        schema_json = blob.download_as_text()
        schema = json.loads(schema_json)
    else:
        # If the path is a local file path, load it directly
        with open(schema_path, 'r') as schema_file:
            schema = json.load(schema_file)
    return schema


def fetch_job_config(file_format, schema, field_delimiter, create_disposition='CREATE_IF_NEEDED', write_disposition='WRITE_APPEND'):
    # write_disposition='WRITE_TRUNCATE'
    config_dict = {
    "json" : bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=schema,
            create_disposition=create_disposition,
            write_disposition = write_disposition
        ),
    
    "avro" : bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.AVRO,
            schema = schema,
            use_avro_logical_types=True,
            create_disposition=create_disposition,
            write_disposition = write_disposition
        ),
    
    "csv" : bigquery.LoadJobConfig(
       source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        field_delimiter=field_delimiter,
        quote_character="",
        schema=schema,
        autodetect=True,
        write_disposition = write_disposition
    ),

    "parquet": bigquery.LoadJobConfig(
       source_format=bigquery.SourceFormat.PARQUET,
       create_disposition = create_disposition,
       schema=schema,
       write_disposition = write_disposition
    )
    }
    return config_dict[file_format]