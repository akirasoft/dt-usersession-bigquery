from google.cloud import bigquery
from pathlib import Path
import json
import os
from datetime import date, timedelta


bigquery_client = bigquery.Client()

schema_file = 'dtsession.bq.schema.json'

TABLE_ID = os.getenv('TABLE_ID')

def _get_field_schema(field):
    name = field['name']
    field_type = field.get('type', 'STRING')
    mode = field.get('mode', 'NULLABLE')
    fields = field.get('fields', [])
    if fields:
        subschema = []
        for f in fields:
            fields_res = _get_field_schema(f)
            subschema.append(fields_res)
    else:
        subschema = []
    field_schema = bigquery.SchemaField(name=name, 
        field_type=field_type,
        mode=mode,
        fields=subschema
    )
    return field_schema


def parse_bq_json_schema(schema_filename):
    schema = []
    with open(schema_filename, 'r') as infile:
        jsonschema = json.load(infile)
    for field in jsonschema:
        schema.append(_get_field_schema(field))
    return schema

res_schema = parse_bq_json_schema(schema_file)

def load_dtsessions(event, context):
    """Cloud Function triggered by Cloud Storage.
       This function loads data into BigQuery when a file is uploaded.

    Args:
        event (dict):  The dictionary with data specific to this type of event.
                       The `data` field contains a description of the event in
                       the Cloud Storage `object` format described here:
                       https://cloud.google.com/storage/docs/json_api/v1/objects#resource
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """    
    job_config = bigquery.LoadJobConfig(
        schema = res_schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values = True,
    )
    # uri = 'gs://' + bucket_name + '/*'
    # pulls bucket and filename from event data
    uri = 'gs://' + str(event['bucket']) + '/' + str(event['name'])
    load_job = bigquery_client.load_table_from_uri(
        uri, TABLE_ID, job_config=job_config
    )  # Make an API request.
    load_job.result()  # Waits for the job to complete.
    destination_table = bigquery_client.get_table(TABLE_ID)
    print("Loaded {} rows.".format(destination_table.num_rows))