from datetime import date
from time import time
import os
from flask import Flask, request, jsonify
from google.cloud import storage
from google.cloud import exceptions

storage_client = storage.Client()

API_TOKEN = os.getenv('API_TOKEN')
ENV_BUCKET = os.getenv('BUCKET_NAME')
class AlreadyExistsError(Exception):
  """Raised when a resource already exists."""
class NotFoundError(Exception):
  """Raised when a resource is not found."""

def insert_bucket(bucket_name):
    """Inserts a Google Cloud Storage Bucket object.
    Args:
      bucket_name: str, the name of the Google Cloud Storage Bucket to insert.
    Returns:
      A dictionary object representing a Google Cloud Storage Bucket.
          type: google.cloud.storage.bucket.Bucket
    Raises:
      AlreadyExistsError: when trying to insert a bucket that already exists.
    """
    try:
        bucket = storage_client.create_bucket(bucket_name)
        print("Bucket {} created".format(bucket.name))
    except exceptions.Conflict as err:
        raise AlreadyExistsError(
            'the Google Cloud Storage Bucket with name {!r} already exists: '
            '{}'.format(bucket_name, err))        
    return bucket

def insert_blob(path, contents, bucket_name):
    """Inserts a new json encoded Blob in the Cloud Storage bucket provided.
    NOTE: If a Blob already exists at the provided path it will be overwritten
    by the new contents without warning.
    Args:
      path: str, the path of the Blob to create relative to the root of the
          Google Cloud Storage Bucket including the name of the Blob.
      contents: dict, a dictionary representing the contents of the new Blob.
      bucket_name: str, the name of the Google Cloud Storage Bucket to insert
          the new Blob into.
    """
    blob = storage.Blob(
        name=path,
        bucket=storage_client.get_bucket(bucket_name),
    )

    blob.upload_from_string(
        data=contents,
        content_type='application/x-ndjson',
    )

    print('Successfully uploaded blob {} to bucket {}.'.format(path, bucket_name))

def add_dtsessions(request):
    verify_token = request.args.get('API_TOKEN')
    if verify_token == API_TOKEN:

        splitoutput = request.get_data(as_text=True).splitlines()
        dtsessions = request.get_data(as_text=True)
        filename = 'dt_' + str(int(time() * 1000))
        print('Number of Dynatrace User Sessions in Payload: ', len(splitoutput))
        # daily bucket - commented out in favor of catchall bucket for POC
        # bucket_name = 'dtsessions-' + str(date.today())
        # catchall bucket based on environment variable BUCKET_NAME
        bucket_name = ENV_BUCKET

        try:
            print('creating bucket: ', bucket_name)
            insert_bucket(bucket_name)            
        except AlreadyExistsError:
            print("Bucket already exists")
        except:
            print("unexpected error")
            return jsonify({'status':'unexpected error'}), 500 
        try:
            insert_blob(filename,dtsessions,bucket_name)
            return jsonify({'status':'success'}), 200
        except:
            print("unexpected error")
            return jsonify({'status':'unexpected error'}), 500        
    else:
        return jsonify({'status':'not authorised'}), 401
