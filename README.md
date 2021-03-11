# dt-usersession-bigquery
This repo contains a pair of Google Cloud Functions written in python to allow importing Dynatrace User Session data into a Google BigQuery table for long-term storage. 

This example does not necessarily follow best practices around BigQuery and could encounter quota limitations in environments where User Sessions are exported more frequently than once per minute. This should simply serve as an example of how one might write Dynatrace user sessions to Google Cloud Storage and how to then import that data into a single Google BigQuery table. 

To deploy, you will need the gcloud beta cli deployed, a BigQuery table created, and Google Cloud Function IAM account will need the ability to interact with both Google Cloud Storage and BigQuery. I have not yet enumerated EXACTLY which permissions are necessary here.

The first function accepts NDJSON from Dynatrace's built-in User Session Export capability. This function will expect an `API_TOKEN` and `BUCKET_NAME` to be defined as environment variables. `API_TOKEN` will be used to allow posts. `BUCKET_NAME` will be the GCS bucket where the ndjson data ends up. The function will create the bucket if it does not exist but remember that buckets are globally unique!

To deploy:

```
cd write_dtsession_gcs
gcloud beta functions deploy add_dtsessions --entry-point add_dtsessions --runtime python38 --trigger-http --security-level=secure-optional --set-env-vars API_TOKEN=SOMETOKENNAME,BUCKET_NAME=dtsessionsexample
```

The second function uses a cloud storage trigger to run everytime a file is uploaded to the bucket specified as the trigger-resource and will append records to the table defined in the `TABLE_ID` environment variable.

To deploy:

```
cd load_dtsessions_from_gcs_to_bigquery
gcloud functions deploy load_dtsessions --runtime python38 --trigger-resource dtsessionsexample --trigger-event google.storage.object.finalize --set-env-vars TABLE_ID=strategic-technical-alliances.dtsessions.sessions
```


