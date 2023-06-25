from google.cloud import bigquery
import os
from google.cloud import dataproc_v1
try:
 def trigger_dataproc(event, context):
    
    file_name = event['name']
    bucket_name = event['bucket']
    
    region = 'us-central1'
    main_python_file_uri = os.environ.get("main_python_file_uri")
    project_id = 'persuasive-pipe-384604'
    cluster_name = 'spark-demo'
# Create the dataproc job client.
    job_client = dataproc_v1.JobControllerClient(
    client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
                )
    
    
    
# Create the dataproc job config.
    job = {
    "placement": {"cluster_name": cluster_name},
    "pyspark_job": {"main_python_file_uri": main_python_file_uri,
    "args": [bucket_name , file_name]       
     }}

    operation = job_client.submit_job_as_operation(
    request={"project_id": project_id, "region": region, "job": job}
            )
    response = operation.result()
    
#write information about the event to be loaded in Bigquery    
    meta_dict = [ {
      'Event_ID' : context.event_id,
      'Event_Type' : context.event_type,
      'Bucket_Name' : event['bucket'],
      'File_name' : event['name'],
      'Created' : event['timeCreated'],
      'Updated' : event['updated']
      
    } ]
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(autodetect = True, create_disposition='CREATE_IF_NEEDED',
                                        write_disposition='WRITE_APPEND')
    meta_table = 'persuasive-pipe-384604.cloudfunction.data_loading_metadata'
    insert_metadata = client.load_table_from_json(meta_dict,meta_table, num_retries = 2,job_config=job_config)
    insert_metadata.result()
    
    print("Successfully triggered.")

except Exception as e:
    print(e)
    
    