from google.cloud import bigquery

def trigger_bqclientload(event, context):
    
    file_name = event['name']
    bucket_name = event['bucket']
    table_id = f"persuasive-pipe-384604.cloudfunction.{event['name'].split('.')[0]}"
    meta_table = 'persuasive-pipe-384604.cloudfunction.data_loading_metadata'
    
    
    
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(autodetect = True, create_disposition='CREATE_IF_NEEDED',
                                        source_format = bigquery.SourceFormat.CSV, 
                                        write_disposition='WRITE_APPEND')

    
    
    uri = f"gs://{bucket_name}/{file_name}"
    load_job = client.load_table_from_uri(uri , table_id,job_config=job_config )
    load_job.result()
     
    
    destination_table = client.get_table(table_id)
    
    meta_dict = [ {
      'Event_ID' : context.event_id,
      'Event_Type' : context.event_type,
      'Bucket_Name' : event['bucket'],
      'File_name' : event['name'],
      'Created' : event['timeCreated'],
      'Updated' : event['updated'],
      'Rows_loaded' : load_job.output_rows
    } ]

    insert_metadata = client.load_table_from_json(meta_dict,meta_table, num_retries = 2,job_config=job_config)
    insert_metadata.result()
    
    print(f"Successfully loaded {load_job.output_rows} rows.")
    
    return f'check the results in the logs'