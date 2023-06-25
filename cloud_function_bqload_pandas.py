import pandas as pd
from pandas.io import gbq
from google.cloud import bigquery


def bqload_pandas(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file_name = event['name']
    bucket_name = event['bucket']
    table_name = file_name.split('.')[0]

    metadata = []
    metadata_dict = {
      'Event_ID' : context.event_id,
      'Event_Type' : context.event_type,
      'Bucket_Name' : event['bucket'],
      'File_name' : event['name'],
      'Created' : event['timeCreated'],
      'Updated' : event['updated']
                    }

    metadata.append(metadata_dict)

    df_metadata = pd.DataFrame.from_records(metadata)
    df_metadata.to_gbq('cloudfunction.data_loading_metadata',
                           project_id = 'xxxxxxx-yyyyy-384604',
                           if_exists = 'append',
                           location = 'us')

    df_data = pd.read_csv('gs://' + event['bucket'] + '/' + event['name'])

    df_data.to_gbq('cloudfunction.' + table_name,
                           project_id = 'xxxxxxx-yyyyy-384604',
                           if_exists = 'append',
                           location = 'us')

