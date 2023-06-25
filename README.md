# Event-triggered-Cloud-Function
This repository is about how to set up Cloud function responding to storage event happening in GCS bucket to load data into Bigquery.Here your source of data is CSV file uploaded in GCS bucket. Any event happening under the GCS bucket is monitored using Cloud Function, in response to which desired action can be taken. 

# Cloud Function

Google Cloud Functions is a serverless execution environment for building and connecting cloud services. With Cloud Functions you write simple, single-purpose functions that are attached to events emitted from your cloud infrastructure and services. Your function is triggered when an event being watched is fired.

# Google Cloud Storage

Google Cloud Storage is an enterprise public cloud storage platform that can house large unstructured data sets.

# Cloud Dataproc

Dataproc is a managed Spark and Hadoop service that lets you take advantage of open source data tools for batch processing, querying, streaming, and machine learning. Dataproc automation helps you create clusters quickly, manage them easily, and save money by turning clusters off when you don't need them.

Three different method are handled to load the file to Bigquery. Following are the methods handled.

1. Bigquery Python client
2. Dataproc Job
3. Pandas for Bigquery

# 1. Bigquery Python client

In this method we use  bigquery python client in Cloud Function to load the file from GCS and write to Bigquery table. This method is suitable for low and medium volume ETL jobs.

# 2. Dataproc Job

Here when a file is uploaded to GCS, Cloud Function submit dataproc job to the dataproc cluster. Dataproc job is pyspark script which reads the file from GCS location and writes to Bigquery. In this approach, we can handle large volume of data, leveraging the power of Dataproc clusters as it can be scaled to multi-node cluster. Also transformation can be handled in pyspark script.

# 3. Pandas for Bigquery

In this approach, we load data to Bigquery using pandas dataframe. This method has limitation that only light weight ETL job can only be handled as pandas is a single node processing framework.


![architecture_diagram](https://github.com/karthikeyanragu/Event-triggered-Cloud-Function/assets/104515955/2abb948b-d53f-4fa2-9a22-699314cf6c3b)


