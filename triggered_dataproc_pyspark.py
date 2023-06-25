
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
import sys

bucket_name = sys.argv[1]
file_name = sys.argv[2]

spark = SparkSession.builder.appName('demo').getOrCreate()


schema = StructType([StructField("VIN", StringType(), False),
                     StructField("County", StringType(), True),
                     StructField("City", StringType(), True),
                     StructField("State", StringType(), True),
                     StructField("Postal_Code", IntegerType(), True),
                     StructField("Model_Year", IntegerType(), True),
                     StructField("Make", StringType(), True),
                     StructField("Model", StringType(), True),
                     StructField("Electric_Vehicle_Type", StringType(), True),
                     StructField("CAFV_Eligibility", StringType(), True),
                     StructField("Electric_Range", IntegerType(), True),
                     StructField("Base_MSRP", IntegerType(), True),
                     StructField("Legislative_District", IntegerType(), True),
                     StructField("DOL_Vehicle_ID", IntegerType(), True),
                     StructField("Vehicle_Location", StringType(), True),
                     StructField("Electric_Utility", StringType(), True),
                     StructField("2020_Census_Tract", IntegerType(), True),
                     StructField("insert_time", TimestampType(), True)])


df  = spark.read.option("header" , True).schema(schema).csv(f'gs://{bucket_name}/{file_name}')


df1= df.withColumn('insert_time',current_timestamp())

df1.write.format('bigquery') \
    .option('table', 'xxxxxxx-yyyyy-384604.bigdata.ev_pop') \
    .option('temporaryGcsBucket', 'xxxxxx_bucket007') \
    .mode('overwrite') \
    .save()
