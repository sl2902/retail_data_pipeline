from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
import os
import uuid
from google.cloud import bigquery
from google.api_core.exceptions import AlreadyExists, Conflict
from config_data.gcp_config_parameters import *
from config_data.pubsublite_config import *
# from dotenv import load_dotenv

# load_dotenv()

location = cloud_region
inv_topic_id = inv_topic_id
txn_topic_id = txn_topic_id
inv_gcs_path = f"gs://{BUCKET_NAME}/stream_data/inventories/"
txn_gcs_path = f"gs://{BUCKET_NAME}/stream_data/transactions/"
# output = f"gs://{BUCKET_NAME}/output/inv"
checkpoint = f"gs://{BUCKET_NAME}/tmp/app"

packages = ",".join([ 
                     "com.google.cloud:pubsublite-spark-sql-streaming:1.0.0",
                     "com.google.cloud:google-cloud-pubsublite:1.9.0",
                     "com.github.ben-manes.caffeine:caffeine:2.9.0",
                     "org.scala-lang.modules:scala-java8-compat_2.12:1.0.0",
                    #  "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.9"
                     ])
spark = (
    SparkSession 
        .builder
        .appName("Retal inventory")
        # .master("local[*]")
        .enableHiveSupport()
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.jars.packages", packages)
        .config('spark.hadoop.fs.gs.impl','com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')
        # .config('google.cloud.auth.service.account.json.keyfile', cred_path)
        .getOrCreate()
)


inv_json_schema = StructType([
    StructField("inventory_id", StringType()),
    StructField("product_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("quantity_change", IntegerType()),
    StructField("store_id", StringType())
])

txn_json_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("product_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", IntegerType()),
    StructField("store_id", StringType())
])

def read_stream(schema, gcs_path):
    df = spark \
            .readStream \
            .format("json") \
            .schema(schema) \
            .load(gcs_path)
    return df

inv_df = read_stream(inv_json_schema, inv_gcs_path)
txn_df = read_stream(txn_json_schema, txn_gcs_path)

inv_df = inv_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
inv_df = inv_df.withColumn("value",
              create_map(lit("inventory_id"), col("inventory_id"),
                         lit("product_id"), col("product_id"),
                         lit("timestamp"), col("timestamp"),
                         lit("quantity_change"), col("quantity_change"),
                         lit("store_id"), col("store_id")
                        )
            )

inv_df = (
        inv_df.withColumn("key", col("inventory_id").cast(BinaryType()))
            .withColumn("data", col("value").cast(StringType()).cast(BinaryType()))
            .withColumnRenamed("timestamp", "event_timestamp")
            .withColumn(
                "attributes",
                create_map(
                    lit("inventory_id"),
                    array(col("inventory_id").cast(BinaryType())),
                    lit("product_id"),
                    array(col("product_id").cast(BinaryType())),
                    lit("event_timestamp"),
                    array(col("event_timestamp").cast(StringType()).cast(BinaryType())),
                    lit("quantity_change"),
                    array(col("quantity_change").cast(StringType()).cast(BinaryType())),
                    lit("store_id"),
                    array(col("store_id").cast(BinaryType()))
                )
            )
            .drop("value", "inventory_id", "product_id", "quantity_change", "store_id")
)

txn_df = txn_df.withColumn("timestamp", col("timestamp").cast("timestamp"))
txn_df = txn_df.withColumn("value",
              create_map(lit("transaction_id"), col("transaction_id"),
                         lit("product_id"), col("product_id"),
                         lit("timestamp"), col("timestamp"),
                         lit("quantity"), col("quantity"),
                         lit("unit_price"), col("unit_price"),
                         lit("store_id"), col("store_id")
                        )
            )

txn_df = (
        txn_df.withColumn("key", col("transaction_id").cast(BinaryType()))
            .withColumn("data", col("value").cast(StringType()).cast(BinaryType()))
            .withColumnRenamed("timestamp", "event_timestamp")
            .withColumn(
                "attributes",
                create_map(
                    lit("transaction_id"),
                    array(col("transaction_id").cast(BinaryType())),
                    lit("product_id"),
                    array(col("product_id").cast(BinaryType())),
                    lit("event_timestamp"),
                    array(col("event_timestamp").cast(StringType()).cast(BinaryType())),
                    lit("quantity"),
                    array(col("quantity").cast(StringType()).cast(BinaryType())),
                    lit("unit_price"),
                    array(col("unit_price").cast(StringType()).cast(BinaryType())),
                    lit("store_id"),
                    array(col("store_id").cast(BinaryType()))
                )
            )
            .drop("value", "transaction_id", "product_id", "quantity", "unit_price", "store_id")
)

def write_stream(df, location, topic_id):
    query = (
        df.writeStream.format("pubsublite")
        .option(
            "pubsublite.topic",
            f"projects/{project_number}/locations/{location}/topics/{topic_id}",
        )
        # Required. Use a unique checkpoint location for each job.
        .option("checkpointLocation", checkpoint + uuid.uuid4().hex)
        .outputMode("append")
        # .trigger(processingTime="1 second")
        .start()
    )
    return query

write_stream(inv_df, location, inv_topic_id)
query = write_stream(txn_df, location, txn_topic_id)

# Wait 60 seconds to terminate the query.
query.awaitTermination(120)
query.stop()