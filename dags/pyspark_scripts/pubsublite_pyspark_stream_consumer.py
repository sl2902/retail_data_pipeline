from pyspark.sql import SparkSession
from pyspark.sql.functions import *
# from pyspark.sql.streaming import GroupStateTimeout
from pyspark.sql.types import *
import json
import os
from google.cloud import bigquery
from google.api_core.exceptions import AlreadyExists, Conflict
from config_data.gcp_config_parameters import *
from config_data.pubsublite_config import *
# from dotenv import load_dotenv

# load_dotenv()


location = cloud_region
txn_subscription_id = f"{txn_topic_id}-topic"
inv_subscription_id = f"{inv_topic_id}-topic"

txn_temp_bucket = f"{BUCKET_NAME}/bq_txn"
inv_temp_bucket = f"{BUCKET_NAME}/bq_inv"
bq_dataset = DATASET
txn_table_name = "transactions"
inv_table_name = "inventories"
packages = ",".join([ 
                     "com.google.cloud.spark:spark-3.3-bigquery:0.36.1",
                    #  "com.google.cloud:pubsublite-spark-sql-streaming:1.0.0",
                     "com.google.cloud:google-cloud-pubsublite:1.9.0",
                     "com.github.ben-manes.caffeine:caffeine:2.9.0",
                     "org.scala-lang.modules:scala-java8-compat_2.12:1.0.0",
                  #  "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.9"
                     ])
jars = "gs://spark-lib/bigquery/spark-3.3-bigquery-0.36.1.jar"
spark = (
            SparkSession 
                .builder
                .appName("Retail")
                # .master("local[*]")
                .enableHiveSupport()
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.streaming.stopGracefullyOnShutdown", "true")
                .config("spark.jars.packages", packages)
                .getOrCreate()
)

# Setup hadoop fs configuration for schema gs://
conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
# conf.set("google.cloud.auth.service.account.json.keyfile", "/Users/home/Documents/secrets/personal-gcp.json")

def read_stream(location, subscription_id):
    df = (
        spark.readStream.format("pubsublite")
        .option(
            "pubsublite.subscription",
            f"projects/{project_number}/locations/{location}/subscriptions/{subscription_id}",
        )
        .load()
    )
    return df

txn_df = read_stream(location, txn_subscription_id)
inv_df = read_stream(location, inv_subscription_id)


txn_df = (
    txn_df.select("attributes")
            .withColumn("transaction_id", col("attributes.transaction_id").getItem(0).cast(StringType()))
            .withColumn("product_id", col("attributes.product_id").getItem(0).cast(StringType()))
            .withColumn("timestamp", col("attributes.event_timestamp").getItem(0).cast(StringType()).cast(TimestampType()))
            .withColumn("quantity", col("attributes.quantity").getItem(0).cast(StringType()).cast(IntegerType()))
            .withColumn("unit_price", col("attributes.unit_price").getItem(0).cast(StringType()).cast(IntegerType()))
            .withColumn("store_id", col("attributes.store_id").getItem(0).cast(StringType()))
            .select("transaction_id", "product_id", "timestamp", "quantity", "unit_price", "store_id")
            .filter(col("transaction_id").isNotNull())
)

txn_df = txn_df.withWatermark("timestamp", "1 seconds").dropDuplicates(["transaction_id", "timestamp"])

inv_df = (
    inv_df.select("attributes")
            .withColumn("inventory_id", col("attributes.inventory_id").getItem(0).cast(StringType()))
            .withColumn("product_id", col("attributes.product_id").getItem(0).cast(StringType()))
            .withColumn("timestamp", col("attributes.event_timestamp").getItem(0).cast(StringType()).cast(TimestampType()))
            .withColumn("quantity_change", col("attributes.quantity_change").getItem(0).cast(StringType()).cast(IntegerType()))
            .withColumn("store_id", col("attributes.store_id").getItem(0).cast(StringType()))
            .select("inventory_id", "product_id", "timestamp", "quantity_change", "store_id")
            .filter(col("inventory_id").isNotNull())
)

inv_df = inv_df.withWatermark("timestamp", "1 seconds").dropDuplicates(["inventory_id", "timestamp"])


def write_bigquery(df, batch_id, temp_bucket, table_name):
    df.write.format("bigquery")\
            .option("temporaryGcsBucket", temp_bucket)\
            .option("table", f"{bq_dataset}.{table_name}")\
            .mode("append")\
            .save()
    
def append_stream_bq(df, func):
    query = df.writeStream\
            .outputMode("append")\
            .foreachBatch(func)\
            .start()
    return query

write_inv_bq_with_params = lambda df, batch_id: write_bigquery(df, batch_id, inv_temp_bucket, inv_table_name)
append_stream_bq(inv_df, write_inv_bq_with_params)

write_txn_bq_with_params = lambda df, batch_id: write_bigquery(df, batch_id, txn_temp_bucket, txn_table_name)
query = append_stream_bq(txn_df, write_txn_bq_with_params)

query.awaitTermination(500)
# # Wait 120 seconds (must be >= 60 seconds) to start receiving messages.
# query.awaitTermination()
query.stop()



