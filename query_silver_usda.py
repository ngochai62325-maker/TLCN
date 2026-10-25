from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("query-usda") \
    .master("local") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.8.1,org.apache.iceberg:iceberg-aws-bundle:1.8.1,org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin") \
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "password123") \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

import os
os.environ["AWS_REGION"] = "us-east-1"
os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password123"

df = spark.sql("SELECT country, market_year, value FROM iceberg.silver.usda_rice_psd LIMIT 10")
df.show(truncate=False)

df_q = spark.sql("SELECT rule_id, error_message, count(*) as cnt FROM iceberg.silver.data_quarantine WHERE dataset = 'usda_psd' GROUP BY rule_id, error_message")
df_q.show(truncate=False)

spark.stop()
