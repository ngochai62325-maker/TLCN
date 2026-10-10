from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("profile-usda-local") \
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

df = spark.sql("SELECT commodity, attribute, country, col_2020_2021, unit_description FROM iceberg.bronze.usda_psd LIMIT 5")
df.show(truncate=False)
spark.stop()
