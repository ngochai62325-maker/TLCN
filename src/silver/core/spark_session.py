"""Spark Session factory for Silver Layer transformations."""

from __future__ import annotations

import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "RiceLakehouseSilver") -> SparkSession:
    """Create or retrieve active SparkSession configured for Iceberg REST Catalog and MinIO."""
    minio_endpoint = os.environ.get("MINIO_ENDPOINT", "minio:9000")
    minio_access_key = os.environ.get("AWS_ACCESS_KEY_ID", "admin")
    minio_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "password123")
    iceberg_uri = os.environ.get("ICEBERG_REST_URI", "http://iceberg-rest:8181")

    # In HTTP URL ensure scheme
    if not minio_endpoint.startswith("http://") and not minio_endpoint.startswith("https://"):
        s3_endpoint = f"http://{minio_endpoint}"
    else:
        s3_endpoint = minio_endpoint

    builder = (
        SparkSession.builder.appName(app_name)
        # Iceberg Catalog configs
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.defaultCatalog", "iceberg")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", iceberg_uri)
        .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.s3.access-key-id", minio_access_key)
        .config("spark.sql.catalog.iceberg.s3.secret-access-key", minio_secret_key)
        # S3A FileSystem for raw files access
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    )

    return builder.getOrCreate()
