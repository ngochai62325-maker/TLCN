import os
import json
import boto3
from botocore.exceptions import ClientError
from typing import List, Optional

class MinioStorage:
    DEFAULT_BUCKET = "warehouse"

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region: str = 'us-east-1',
        secure: bool = False,
        default_bucket: str = DEFAULT_BUCKET,
    ):
        self.endpoint = endpoint or os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        self.access_key = access_key or os.environ.get("MINIO_ROOT_USER", "admin")
        self.secret_key = secret_key or os.environ.get("MINIO_ROOT_PASSWORD", "password123")
        self.region = region
        self.default_bucket = default_bucket
        
        protocol = "https" if secure else "http"
        endpoint_url = f"{protocol}://{self.endpoint}"
        
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region
        )

    def _resolve_bucket(self, bucket: Optional[str]) -> str:
        if not bucket or bucket == "bronze":
            return self.default_bucket
        return bucket

    def ensure_bucket(self, bucket: str = DEFAULT_BUCKET) -> None:
        target_bucket = self._resolve_bucket(bucket)
        try:
            self.s3_client.head_bucket(Bucket=target_bucket)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.s3_client.create_bucket(Bucket=target_bucket)
            else:
                raise

    def upload_file(self, local_path: str, bucket: str = DEFAULT_BUCKET, key: str = "") -> str:
        target_bucket = self._resolve_bucket(bucket)
        self.ensure_bucket(target_bucket)
        self.s3_client.upload_file(local_path, target_bucket, key)
        return f"s3://{target_bucket}/{key}"

    def upload_bytes(self, data: bytes, bucket: str = DEFAULT_BUCKET, key: str = "") -> str:
        target_bucket = self._resolve_bucket(bucket)
        self.ensure_bucket(target_bucket)
        self.s3_client.put_object(Bucket=target_bucket, Key=key, Body=data)
        return f"s3://{target_bucket}/{key}"

    def upload_json(self, data: dict, bucket: str = DEFAULT_BUCKET, key: str = "") -> str:
        target_bucket = self._resolve_bucket(bucket)
        self.ensure_bucket(target_bucket)
        json_str = json.dumps(data)
        self.s3_client.put_object(Bucket=target_bucket, Key=key, Body=json_str.encode('utf-8'), ContentType='application/json')
        return f"s3://{target_bucket}/{key}"

    def download_file(self, bucket: str, key: str, local_path: str) -> str:
        target_bucket = self._resolve_bucket(bucket)
        self.s3_client.download_file(target_bucket, key, local_path)
        return local_path

    def file_exists(self, bucket: str, key: str) -> bool:
        target_bucket = self._resolve_bucket(bucket)
        try:
            self.s3_client.head_object(Bucket=target_bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    def get_object_metadata(self, bucket: str, key: str) -> dict:
        target_bucket = self._resolve_bucket(bucket)
        try:
            response = self.s3_client.head_object(Bucket=target_bucket, Key=key)
            return response
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return {}
            raise

    def list_objects(self, bucket: str, prefix: str) -> List[dict]:
        target_bucket = self._resolve_bucket(bucket)
        paginator = self.s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=target_bucket, Prefix=prefix)
        objects = []
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects.append(obj)
        return objects
