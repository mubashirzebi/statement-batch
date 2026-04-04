import uuid

import boto3
from botocore.exceptions import ClientError


class S3Uploader:
    def __init__(self, credentials):
        session_kwargs = {"region_name": credentials.region}
        if credentials.access_key_id and credentials.secret_access_key:
            session_kwargs["aws_access_key_id"] = credentials.access_key_id
            session_kwargs["aws_secret_access_key"] = credentials.secret_access_key
            if credentials.session_token:
                session_kwargs["aws_session_token"] = credentials.session_token

        session = boto3.session.Session(**session_kwargs)
        client_kwargs = {}
        if credentials.endpoint_url:
            client_kwargs["endpoint_url"] = credentials.endpoint_url

        self.bucket = credentials.bucket
        self.client = session.client("s3", **client_kwargs)

    def upload_file(self, local_path, object_key):
        self.client.upload_file(str(local_path), self.bucket, object_key)

    def check_access(self):
        self.client.head_bucket(Bucket=self.bucket)
        probe_key = "batch-job-healthcheck/%s.txt" % uuid.uuid4().hex
        self.client.put_object(Bucket=self.bucket, Key=probe_key, Body=b"healthcheck")
        response = self.client.get_object(Bucket=self.bucket, Key=probe_key)
        body_sample = response["Body"].read(16)

        delete_allowed = True
        cleanup_warning = ""
        try:
            self.client.delete_object(Bucket=self.bucket, Key=probe_key)
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code in ("AccessDenied", "UnauthorizedOperation"):
                delete_allowed = False
                cleanup_warning = str(exc)
            else:
                raise

        return {
            "bucket": self.bucket,
            "probe_key": probe_key,
            "read_sample": body_sample.decode("utf-8", errors="replace"),
            "delete_allowed": delete_allowed,
            "cleanup_warning": cleanup_warning,
        }
