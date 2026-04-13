import json
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Optional

import boto3

from app.config import SECRET_MODE_ENV
from app.config import SECRET_MODE_SECRETS_MANAGER


@dataclass
class DbCredentials:
    username: str
    password: str
    dsn: str


@dataclass
class S3Credentials:
    bucket: str
    region: str
    access_key_id: str
    secret_access_key: str
    session_token: str
    endpoint_url: str


class CredentialResolver:
    def __init__(self, config, logger):
        self.config = config
        self.logger = logger

    def get_db_credentials(self) -> DbCredentials:
        if self.config.secret_mode == SECRET_MODE_ENV:
            return DbCredentials(
                username=self.config.db_username,
                password=self.config.db_password,
                dsn=self.config.db_dsn,
            )

        payload = self._get_json_secret(self.config.db_secret_name)
        return DbCredentials(
            username=str(payload["username"]),
            password=str(payload["password"]),
            dsn=str(payload["dsn"]),
        )

    def get_s3_credentials(self) -> S3Credentials:
        # S3 now strictly uses Environment Variables or IAM Roles.
        # Secrets Manager is bypassed for S3 credentials.
        return S3Credentials(
            bucket=self.config.s3_bucket,
            region=self.config.aws_region,
            access_key_id=self.config.aws_access_key_id,
            secret_access_key=self.config.aws_secret_access_key,
            session_token=self.config.aws_session_token,
            endpoint_url=self.config.s3_endpoint_url,
        )

    def check_secret_access(self) -> Dict[str, Any]:
        if self.config.secret_mode != SECRET_MODE_SECRETS_MANAGER:
            return {"mode": self.config.secret_mode, "message": "Secrets Manager not in use"}

        db_payload = self._get_json_secret(self.config.db_secret_name)
        return {
            "mode": self.config.secret_mode,
            "db_secret_keys": sorted(db_payload.keys()),
            "s3_status": "Using IAM/Env (Secrets Manager bypassed for S3)",
        }

    def _get_json_secret(self, secret_name: str) -> Dict[str, Any]:
        if not secret_name:
            raise ValueError("Secret name is required")

        session = self._build_boto3_session(profile_name=self.config.aws_profile)
        client = session.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response.get("SecretString")
        if not secret_string:
            raise RuntimeError("Secret %s did not return SecretString" % secret_name)
        return json.loads(secret_string)

    def _build_boto3_session(self, profile_name: Optional[str] = None):
        kwargs = {"region_name": self.config.aws_region}
        if profile_name:
            kwargs["profile_name"] = profile_name
        if self.config.aws_access_key_id and self.config.aws_secret_access_key:
            kwargs["aws_access_key_id"] = self.config.aws_access_key_id
            kwargs["aws_secret_access_key"] = self.config.aws_secret_access_key
            if self.config.aws_session_token:
                kwargs["aws_session_token"] = self.config.aws_session_token
        return boto3.session.Session(**kwargs)
