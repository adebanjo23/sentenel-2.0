"""CloudWatch logging — sends structured logs to AWS CloudWatch Logs."""

import boto3
import logging
import json
import threading
from datetime import datetime
from botocore.exceptions import ClientError
from app.config import get_settings


class CloudWatchHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.batch = []
        self.batch_lock = threading.Lock()

        settings = get_settings()
        if settings.cloudwatch_environment != "development":
            self.client = boto3.client(
                "logs",
                aws_access_key_id=settings.aws_access_key_id,
                aws_secret_access_key=settings.aws_secret_access_key,
                region_name=settings.aws_region,
            )
            self._create_log_group()
        else:
            self.client = None

    def _create_log_group(self):
        settings = get_settings()
        try:
            self.client.create_log_group(logGroupName=settings.cloudwatch_log_group)
            self.client.put_retention_policy(
                logGroupName=settings.cloudwatch_log_group,
                retentionInDays=settings.cloudwatch_retention_days,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                raise

    def emit(self, record: logging.LogRecord):
        settings = get_settings()
        if settings.cloudwatch_environment == "development" or not self.client:
            return

        log_stream = f"{settings.cloudwatch_environment}/{datetime.now().strftime('%Y/%m/%d')}"

        try:
            self.client.create_log_stream(
                logGroupName=settings.cloudwatch_log_group,
                logStreamName=log_stream,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                raise

        log_entry = {
            "timestamp": int(record.created * 1000),
            "message": self.format(record),
            "level": record.levelname,
            "logger": record.name,
            "function": record.funcName,
            "line": record.lineno,
            "environment": settings.cloudwatch_environment,
        }

        try:
            self.client.put_log_events(
                logGroupName=settings.cloudwatch_log_group,
                logStreamName=log_stream,
                logEvents=[
                    {
                        "timestamp": log_entry["timestamp"],
                        "message": json.dumps(log_entry),
                    }
                ],
            )
        except Exception as e:
            print(f"Failed to send logs to CloudWatch: {e}")


class CloudWatchLogger:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        settings = get_settings()
        self.logger = logging.getLogger("sentinel")
        self.logger.setLevel(logging.INFO)

        # Console output is handled by basicConfig on the root logger (propagate=True by default).
        # Only add CloudWatch handler in production — no extra console handlers here.
        if settings.cloudwatch_environment != "development":
            formatter = logging.Formatter(
                "%(asctime)s [%(name)s] %(levelname)s: %(message)s",
                datefmt="%H:%M:%S",
            )
            cloudwatch_handler = CloudWatchHandler()
            cloudwatch_handler.setLevel(logging.INFO)
            cloudwatch_handler.setFormatter(formatter)
            self.logger.addHandler(cloudwatch_handler)

    def get_logger(self):
        return self.logger
