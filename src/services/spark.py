import subprocess
import os
import signal
import logging
from datetime import datetime, timezone

from src.jobs.user_events_batch import spark_kafka_consuming


class SparkJobService:
    def __init__(self):
        self.process: subprocess.Popen | None = None
        self.last_run: datetime | None = None
        self.status: str = "idle"

    def run(self):
        if self.process and self.process.poll() is None:
            raise RuntimeError("Spark job already running")

        spark_kafka_consuming()

        logging.info("Starting Spark job")
        self.last_run = datetime.utcnow()
        self.status = "running"

    # def run(self):
    #     if self.process and self.process.poll() is None:
    #         raise RuntimeError("Spark job already running")

    #     cmd = [
    #         "docker", "exec", "spark",
    #         "/opt/bitnami/spark/bin/spark-submit",
    #         "--master", SPARK_MASTER,
    #         "/jobs/user_events_batch.py",
    #     ]

    #     logging.info("Starting Spark job")
    #     self.process = subprocess.Popen(cmd)
    #     self.last_run = datetime.utcnow()
    #     self.status = "running"

    def cancel(self):
        if not self.process or self.process.poll() is not None:
            raise RuntimeError("No running Spark job")

        logging.info("Stopping Spark job")
        self.process.send_signal(signal.SIGTERM)
        self.status = "cancelled"
