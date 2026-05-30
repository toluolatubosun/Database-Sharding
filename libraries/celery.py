from celery import Celery
from configs.config import CONFIGS

# Create Celery instance
celery_client = Celery(
    "fastapi_server_tasks",
    broker=CONFIGS["REDIS_URI"],
    backend=CONFIGS["REDIS_URI"]
)

# Configure Celery
celery_client.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    broker_connection_retry_on_startup=True,  # Added to fix deprecation warning
)

# Include tasks from tasks module
celery_client.autodiscover_tasks(["tasks"])
