import os
import sys

# Add the project root directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from libraries.celery import celery_client
# Import tasks to ensure they are registered
import tasks.dummy_tasks

# This makes the Celery app available when this file is imported
app = celery_client