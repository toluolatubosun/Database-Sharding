from libraries.celery import celery_client


@celery_client.task(name="tasks.example_task")
def example_task(data: dict) -> dict:
    return {"status": "ok", "data": data}
