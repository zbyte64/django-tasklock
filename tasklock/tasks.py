from celery.task import task

from models import TaskLock

@task(ignore_result=True)
def cleanup_finished_task_locks():
    TaskLock.objects.cleanup_finished_locks()

