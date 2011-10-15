import datetime

from celery import registry

from django.db import models

from settings import FINISHED_STATES
from managers import TaskLockManager

class TaskLock(models.Model):
    key = models.CharField(max_length=255, unique=True)
    task_id = models.CharField(max_length=255)
    task_name = models.CharField(max_length=255)
    timestamp = models.DateTimeField(auto_now=True)
    expiration = models.DateTimeField(null=True, blank=True)
    state = models.CharField(max_length=15, blank=True)
    
    objects = TaskLockManager()
    
    def get_async_result(self):
        return registry.tasks[self.task_name].AsyncResult(self.task_id)
    
    def ready(self):
        if self.expiration and datetime.datetime.now() > self.expiration:
            return True
        if self.task_id == '': #still being initialized
            return False 
        if self.state in FINISHED_STATES:
            return True
        result = self.get_async_result()
        if result.state in FINISHED_STATES:
            self.state = result.state
            self.save()
            return True
        return False
    
    def __unicode__(self):
        return self.key

