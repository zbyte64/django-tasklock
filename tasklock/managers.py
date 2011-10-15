from celery import registry
import datetime

from django.db.models import Q, Manager

from settings import DEFAULT_EXPIRATION, FINISHED_STATES
from utils import ForUpdateQuerySet

class TaskLockManager(Manager):
    def _select_for_update(self):
        return ForUpdateQuerySet(self.model, self._db)
    
    def schedule_task(self, key, task_name, *args, **kwargs):
        expiration = DEFAULT_EXPIRATION
        if isinstance(expiration, (int, long)):
            expiration = datetime.timedelta(seconds=expiration)
        if isinstance(expiration, datetime.timedelta):
            expiration = datetime.datetime.now() + expiration
        return self.schedule_task_with_expiration(key, task_name, expiration, *args, **kwargs)
    
    def schedule_task_with_expiration(self, key, task_name, expiration, *args, **kwargs): #please ensure you do this in a transaction
        task = registry.tasks[task_name]
        try:
            if hasattr(self, 'select_for_update'): #django 1.4
                lock = self.select_for_update().get(key=key)
            else:
                lock = self._select_for_update().filter(key=key).for_update()[0] #ensure we get lock on this
        except (self.model.DoesNotExist, IndexError):
            lock = self.create(key=key, expiration=expiration)
        else:
            if not lock.ready():
                return lock
        result = task.delay(*args, **kwargs)
        lock.task_id = result.task_id
        lock.task_name = task_name
        lock.expiration = expiration
        lock.save()
        return lock
    
    def ready(self, key):
        try:
            lock = self.get(key=key)
        except self.model.DoesNotExist:
            return True
        else:
            return lock.ready()
    
    def get_finished_locks(self):
        return self.filter(Q(expiration__gte=datetime.datetime.now)|Q(state__in=FINISHED_STATES))
    
    def cleanup_finished_locks(self):
        return self.get_finished_locks().delete()
