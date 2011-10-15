from django.test import TestCase
from models import TaskLock
import tasks

class TaskLockTest(TestCase):
    def testPing(self):
        task_lock = TaskLock.objects.schedule_task('thekey', 'celery.ping')
        self.assertEqual(TaskLock.objects.all().count(), 1)
        task_lock.ready()
    
    def testCleanup(self):
        #TODO load in data for it to cleanup
        tasks.cleanup_finished_task_locks()
