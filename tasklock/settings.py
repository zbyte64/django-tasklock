from django.conf import settings

DEFAULT_EXPIRATION = getattr(settings, 'TASKLOCK_EXPIRATION', 3600)
FINISHED_STATES = ('FAILURE', 'SUCCESS')
