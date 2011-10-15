#see https://coderanger.net/2011/01/select-for-update/
from django.db import models, connections
from django.db.models.query import QuerySet

class ForUpdateQuerySet(QuerySet):
    def for_update(self):
        if 'sqlite' in connections[self.db].settings_dict['ENGINE'].lower():
            # Noop on SQLite since it doesn't support FOR UPDATE
            return self
        sql, params = self.query.get_compiler(self.db).as_sql()
        return self.model._default_manager.raw(sql.rstrip() + ' FOR UPDATE', params)

class ForUpdateManager(models.Manager):
    def get_query_set(self):
        return ForUpdateQuerySet(self.model, using=self._db)

