from django.core.exceptions import ImproperlyConfigured

from .creation import DatabaseCreation
from .operations import DatabaseOperations
from .serializer import Decoder, Encoder
from pyes import ES

from djangotoolbox.db.base import NonrelDatabaseFeatures, \
    NonrelDatabaseWrapper, NonrelDatabaseClient, \
    NonrelDatabaseValidation, NonrelDatabaseIntrospection

class DatabaseFeatures(NonrelDatabaseFeatures):
    string_based_auto_field = True

class DatabaseClient(NonrelDatabaseClient):
    pass

class DatabaseValidation(NonrelDatabaseValidation):
    pass

class DatabaseIntrospection(NonrelDatabaseIntrospection):
    def table_names(self):
        """
        Show defined models
        """
        # TODO: get indexes
        return []

    def sequence_list(self):
        # TODO: check if it's necessary to implement that
        pass

class DatabaseWrapper(NonrelDatabaseWrapper):
    def _cursor(self):
        self._ensure_is_connected()
        return self._connection

    def __init__(self, *args, **kwds):
        super(DatabaseWrapper, self).__init__(*args, **kwds)
        self.features = DatabaseFeatures(self)
        self.ops = DatabaseOperations(self)
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.validation = DatabaseValidation(self)
        self.introspection = DatabaseIntrospection(self)
        self._is_connected = False

    @property
    def db_connection(self):
        self._ensure_is_connected()
        return self._db_connection

    def _ensure_is_connected(self):
        if not self._is_connected:
            try:
                port = int(self.settings_dict['PORT'])
            except ValueError:
                raise ImproperlyConfigured("PORT must be an integer")

            self.db_name = self.settings_dict['NAME']

            self._connection = ES("%s:%s" % (self.settings_dict['HOST'], port),
                                  decoder=Decoder,
                                  encoder=Encoder,
                                  autorefresh=True,
                                  default_indexes=[self.db_name])

            self._db_connection = self._connection
            #auto index creation: check if to remove
            try:
                self._connection.create_index(self.db_name)
            except:
                pass
            # We're done!
            self._is_connected = True
