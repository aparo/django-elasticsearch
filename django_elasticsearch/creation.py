from djangotoolbox.db.base import NonrelDatabaseCreation
from pyes.exceptions import NotFoundException
TEST_DATABASE_PREFIX = 'test_'

class DatabaseCreation(NonrelDatabaseCreation):
    data_types = {
        'DateTimeField':                'datetime',
        'DateField':                    'date',
        'TimeField':                    'time',
        'FloatField':                   'float',
        'EmailField':                   'unicode',
        'URLField':                     'unicode',
        'BooleanField':                 'bool',
        'NullBooleanField':             'bool',
        'CharField':                    'unicode',
        'CommaSeparatedIntegerField':   'unicode',
        'IPAddressField':               'unicode',
        'SlugField':                    'unicode',
        'FileField':                    'unicode',
        'FilePathField':                'unicode',
        'TextField':                    'unicode',
        'XMLField':                     'unicode',
        'IntegerField':                 'int',
        'SmallIntegerField':            'int',
        'PositiveIntegerField':         'int',
        'PositiveSmallIntegerField':    'int',
        'BigIntegerField':              'int',
        'GenericAutoField':             'unicode',
        'StringForeignKey':             'unicode',
        'AutoField':                    'unicode',
        'RelatedAutoField':             'unicode',
        'OneToOneField':                'int',
        'DecimalField':                 'float',
    }

    def sql_indexes_for_field(self, model, f, style):
        """Not required. In ES all is index!!"""
        return []

    def index_fields_group(self, model, group, style):
        """Not required. In ES all is index!!"""
        return []

    def sql_indexes_for_model(self, model, style):
        """Not required. In ES all is index!!"""
        return []

    def sql_create_model(self, model, style, known_models=set()):
        from mapping import model_to_mapping
        mappings = model_to_mapping(model)
        self.connection.db_connection.put_mapping(model._meta.db_table, {mappings.name:mappings.as_dict()})
        return [], {}

    def set_autocommit(self):
        "Make sure a connection is in autocommit mode."
        pass

    def create_test_db(self, verbosity=1, autoclobber=False):
        # No need to create databases in mongoDB :)
        # but we can make sure that if the database existed is emptied
        from django.core.management import call_command
        if self.connection.settings_dict.get('TEST_NAME'):
            test_database_name = self.connection.settings_dict['TEST_NAME']
        elif 'NAME' in self.connection.settings_dict:
            test_database_name = TEST_DATABASE_PREFIX + self.connection.settings_dict['NAME']
        elif 'DATABASE_NAME' in self.connection.settings_dict:
            if self.connection.settings_dict['DATABASE_NAME'].startswith(TEST_DATABASE_PREFIX):
                # already been set up
                # must be because this is called from a setUp() instead of something formal.
                # suspect this Django 1.1
                test_database_name = self.connection.settings_dict['DATABASE_NAME']
            else:
                test_database_name = TEST_DATABASE_PREFIX + \
                  self.connection.settings_dict['DATABASE_NAME']
        else:
            raise ValueError("Name for test database not defined")

        self.connection.settings_dict['NAME'] = test_database_name
        # This is important. Here we change the settings so that all other code
        # things that the chosen database is now the test database. This means
        # that nothing needs to change in the test code for working with 
        # connections, databases and collections. It will appear the same as
        # when working with non-test code.

        # In this phase it will only drop the database if it already existed
        # which could potentially happen if the test database was created but 
        # was never dropped at the end of the tests
        try:
            self._drop_database(test_database_name)
        except NotFoundException:
            pass

        self.connection.db_connection.create_index(test_database_name)
        self.connection.db_connection.cluster_health(wait_for_status='green')

        call_command('syncdb', verbosity=max(verbosity - 1, 0), interactive=False, database=self.connection.alias)


    def destroy_test_db(self, old_database_name, verbosity=1):
        """
        Destroy a test database, prompting the user for confirmation if the
        database already exists. Returns the name of the test database created.
        """
        if verbosity >= 1:
            print "Destroying test database '%s'..." % self.connection.alias
        test_database_name = self.connection.settings_dict['NAME']
        self._drop_database(test_database_name)
        self.connection.settings_dict['NAME'] = old_database_name

    def _drop_database(self, database_name):
        try:
            self.connection.db_connection.delete_index(database_name)
        except NotFoundException:
            pass
        self.connection.db_connection.cluster_health(wait_for_status='green')

    def sql_destroy_model(self, model, references_to_delete, style):
        print model
