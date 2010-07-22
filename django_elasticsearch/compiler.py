import sys
import re

from datetime import datetime
from functools import wraps

from django.conf import settings
from django.db import models
from django.db.models.sql import aggregates as sqlaggregates
from django.db.models.sql.compiler import SQLCompiler
from django.db.models.sql import aggregates as sqlaggregates
from django.db.models.sql.constants import LOOKUP_SEP, MULTI, SINGLE
from django.db.models.sql.where import AND, OR
from django.db.utils import DatabaseError, IntegrityError
from django.db.models.sql.where import WhereNode
from django.db.models.fields import NOT_PROVIDED, AutoField
from django.utils.tree import Node
from pyes import MatchAllQuery, FilteredQuery, BoolQuery, StringQuery, ObjectId, WildcardQuery, RegexTermQuery, RangeQuery, ESRange
from djangotoolbox.db.basecompiler import NonrelQuery, NonrelCompiler, \
    NonrelInsertCompiler, NonrelUpdateCompiler, NonrelDeleteCompiler
from brainaetic.es.query import WildcardQuery

TYPE_MAPPING_FROM_DB = {
    'unicode':  lambda val: unicode(val),
    'int':      lambda val: int(val),
    'float':    lambda val: float(val),
    'bool':     lambda val: bool(val),
    'objectid': lambda val: unicode(val),
}

TYPE_MAPPING_TO_DB = {
    'unicode':  lambda val: unicode(val),
    'int':      lambda val: int(val),
    'float':    lambda val: float(val),
    'bool':     lambda val: bool(val),
    'date':     lambda val: datetime(val.year, val.month, val.day),
    'time':     lambda val: datetime(2000, 1, 1, val.hour, val.minute,
                                     val.second, val.microsecond),
}

OPERATORS_MAP = {
    'exact':    lambda val: val,
    'iexact':    lambda val: val,
    'startswith':    lambda val: '%s*'%val,
    'istartswith':    lambda val: '%s*'%val.lower(),
    'endswith':    lambda val: '*%s'%val,
    'iendswith':    lambda val: '*%s'%val.lower(),
    'contains':    lambda val: '*%s*'%val,
    'icontains':    lambda val: '*%s*'%val.lower(),
    'regex':    lambda val: val,
    'iregex':   lambda val: val.lower(),
    'gt':       lambda val: {"_from" : val, "include_lower" : False},
    'gte':      lambda val: {"_from" : val, "include_lower" : True},
    'lt':       lambda val: {"_to" : val, "include_upper": False},
    'lte':      lambda val: {"_to" : val, "include_upper": True},
    'range':    lambda val: {"_from" : val[0], "_to" : val[1], "include_lower" : True, "include_upper": True},
    'year':     lambda val: {"_from" : val[0], "_to" : val[1], "include_lower" : True, "include_upper": False},
    'isnull':   lambda val: None if val else {'$ne': None},
    'in':       lambda val: {'$in': val},
}

NEGATED_OPERATORS_MAP = {
    'exact':    lambda val: {'$ne': val},
    'gt':       lambda val: {"_to" : val, "include_upper": True},
    'gte':      lambda val: {"_to" : val, "include_upper": False},
    'lt':       lambda val: {"_from" : val, "include_lower" : True},
    'lte':      lambda val: {"_from" : val, "include_lower" : False},
    'isnull':   lambda val: {'$ne': None} if val else None,
    'in':       lambda val: {'$nin': val},
}

def _get_mapping(db_type, value, mapping):
    # TODO - comments. lotsa comments

    if value == NOT_PROVIDED:
        return None

    if value is None:
        return None

    if db_type in mapping:
        _func = mapping[db_type]
    else:
        _func = lambda val: val
    # TODO - what if the data is represented as list on the python side?
    if isinstance(value, list):
        return map(_func, value)
    return _func(value)

def python2db(db_type, value):
    return _get_mapping(db_type, value, TYPE_MAPPING_TO_DB)

def db2python(db_type, value):
    return _get_mapping(db_type, value, TYPE_MAPPING_FROM_DB)

def safe_call(func):
    @wraps(func)
    def _func(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception, e:
            import traceback
            traceback.print_exc()
            raise DatabaseError, DatabaseError(str(e)), sys.exc_info()[2]
    return _func

class DBQuery(NonrelQuery):
    # ----------------------------------------------
    # Public API
    # ----------------------------------------------
    def __init__(self, compiler, fields):
        super(DBQuery, self).__init__(compiler, fields)
        self._connection = self.connection.db_connection
        self.indexname =  self.query.get_meta().db_table
        self._ordering = []
        self.db_query = BoolQuery()

    # This is needed for debugging
    def __repr__(self):
        return '<DBQuery: %r ORDER %r>' % (self.db_query, self._ordering)

    @safe_call
    def fetch(self, low_mark, high_mark):
        results = self._get_results()
        hits =  results['hits']['hits']
        
        if low_mark > 0:
            hits = hits[low_mark:]
        if high_mark is not None:
            hits = hits[low_mark:high_mark - low_mark]

        for hit in hits:
            entity = hit["_source"]
            entity['id'] = hit['_id']
            yield entity

    @safe_call
    def count(self, limit=None):
        query = self.db_query
        if self.db_query.is_empty():
            query = MatchAllQuery()
        
        res = self._connection.count(query, indexes=[self.connection.db_name], doc_types=[self.indexname])
        return res["count"]

    @safe_call
    def delete(self):
        self._collection.remove(self.db_query)

    @safe_call
    def order_by(self, ordering):
        for order in ordering:
            if order.startswith('-'):
                order, direction = order[1:], {"reverse" : True}
            else:
                direction = 'desc'
            self._ordering.append({order: direction})

    # This function is used by the default add_filters() implementation
    @safe_call
    def add_filter(self, column, lookup_type, negated, db_type, value):
        # Emulated/converted lookups

        if negated and lookup_type in NEGATED_OPERATORS_MAP:
            op = NEGATED_OPERATORS_MAP[lookup_type]
            negated = False
        else:
            op = OPERATORS_MAP[lookup_type]
        value = op(self.convert_value_for_db(db_type, value))

        queryf = self._get_query_type(column, lookup_type, db_type, value)
        
        if negated:
            self.db_query.add_must_not(queryf)
        else:
            self.db_query.add_must(queryf)

    def _get_query_type(self, column, lookup_type, db_type, value):
        if db_type == "unicode":
            if (lookup_type == "exact" or lookup_type == "iexact"):
                return StringQuery('"%s"'%value, default_field=column)
            if (lookup_type == "startswith" or lookup_type == "istartswith"):
                return WildcardQuery(column, value)
            if (lookup_type == "endswith" or lookup_type == "iendswith"):
                return WildcardQuery(column, value)
            if (lookup_type == "contains" or lookup_type == "icontains"):
                return WildcardQuery(column, value)
            if (lookup_type == "regex" or lookup_type == "iregex"):
                return RegexTermQuery(column, value)

        if db_type == "datetime" or db_type == "date":
            if (lookup_type == "exact" or lookup_type == "iexact"):
                return TermQuery(column, value)
        
        if lookup_type in ["gt", "gte", "lt", "lte", "range", "year"]:
            value['field'] = column
            return RangeQuery(ESRange(**value))

        raise NotImplemented

    def _get_results(self):
        """
        @returns: elasticsearch iterator over results
        defined by self.query
        """
        query = self.db_query
        if self.db_query.is_empty():
            query = MatchAllQuery()
        if self._ordering:
            query.sort = self._ordering
        return self._connection.search(query, indexes=[self.connection.db_name], doc_types=[self.indexname])

class SQLCompiler(NonrelCompiler):
    """
    A simple query: no joins, no distinct, etc.
    """
    query_class = DBQuery

    def convert_value_from_db(self, db_type, value):
        # Handle list types
        if db_type is not None and \
                isinstance(value, (list, tuple)) and len(value) and \
                db_type.startswith('ListField:'):
            db_sub_type = db_type.split(':', 1)[1]
            value = [self.convert_value_from_db(db_sub_type, subvalue)
                     for subvalue in value]
        else:
            value = db2python(db_type, value)
        return value

    # This gets called for each field type when you insert() an entity.
    # db_type is the string that you used in the DatabaseCreation mapping
    def convert_value_for_db(self, db_type, value):
        if db_type is not None and \
                isinstance(value, (list, tuple)) and len(value) and \
                db_type.startswith('ListField:'):
            db_sub_type = db_type.split(':', 1)[1]
            value = [self.convert_value_for_db(db_sub_type, subvalue)
                     for subvalue in value]
        else:
            value = python2db(db_type, value)
        return value

class SQLInsertCompiler(NonrelInsertCompiler, SQLCompiler):
    @safe_call
    def insert(self, data, return_id=False):
        pk_column = self.query.get_meta().pk.column
        pk = None
        if pk_column in data:
            pk = data[pk_column]
        else:
            pk = unicode(ObjectId())
            data[pk_column] = pk
        db_table = self.query.get_meta().db_table
        res = self.connection.db_connection.index(data, self.connection.db_name, db_table, pk)
        #TODO: remove or timeout the refresh
        self.connection.db_connection.refresh([self.connection.db_name])

        return res['_id']

# TODO: Define a common nonrel API for updates and add it to the nonrel
# backend base classes and port this code to that API
class SQLUpdateCompiler(SQLCompiler):
    def execute_sql(self, return_id=False):
        """
        self.query - the data that should be inserted
        """
        data = {}
        for (field, value), column in zip(self.query.values, self.query.columns):
            data[column] = python2db(field.db_type(connection=self.connection), value)
        # every object should have a unique pk
        pk_field = self.query.model._meta.pk
        pk_name = pk_field.attname

        db_table = self.query.get_meta().db_table
        res = self.connection.db_connection.index(data, self.connection.db_name, db_table, pk)

        #TODO: remove or timeout the refresh
        self.connection.db_connection.refresh([self.connection.db_name])
        return res['_id']

class SQLDeleteCompiler(NonrelDeleteCompiler, SQLCompiler):
    def execute_sql(self, return_id=False):
        """
        self.query - the data that should be inserted
        """
        db_table = self.query.get_meta().db_table
        if len(self.query.where.children)==1 and isinstance(self.query.where.children[0][0].field, AutoField) and  self.query.where.children[0][1]=="in":
            for pk in self.query.where.children[0][3]:
                res = self.connection.db_connection.delete(self.connection.db_name, db_table, pk)
        #TODO: remove or timeout the refresh
        self.connection.db_connection.refresh([self.connection.db_name])
        return 
