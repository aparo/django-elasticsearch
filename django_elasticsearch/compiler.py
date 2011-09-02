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
from django.db.models.fields import NOT_PROVIDED
from django.utils.tree import Node
from pyes import MatchAllQuery, FilteredQuery, BoolQuery, StringQuery, \
                WildcardQuery, RegexTermQuery, RangeQuery, ESRange, \
                TermQuery, ConstantScoreQuery, TermFilter, TermsFilter, NotFilter, RegexTermFilter
from djangotoolbox.db.basecompiler import NonrelQuery, NonrelCompiler, \
    NonrelInsertCompiler, NonrelUpdateCompiler, NonrelDeleteCompiler
from django.db.models.fields import AutoField
import logging

TYPE_MAPPING_FROM_DB = {
    'unicode':  lambda val: unicode(val),
    'int':      lambda val: int(val),
    'float':    lambda val: float(val),
    'bool':     lambda val: bool(val),
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
    'iexact':    lambda val: val, #tofix
    'startswith':    lambda val: r'^%s' % re.escape(val),
    'istartswith':    lambda val: r'^%s' % re.escape(val),
    'endswith':    lambda val: r'%s$' % re.escape(val),
    'iendswith':    lambda val: r'%s$' % re.escape(val),
    'contains':    lambda val: r'%s' % re.escape(val),
    'icontains':    lambda val: r'%s' % re.escape(val),
    'regex':    lambda val: val,
    'iregex':   lambda val: re.compile(val, re.IGNORECASE),
    'gt':       lambda val: {"_from" : val, "include_lower" : False},
    'gte':      lambda val: {"_from" : val, "include_lower" : True},
    'lt':       lambda val: {"_to" : val, "include_upper": False},
    'lte':      lambda val: {"_to" : val, "include_upper": True},
    'range':    lambda val: {"_from" : val[0], "_to" : val[1], "include_lower" : True, "include_upper": True},
    'year':     lambda val: {"_from" : val[0], "_to" : val[1], "include_lower" : True, "include_upper": False},
    'isnull':   lambda val: None if val else {'$ne': None},
    'in':       lambda val: val,
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
        self._ordering = []
        self.db_query = ConstantScoreQuery()

    # This is needed for debugging
    def __repr__(self):
        return '<DBQuery: %r ORDER %r>' % (self.db_query, self._ordering)

    @safe_call
    def fetch(self, low_mark, high_mark):
        results = self._get_results()

        if low_mark > 0:
            results = results[low_mark:]
        if high_mark is not None:
            results = results[low_mark:high_mark - low_mark]

        for hit in results:
            entity = hit.get_data()
            entity['id'] = hit.meta.id
            yield entity

    @safe_call
    def count(self, limit=None):
        query = self.db_query
        if self.db_query.is_empty():
            query = MatchAllQuery()

        res = self._connection.count(query, doc_types=self.query.model._meta.db_table)
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
        if column == self.query.get_meta().pk.column:
            column = '_id'
        # Emulated/converted lookups

        if negated and lookup_type in NEGATED_OPERATORS_MAP:
            op = NEGATED_OPERATORS_MAP[lookup_type]
            negated = False
        else:
            op = OPERATORS_MAP[lookup_type]
        value = op(self.convert_value_for_db(db_type, value))

        queryf = self._get_query_type(column, lookup_type, db_type, value)

        if negated:
            self.db_query.add([NotFilter(queryf)])
        else:
            self.db_query.add([queryf])

    def _get_query_type(self, column, lookup_type, db_type, value):
        if db_type == "unicode":
            if (lookup_type == "exact" or lookup_type == "iexact"):
                q = TermQuery(column, value)
                return q
            if (lookup_type == "startswith" or lookup_type == "istartswith"):
                return RegexTermFilter(column, value)
            if (lookup_type == "endswith" or lookup_type == "iendswith"):
                return RegexTermFilter(column, value)
            if (lookup_type == "contains" or lookup_type == "icontains"):
                return RegexTermFilter(column, value)
            if (lookup_type == "regex" or lookup_type == "iregex"):
                return RegexTermFilter(column, value)

        if db_type == "datetime" or db_type == "date":
            if (lookup_type == "exact" or lookup_type == "iexact"):
                return TermFilter(column, value)

        #TermFilter, TermsFilter
        if lookup_type in ["gt", "gte", "lt", "lte", "range", "year"]:
            value['field'] = column
            return RangeQuery(ESRange(**value))
        if lookup_type == "in":
#            terms = [TermQuery(column, val) for val in value]
#            if len(terms) == 1:
#                return terms[0]
#            return BoolQuery(should=terms)
            return TermsFilter(field=column, values=value)
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
        #print "query", self.query.tables, query
        return self._connection.search(query, indices=[self.connection.db_name], doc_types=self.query.model._meta.db_table)

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

    def insert_params(self):
        conn = self.connection

        params = {
            'safe': conn.safe_inserts,
        }

        if conn.w:
            params['w'] = conn.w

        return params

    def _get_ordering(self):
        if not self.query.default_ordering:
            ordering = self.query.order_by
        else:
            ordering = self.query.order_by or self.query.get_meta().ordering
        result = []
        for order in ordering:
            if LOOKUP_SEP in order:
                #raise DatabaseError("Ordering can't span tables on non-relational backends (%s)" % order)
                print "Ordering can't span tables on non-relational backends (%s):skipping" % order
                continue
            if order == '?':
                raise DatabaseError("Randomized ordering isn't supported by the backend")

            order = order.lstrip('+')

            descending = order.startswith('-')
            name = order.lstrip('-')
            if name == 'pk':
                name = self.query.get_meta().pk.name
                order = '-' + name if descending else name

            if self.query.standard_ordering:
                result.append(order)
            else:
                if descending:
                    result.append(name)
                else:
                    result.append('-' + name)
        return result


class SQLInsertCompiler(NonrelInsertCompiler, SQLCompiler):
    @safe_call
    def insert(self, data, return_id=False):
        pk_column = self.query.get_meta().pk.column
        pk = None
        if pk_column in data:
            pk = data[pk_column]
        db_table = self.query.get_meta().db_table
        logging.debug("Insert data %s: %s" % (db_table, data))
        #print("Insert data %s: %s" % (db_table, data))
        res = self.connection.db_connection.index(data, self.connection.db_name, db_table, id=pk)
        #print "Insert result", res
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
        res = self.connection.db_connection.index(data, self.connection.db_name, db_table, id=pk)

        return res['_id']

class SQLDeleteCompiler(NonrelDeleteCompiler, SQLCompiler):
    def execute_sql(self, return_id=False):
        """
        self.query - the data that should be inserted
        """
        db_table = self.query.get_meta().db_table
        if len(self.query.where.children) == 1 and isinstance(self.query.where.children[0][0].field, AutoField) and  self.query.where.children[0][1] == "in":
            for pk in self.query.where.children[0][3]:
                self.connection.db_connection.delete(self.connection.db_name, db_table, pk)
        return
