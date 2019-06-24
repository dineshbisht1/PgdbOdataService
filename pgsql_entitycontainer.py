#!/usr/bin/env python

#   Postgres SQL container for pyslet Odata implementation
#   Copyright (C), 2014 Chris Daley <chebizarro@gmail.com>
#
#   This program is free software; you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation; either version 2 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program; if not, write to the Free Software
#   Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.

# This module implements a Postgres SQL import and export dialog to connect
# to a Postgres SQL database server as well as an export module to dump a diagram
# to Postgres SQL compatible SQL.
#

import psycopg2
import collections
import logging
import itertools
from pyslet.odata2 import sqlds
import pyslet.odata2.csdl as edm
from pyslet.odata2.core import EntityCollection, CommonExpression, PropertyExpression, BinaryExpression, \
    LiteralExpression, Operator, SystemQueryOption, ODataURI
from pyslet.odata2.sqlds import SQLTransaction
from pyslet.py2 import (
    buffer2,
    dict_items,
    dict_values,
    is_text,
    range3,
    to_text,
    ul)

# : the standard timeout while waiting for a database connection, in seconds
SQL_TIMEOUT = 90
MAX_CONNECTIONS=100

class PgSQLEntityContainer(sqlds.SQLEntityContainer):

    """Creates a container that represents a PgSQL database.

    Additional keyword arguments:

    pgsql_options
                A dictionary of additional options to pass as named arguments
                to the connect method.  It defaults to an empty dictionary

                For more information see psycopg2

    ..  psycopg2:   http://initd.org/psycopg/

    All other keyword arguments required to initialise the base class must be
    passed on construction except *dbapi* which is automatically set to the
    Python psycopg2 module."""

    def __init__(self, container, dsn, pgsql_options={}, **kwargs):
        self.container = container
        #self.entity_set = container.EntitySet
        self.dsn = dsn
        self.pgsql_options = pgsql_options
        #self.connection = self.open(self.pgsql_options)
        self.ParamsClass = PyFormatParams
        self.table_column_count = collections.defaultdict(int)
        self.dbapi = psycopg2
        super(PgSQLEntityContainer, self).__init__(container, dbapi=psycopg2, max_connections=MAX_CONNECTIONS, **kwargs)
        for es in self.container.EntitySet:
            self.bind_entity_set(es)

    def bind_entity_set(self, entity_set):
        entity_set.bind(self.get_collection_class(), container=self)

    def get_collection_class(self):
        'Overridden to return :py:class:`PgSQLEntityCollection`'
        return PgDBOpportunity

    def get_symmetric_navigation_class(self):
        'Overridden to return :py:class:`PgSQLAssociationCollection`'
        return PgSQLAssociationCollection

    def get_fk_class(self):
        'Overridden to return :py:class:`PgSQLForeignKeyCollection`'
        return PgSQLForeignKeyCollection

    def get_rk_class(self):
        'Overridden to return :py:class:`PgSQLReverseKeyCollection`'
        return PgSQLReverseKeyCollection
		
    #def mangle_name(self, source_path):
        #mangled_name = self.container.mangled_names[source_path]
        #if prefix:
        #    mangled_name = "%s.%s" % (self.table_name, mangled_name)
    #    print source_path
    #    return source_path 

    def open(self):
        dbc = self.dbapi.connect(**self.pgsql_options)
        c = dbc.cursor()
        c.execute("SELECT version();")
        record = c.fetchone()
        print("You are connected to - ", record,"\n")
        c.close()
        return dbc

    def break_connection(self, connection):
        'Calls the underlying interrupt method.'
        connection.interrupt()

    def prepare_sql_type(self, simple_value, params, nullable=None):
        '''
        Performs PgSQL custom mappings

        We inherit most of the type mappings but the following types use custom
        mappings:

        ==================  ===================================
            EDM Type         PgSQL Equivalent
        ------------------  -----------------------------------
        Edm.Binary          BYTEA
        Edm.Guid            UUID
        Edm.String          TEXT
        ==================  ===================================
        '''

        p = simple_value.pDef
        column_def = []

        if isinstance(simple_value, edm.BinaryValue):
            column_def.append(u"BYTEA")
        elif isinstance(simple_value, edm.GuidValue):
            column_def.append(u"UUID")
        elif isinstance(simple_value, edm.StringValue):
            if p.unicode is None or p.unicode:
                n = ""  # formerly "N", but "NVARCHAR" isn't a thing
                        # in postgres
            else:
                n = ""
            if p.fixedLength:
                if p.maxLength:
                    column_def.append(u"%sCHAR(%i)" % (n, p.maxLength))
                else:
                    raise edm.ModelConstraintError("Edm.String of fixed length missing max: %s" % p.name)
            elif p.maxLength:
                column_def.append(u"%sVARCHAR(%i)" % (n, p.maxLength))
            else:
                column_def.append(u"TEXT")
        else:
            return super(PgSQLEntityContainer, self).prepare_sql_type(
                simple_value, params, nullable
            )
        null_conditions = (
            nullable is not None and not nullable,
            nullable is None and not p.nullable,
        )
        if any(null_conditions):
            column_def.append(u' NOT NULL')
        if simple_value:
            # Format the default
            column_def.append(u' DEFAULT ')
            column_def.append(
                params.add_param(self.prepare_sql_value(simple_value))
            )
        return ''.join(column_def)

    def prepare_sql_value(self, simple_value):
        '''
        Returns a python value suitable for passing as a parameter.

        We inherit most of the value mappings but the following types have
        custom mappings.

        ==================  =============================================
            EDM Type         Python value added as parameter
        ------------------  ---------------------------------------------
        Edm.Binary          buffer object
        Edm.Guid            buffer object containing bytes representation
        ==================  ==============================================

        Our use of buffer type is not ideal as it generates warning when Python
        is run with the -3 flag (to check for Python 3 compatibility) but it
        seems unavoidable at the current time.
        '''
        if not simple_value:
            return None
        elif isinstance(simple_value, edm.BinaryValue):
            return buffer(simple_value.value)
        elif isinstance(simple_value, edm.GuidValue):
            return buffer(simple_value.value.bytes)
        else:
            return super(PgSQLEntityContainer, self).prepare_sql_value(
                simple_value
            )

#    def read_sql_value(self, simple_value, new_value):
#        'Reverses the transformation performed by prepare_sql_value'
#        if new_value is None:
#            simple_value.SetNull()
#        elif isinstance(new_value, types.BufferType):
#            new_value = str(new_value)
#            simple_value.SetFromValue(new_value)
#        else:
#            simple_value.SetFromValue(new_value)
#
#    def new_from_sql_value(self, sql_value):
#        '''
#        Returns a new simple value instance initialised from *sql_value*#
#
#        Overridden to ensure that buffer objects returned by the underlying DB
#        API are converted to strings.  Otherwise *sql_value* is passed directly
#        to the parent.
#        '''
#        if isinstance(sql_value, types.BufferType):
#            result = edm.BinaryValue()
#            result.SetFromValue(str(sql_value))
#            return result
#        else:
#            return super(PgSQLEntityContainer, self).new_from_sql_value(
#                sql_value
#            )


class PgSQLEntityCollectionBase(sqlds.SQLCollectionBase):
    '''
    Base class for PgSQL SQL custom mappings.

    This class provides some PgSQL specific mappings for certain functions to
    improve compatibility with the OData expression language.
    '''

    def sql_expression_mod(self, expression, params, context):
        'Converts the mod expression'
        'Converts the div expression: maps to SQL "/" '  # TODO: What is this doing here?
        return self.sql_expression_generic_binary(
            expression, params, context, '%'
        )

    def sql_expression_replace(self, expression, params, context):
        'Converts the replace method'
        query = ["replace("]
        query.append(self.sql_expression(expression.operands[0], params, ','))
        query.append(")")
        return ''.join(query)  # don't bother with brackets!

    def sql_expression_length(self, expression, params, context):
        'Converts the length method: maps to length( op[0] )'
        query = ["length("]
        query.append(self.sql_expression(expression.operands[0], params, ','))
        query.append(")")
        return ''.join(query)  # don't bother with brackets!

    def sql_expression_round(self, expression, params, context):
        'Converts the round method'
        query = ["round("]
        query.append(self.sql_expression(expression.operands[0], params, ','))
        query.append(")")
        return ''.join(query)  # don't bother with brackets!

    def sql_expression_floor(self, expression, params, context):
        'Converts the floor method'
        query = ["floor("]
        query.append(self.sql_expression(expression.operands[0], params, ','))
        query.append(")")
        return ''.join(query)  # don't bother with brackets!

    def sql_expression_ceiling(self, expression, params, context):
        'Converts the ceiling method'
        query = ["ceil("]
        query.append(self.sql_expression(expression.operands[0], params, ','))
        query.append(")")
        return ''.join(query)  # don't bother with brackets!


class PgSQLEntityCollection(PgSQLEntityCollectionBase, sqlds.SQLEntityCollection):
    'PgSQL-specific collection for entity sets'
    pass

class PgSQLAssociationCollection(PgSQLEntityCollectionBase, sqlds.SQLAssociationCollection):
    'PgSQL-specific collection for symmetric association sets'
    pass


class PgSQLForeignKeyCollection(PgSQLEntityCollectionBase, sqlds.SQLForeignKeyCollection):
    'PgSQL-specific collection for navigation from a foreign key'
    pass


class PgSQLReverseKeyCollection(PgSQLEntityCollectionBase, sqlds.SQLReverseKeyCollection):
    'PgSQL-specific collection for navigation to a foreign key'
    pass


class PyFormatParams(sqlds.SQLParams):
    'A class for building parameter lists using ":1", ":2",... syntax'

    def __init__(self):
        super(PyFormatParams, self).__init__()
        self.params = []

    def add_param(self, value):
        self.params.append(value)
        return "%s"

def unmangle_entity_set_name(name):
    db_name, m_name = name.split('__', 1)
    db_name = unmangle_db_name(db_name)
    m_name = unmangle_measurement_name(m_name)
    return db_name, m_name

class PgDBOpportunity(EntityCollection):
    """A base class to provide core SQL functionality.

    Additional keyword arguments:

    container
            A :py:class:`SQLEntityContainer` instance.

    On construction a data connection is acquired from *container*, this
    may prevent other threads from using the database until the lock is
    released by the :py:meth:`close` method."""

    DEFAULT_VALUE = True
    """A boolean indicating whether or not the collection supports the
    syntax::

        UPDATE "MyTable" SET "MyField"=DEFAULT

    Most databases do support this syntax but SQLite does not.  In cases
    where this is False, default values are set explicitly as they are
    defined in the metadata model instead.  If True then the default
    values defined in the metadata model are ignored by the
    collection object."""

    def __init__(self, container, **kwargs):
        super(PgDBOpportunity, self).__init__(**kwargs)
        #: the parent container (database) for this collection
        self.container = container
        # the quoted table name containing this collection
        self.table_name = self.container.mangled_names[(self.entity_set.name,)]
        #print self.entity_set.name,"\n";
        #self.table_name = self.entity_set.name
        self.auto_keys = False
        for k in self.entity_set.keys:
            source_path = (self.entity_set.name, k)
            if source_path in self.container.ro_names:
                self.auto_keys = True
        self._joins = None
        # force orderNames to be initialised
        self.set_orderby(None)
        #: a connection to the database acquired with
        #: :meth:`SQLEntityContainer.acquire_connection`
        self.connection = None
        self._sqlLen = None
        self._sqlGen = None
        try:
            self.connection = self.container.acquire_connection(SQL_TIMEOUT)
            if self.connection is None:
                raise DatabaseBusy(
                    "Failed to acquire connection after %is" % SQL_TIMEOUT)
        except:
            self.close()
            raise

    def close(self):
        """Closes the cursor and database connection if they are open."""
        if self.connection is not None:
            self.container.release_connection(self.connection)
            self.connection = None

    def __len__(self):
        if self._sqlLen is None:
            query = ["SELECT COUNT(*) FROM %s" % self.table_name]
            params = self.container.ParamsClass()
            where = self.where_clause(None, params)
            query.append(self.join_clause())
            query.append(where)
            query = ''.join(query)
            self._sqlLen = (query, params)
        else:
            query, params = self._sqlLen
        transaction = SQLTransaction(self.container, self.connection)
        try:
            transaction.begin()
            logging.info("%s; %s", query, to_text(params.params))
            transaction.execute(query, params)
            # get the result
            result = transaction.cursor.fetchone()[0]
            # we haven't changed the database, but we don't want to
            # leave the connection idle in transaction
            transaction.commit()
            return result
        except Exception as e:
            # we catch (almost) all exceptions and re-raise after rollback
            transaction.rollback(e)
        finally:
            transaction.close()

    def entity_generator(self):
        entity, values = None, None
        if self._sqlGen is None:
            entity = self.new_entity()
            query = ["SELECT "]
            params = self.container.ParamsClass()
            column_names, values = zip(*list(self.select_fields(entity)))
            # values is used later for the first result
            column_names = list(column_names)
            self.orderby_cols(column_names, params)
            query.append(", ".join(column_names))
            query.append(' FROM ')
            query.append(self.table_name)
            # we force where and orderby to be calculated before the
            # join clause is added as they may add to the joins
            where = self.where_clause(
                None, params, use_filter=True, use_skip=False)
            orderby = self.orderby_clause()
            query.append(self.join_clause())
            query.append(where)
            query.append(orderby)
            query = ''.join(query)
            self._sqlGen = query, params
        else:
            query, params = self._sqlGen
        transaction = SQLTransaction(self.container, self.connection)
        try:
            transaction.begin()
            logging.info("%s; %s", query, to_text(params.params))
            transaction.execute(query, params)
            while True:
                row = transaction.cursor.fetchone()
                if row is None:
                    break
                if entity is None:
                    entity = self.new_entity()
                    values = next(
                        itertools.islice(
                            zip(*list(self.select_fields(entity))), 1, None))
                for value, new_value in zip(values, row):
                    self.container.read_sql_value(value, new_value)
                entity.exists = True
                yield entity
                entity, values = None, None
            # we haven't changed the database, but we don't want to
            # leave the connection idle in transaction
            transaction.commit()
        except Exception as e:
            transaction.rollback(e)
        finally:
            transaction.close()

    def itervalues(self):
        return self.expand_entities(
            self.entity_generator())

    def set_page(self, top, skip=0, skiptoken=None):
        """Sets the values for paging.

        Our implementation uses a special format for *skiptoken*.  It is
        a comma-separated list of simple literal values corresponding to
        the values required by the ordering augmented with the key
        values to ensure uniqueness.

        For example, if $orderby=A,B on an entity set with key K then
        the skiptoken will typically have three values comprising the
        last values returned for A,B and K in that order.  In cases
        where the resulting skiptoken would be unreasonably large an
        additional integer (representing a further skip) may be appended
        and the whole token expressed relative to an earlier skip
        point."""
        self.top = top
        self.skip = skip
        if skiptoken is None:
            self.skiptoken = None
        else:
            # parse a sequence of literal values
            p = core.Parser(skiptoken)
            self.skiptoken = []
            while True:
                p.parse_wsp()
                self.skiptoken.append(
                    p.require_production(p.parse_uri_literal()))
                p.parse_wsp()
                if not p.parse(','):
                    if p.match_end():
                        break
                    else:
                        raise core.InvalidSystemQueryOption(
                            "Unrecognized $skiptoken: %s" % skiptoken)
            if self.orderby is None:
                order_len = 0
            else:
                order_len = len(self.orderby)
            if (len(self.skiptoken) ==
                    order_len + len(self.entity_set.keys) + 1):
                # the last value must be an integer we add to skip
                if isinstance(self.skiptoken[-1], edm.Int32Value):
                    self.skip += self.skiptoken[-1].value
                    self.skiptoken = self.skiptoken[:-1]
                else:
                    raise core.InvalidSystemQueryOption(
                        "skiptoken incompatible with ordering: %s" % skiptoken)
            elif len(self.skiptoken) != order_len + len(self.entity_set.keys):
                raise core.InvalidSystemQueryOption(
                    "skiptoken incompatible with ordering: %s" % skiptoken)
        self.nextSkiptoken = None

    def next_skiptoken(self):
        if self.nextSkiptoken:
            token = []
            for t in self.nextSkiptoken:
                token.append(core.ODataURI.format_literal(t))
            return ",".join(token)
        else:
            return None

    def page_generator(self, set_next=False):
        if self.top == 0:
            # end of paging
            return
        skip = self.skip
        top = self.top
        topmax = self.topmax
        if topmax is not None:
            if top is not None:
                limit = min(top, topmax)
            else:
                limit = topmax
        else:
            limit = top
        entity = self.new_entity()
        query = ["SELECT "]
        skip, limit_clause = self.container.select_limit_clause(skip, limit)
        if limit_clause:
            query.append(limit_clause)
        params = self.container.ParamsClass()
        column_names, values = zip(*list(self.select_fields(entity)))
        column_names = list(column_names)
        self.orderby_cols(column_names, params, True)
        query.append(", ".join(column_names))
        query.append(' FROM ')
        query.append(self.table_name)
        where = self.where_clause(None, params, use_filter=True, use_skip=True)
        orderby = self.orderby_clause()
        query.append(self.join_clause())
        query.append(where)
        query.append(orderby)
        skip, limit_clause = self.container.limit_clause(skip, limit)
        if limit_clause:
            query.append(limit_clause)
        query = ''.join(query)
        transaction = SQLTransaction(self.container, self.connection)
        try:
            transaction.begin()
            logging.info("%s; %s", query, to_text(params.params))
            transaction.execute(query, params)
            while True:
                row = transaction.cursor.fetchone()
                if row is None:
                    # no more pages
                    if set_next:
                        self.top = self.skip = 0
                        self.skipToken = None
                    break
                if skip:
                    skip = skip - 1
                    continue
                if entity is None:
                    entity = self.new_entity()
                    values = next(
                        itertools.islice(
                            zip(*list(self.select_fields(entity))), 1, None))
                row_values = list(row)
                for value, new_value in zip(values, row_values):
                    self.container.read_sql_value(value, new_value)
                entity.exists = True
                yield entity
                if topmax is not None:
                    topmax = topmax - 1
                    if topmax < 1:
                        # this is the last entity, set the nextSkiptoken
                        order_values = row_values[-len(self.orderNames):]
                        self.nextSkiptoken = []
                        for v in order_values:
                            self.nextSkiptoken.append(
                                self.container.new_from_sql_value(v))
                        tokenlen = 0
                        for v in self.nextSkiptoken:
                            if v and isinstance(v, (edm.StringValue,
                                                    edm.BinaryValue)):
                                tokenlen += len(v.value)
                        # a really large skiptoken is no use to anyone
                        if tokenlen > 512:
                            # ditch this one, copy the previous one and add a
                            # skip
                            self.nextSkiptoken = list(self.skiptoken)
                            v = edm.Int32Value()
                            v.set_from_value(self.topmax)
                            self.nextSkiptoken.append(v)
                        if set_next:
                            self.skiptoken = self.nextSkiptoken
                            self.skip = 0
                        break
                if top is not None:
                    top = top - 1
                    if top < 1:
                        if set_next:
                            if self.skip is not None:
                                self.skip = self.skip + self.top
                            else:
                                self.skip = self.top
                        break
                entity = None
            # we haven't changed the database, but we don't want to
            # leave the connection idle in transaction
            transaction.commit()
        except Exception as e:
            transaction.rollback(e)
        finally:
            transaction.close()

    def iterpage(self, set_next=False):
        return self.expand_entities(
            self.page_generator(set_next))

    def __getitem__(self, key):
        entity = self.new_entity()
        entity.set_key(key)
        params = self.container.ParamsClass()
        query = ["SELECT "]
        column_names, values = zip(*list(self.select_fields(entity)))
        query.append(", ".join(column_names))
        query.append(' FROM ')
        query.append(self.table_name)
        where = self.where_clause(entity, params)
        query.append(self.join_clause())
        query.append(where)
        query = ''.join(query)
        transaction = SQLTransaction(self.container, self.connection)
        try:
            transaction.begin()
            logging.info("%s; %s", query, to_text(params.params))
            transaction.execute(query, params)
            rowcount = transaction.cursor.rowcount
            row = transaction.cursor.fetchone()
            if rowcount == 0 or row is None:
                raise KeyError
            elif rowcount > 1 or (rowcount == -1 and
                                  transaction.cursor.fetchone() is not None):
                # whoops, that was unexpected
                raise SQLError(
                    "Integrity check failure, non-unique key: %s" % repr(key))
            for value, new_value in zip(values, row):
                self.container.read_sql_value(value, new_value)
            entity.exists = True
            entity.expand(self.expand, self.select)
            transaction.commit()
            return entity
        except KeyError:
            # no need to do a rollback for a KeyError, will still
            # close the transaction of course
            raise
        except Exception as e:
            transaction.rollback(e)
        finally:
            transaction.close()

    def read_stream(self, key, out=None):
        entity = self.new_entity()
        entity.set_key(key)
        svalue = self._get_streamid(key)
        sinfo = core.StreamInfo()
        if svalue:
            estream = self.container.streamstore.get_stream(svalue.value)
            sinfo.type = params.MediaType.from_str(estream['mimetype'].value)
            sinfo.created = estream['created'].value.with_zone(0)
            sinfo.modified = estream['modified'].value.with_zone(0)
            sinfo.size = estream['size'].value
            sinfo.md5 = estream['md5'].value
        else:
            estream = None
            sinfo.size = 0
            sinfo.md5 = hashlib.md5(b'').digest()
        if out is not None and svalue:
            with self.container.streamstore.open_stream(estream, 'r') as src:
                actual_size, actual_md5 = self._copy_src(src, out)
            if sinfo.size is not None and sinfo.size != actual_size:
                # unexpected size mismatch
                raise SQLError("stream size mismatch on read %s" %
                               entity.get_location())
            if sinfo.md5 is not None and sinfo.md5 != actual_md5:
                # md5 mismatch
                raise SQLError("stream checksum mismatch on read %s" %
                               entity.get_location())
        return sinfo

    def read_stream_close(self, key):
        entity = self.new_entity()
        entity.set_key(key)
        svalue = self._get_streamid(key)
        sinfo = core.StreamInfo()
        if svalue:
            estream = self.container.streamstore.get_stream(svalue.value)
            sinfo.type = params.MediaType.from_str(estream['mimetype'].value)
            sinfo.created = estream['created'].value.with_zone(0)
            sinfo.modified = estream['modified'].value.with_zone(0)
            sinfo.size = estream['size'].value
            sinfo.md5 = estream['md5'].value
            return sinfo, self._read_stream_gen(estream, sinfo)
        else:
            estream = None
            sinfo.size = 0
            sinfo.md5 = hashlib.md5('').digest()
            self.close()
            return sinfo, []

    def _read_stream_gen(self, estream, sinfo):
        try:
            with self.container.streamstore.open_stream(estream, 'r') as src:
                h = hashlib.md5()
                count = 0
                while True:
                    data = src.read(io.DEFAULT_BUFFER_SIZE)
                    if len(data):
                        count += len(data)
                        h.update(data)
                        yield data
                    else:
                        break
            if sinfo.size is not None and sinfo.size != count:
                # unexpected size mismatch
                raise SQLError("stream size mismatch on read [%i]" %
                               estream.key())
            if sinfo.md5 is not None and sinfo.md5 != h.digest():
                # md5 mismatch
                raise SQLError("stream checksum mismatch on read [%i]" %
                               estream.key())
        finally:
            self.close()

    def update_stream(self, src, key, sinfo=None):
        e = self.new_entity()
        e.set_key(key)
        if sinfo is None:
            sinfo = core.StreamInfo()
        etag = e.etag_values()
        if len(etag) == 1 and isinstance(etag[0], edm.BinaryValue):
            h = hashlib.sha256()
            etag = etag[0]
        else:
            h = None
        c, v = self.stream_field(e, prefix=False)
        if self.container.streamstore:
            # spool the data into the store and store the stream key
            estream = self.container.streamstore.new_stream(sinfo.type,
                                                            sinfo.created)
            with self.container.streamstore.open_stream(estream, 'w') as dst:
                sinfo.size, sinfo.md5 = self._copy_src(src, dst, sinfo.size, h)
            if sinfo.modified is not None:
                # force modified date based on input
                estream['modified'].set_from_value(
                    sinfo.modified.shift_zone(0))
                estream.commit()
            v.set_from_value(estream.key())
        else:
            raise NotImplementedError
        if h is not None:
            etag.set_from_value(h.digest())
        oldvalue = self._get_streamid(key)
        transaction = SQLTransaction(self.container, self.connection)
        try:
            transaction.begin()
            # store the new stream value for the entity
            query = ['UPDATE ', self.table_name, ' SET ']
            params = self.container.ParamsClass()
            query.append(
                "%s=%s" %
                (c, params.add_param(self.container.prepare_sql_value(v))))
            query.append(' WHERE ')
            where = []
            for k, kv in dict_items(e.key_dict()):
                where.append(
                    '%s=%s' %
                    (self.container.mangled_names[(self.entity_set.name, k)],
                     params.add_param(self.container.prepare_sql_value(kv))))
            query.append(' AND '.join(where))
            query = ''.join(query)
            logging.info("%s; %s", query, to_text(params.params))
            transaction.execute(query, params)
        except Exception as e:
            # we allow the stream store to re-use the same database but
            # this means we can't transact on both at once (from the
            # same thread) - settle for logging at the moment
            # self.container.streamstore.delete_stream(estream)
            logging.error("Orphan stream created %s[%i]",
                          estream.entity_set.name, estream.key())
            transaction.rollback(e)
        finally:
            transaction.close()
        # now remove the old stream
        if oldvalue:
            oldstream = self.container.streamstore.get_stream(oldvalue.value)
            self.container.streamstore.delete_stream(oldstream)

    def _get_streamid(self, key, transaction=None):
        entity = self.new_entity()
        entity.set_key(key)
        params = self.container.ParamsClass()
        query = ["SELECT "]
        sname, svalue = self.stream_field(entity)
        query.append(sname)
        query.append(' FROM ')
        query.append(self.table_name)
        query.append(self.where_clause(entity, params, use_filter=False))
        query = ''.join(query)
        if transaction is None:
            transaction = SQLTransaction(self.container, self.connection)
        try:
            transaction.begin()
            logging.info("%s; %s", query, to_text(params.params))
            transaction.execute(query, params)
            rowcount = transaction.cursor.rowcount
            row = transaction.cursor.fetchone()
            if rowcount == 0 or row is None:
                raise KeyError
            elif rowcount > 1 or (rowcount == -1 and
                                  transaction.cursor.fetchone() is not None):
                # whoops, that was unexpected
                raise SQLError(
                    "Integrity check failure, non-unique key: %s" % repr(key))
            self.container.read_sql_value(svalue, row[0])
            entity.exists = True
            transaction.commit()
        except Exception as e:
            transaction.rollback(e)
        finally:
            transaction.close()
        return svalue

    def _copy_src(self, src, dst, max_bytes=None, xhash=None):
        md5 = hashlib.md5()
        rbytes = max_bytes
        count = 0
        while rbytes is None or rbytes > 0:
            if rbytes is None:
                data = src.read(io.DEFAULT_BUFFER_SIZE)
            else:
                data = src.read(min(rbytes, io.DEFAULT_BUFFER_SIZE))
                rbytes -= len(data)
            if not data:
                # we're done
                break
            # add the data to the hash
            md5.update(data)
            if xhash is not None:
                xhash.update(data)
            while data:
                wbytes = dst.write(data)
                if wbytes is None:
                    if not isinstance(dst, io.RawIOBase):
                        wbytes = len(data)
                    else:
                        wbytes = 0
                        time.sleep(0)   # yield to prevent hard loop
                if wbytes < len(data):
                    data = data[wbytes:]
                else:
                    data = None
                count += wbytes
        return count, md5.digest()

    def reset_joins(self):
        """Sets the base join information for this collection"""
        self._joins = {}
        self._aliases = set()
        self._aliases.add(self.table_name)

    def next_alias(self):
        i = len(self._aliases)
        while True:
            alias = "nav%i" % i
            if alias in self._aliases:
                i += 1
            else:
                break
        return alias

    def add_join(self, name):
        """Adds a join to this collection

        name
            The name of the navigation property to traverse.

        The return result is the alias name to use for the target table.

        As per the specification, the target must have multiplicity 1 or
        0..1."""
        if self._joins is None:
            self.reset_joins()
        elif name in self._joins:
            return self._joins[name][0]
        alias = self.next_alias()
        src_multiplicity, dst_multiplicity = \
            self.entity_set.get_multiplicity(name)
        if dst_multiplicity not in (edm.Multiplicity.ZeroToOne,
                                    edm.Multiplicity.One):
            # we can't join on this navigation property
            raise NotImplementedError(
                "NavigationProperty %s.%s cannot be used in an expression" %
                (self.entity_set.name, name))
        fk_mapping = self.container.fk_table[self.entity_set.name]
        link_end = self.entity_set.navigation[name]
        target_set = self.entity_set.get_target(name)
        target_table_name = self.container.mangled_names[(target_set.name, )]
        join = []
        if link_end in fk_mapping:
            # we own the foreign key
            for key_name in target_set.keys:
                join.append(
                    '%s.%s=%s.%s' %
                    (self.table_name, self.container.mangled_names[
                        (self.entity_set.name, link_end.parent.name,
                         key_name)],
                     alias,
                     self.container.mangled_names[
                        (target_set.name, key_name)]))
            join = ' LEFT JOIN %s AS %s ON %s' % (
                target_table_name, alias, ' AND '.join(join))
            self._joins[name] = (alias, join)
            self._aliases.add(alias)
        else:
            target_fk_mapping = self.container.fk_table[target_set.name]
            if link_end.otherEnd in target_fk_mapping:
                # target table has the foreign key
                for key_name in self.entity_set.keys:
                    join.append(
                        '%s.%s=%s.%s' %
                        (self.table_name, self.container.mangled_names[
                            (self.entity_set.name, key_name)],
                         alias,
                         self.container.mangled_names[
                            (target_set.name,
                             link_end.parent.name, key_name)]))
                join = ' LEFT JOIN %s AS %s ON %s' % (
                    target_table_name, alias, ' AND '.join(join))
                self._joins[name] = (alias, join)
                self._aliases.add(alias)
            else:
                # relation is in an auxiliary table
                src_set, src_name, dst_set, dst_name, ukeys = \
                    self.container.aux_table[link_end.parent.name]
                if self.entity_set is src_set:
                    name2 = dst_name
                else:
                    name2 = src_name
                aux_table_name = self.container.mangled_names[(
                    link_end.parent.name, )]
                for key_name in self.entity_set.keys:
                    join.append(
                        '%s.%s=%s.%s' %
                        (self.table_name, self.container.mangled_names[
                            (self.entity_set.name, key_name)],
                         alias, self.container.mangled_names[
                            (link_end.parent.name, self.entity_set.name,
                             name, key_name)]))
                join = ' LEFT JOIN %s AS %s ON %s' % (
                    aux_table_name, alias, ' AND '.join(join))
                self._aliases.add(alias)
                join2 = []
                alias2 = self.next_alias()
                for key_name in target_set.keys:
                    join2.append(
                        '%s.%s=%s.%s' %
                        (alias, self.container.mangled_names[
                            (link_end.parent.name, target_set.name,
                             name2, key_name)],
                         alias2, self.container.mangled_names[
                            (target_set.name, key_name)]))
                join2 = ' LEFT JOIN %s AS %s ON %s' % (
                    target_table_name, alias2, ' AND '.join(join2))
                self._aliases.add(alias2)
                alias = alias2
                self._joins[name] = (alias, join + join2)
        return alias

    def join_clause(self):
        """A utility method to return the JOIN clause.

        Defaults to an empty expression."""
        if self._joins is None:
            self.reset_joins()
        return ''.join(x[1] for x in dict_values(self._joins))

    def set_filter(self, filter):
        self._joins = None
        self.filter = filter
        self.set_page(None)
        self._sqlLen = None
        self._sqlGen = None

    def where_clause(
            self,
            entity,
            params,
            use_filter=True,
            use_skip=False,
            null_cols=()):
        """A utility method that generates the WHERE clause for a query

        entity
                An optional entity within this collection that is the focus
                of this query.  If not None the resulting WHERE clause will
                restrict the query to this entity only.

        params
                The :py:class:`SQLParams` object to add parameters to.

        use_filter
                Defaults to True, indicates if this collection's filter should
                be added to the WHERE clause.

        use_skip
                Defaults to False, indicates if the skiptoken should be used
                in the where clause.  If True then the query is limited to
                entities appearing after the skiptoken's value (see below).

        null_cols
                An iterable of mangled column names that must be NULL (defaults
                to an empty tuple).  This argument is used during updates to
                prevent the replacement of non-NULL foreign keys.

        The operation of the skiptoken deserves some explanation.  When in
        play the skiptoken contains the last value of the order expression
        returned.  The order expression always uses the keys to ensure
        unambiguous ordering.  The clause added is best served with an
        example.  If an entity has key K and an order expression such
        as "tolower(Name) desc" then the query will contain
        something like::

                SELECT K, Nname, DOB, LOWER(Name) AS o_1, K ....
                        WHERE (o_1 < ? OR (o_1 = ? AND K > ?))

        The values from the skiptoken will be passed as parameters."""
        where = []
        if entity is not None:
            self.where_entity_clause(where, entity, params)
        if self.filter is not None and use_filter:
            # use_filter option adds the current filter too
            where.append('(' + self.sql_expression(self.filter, params) + ')')
        if self.skiptoken is not None and use_skip:
            self.where_skiptoken_clause(where, params)
        for nullCol in null_cols:
            where.append('%s IS NULL' % nullCol)
        if where:
            return ' WHERE ' + ' AND '.join(where)
        else:
            return ''

    def where_entity_clause(self, where, entity, params):
        """Adds the entity constraint expression to a list of SQL expressions.

        where
                The list to append the entity expression to.

        entity
                An expression is added to restrict the query to this entity"""
        for k, v in dict_items(entity.key_dict()):
            where.append(
                '%s.%s=%s' %
                (self.table_name,
                 self.container.mangled_names[(self.entity_set.name, k)],
                 params.add_param(self.container.prepare_sql_value(v))))

    def where_skiptoken_clause(self, where, params):
        """Adds the entity constraint expression to a list of SQL expressions.

        where
                The list to append the skiptoken expression to."""
        skip_expression = []
        i = ket = 0
        while True:
            if self.orderby and i < len(self.orderby):
                oname = None
                expression, dir = self.orderby[i]
            else:
                oname, dir = self.orderNames[i]
            v = self.skiptoken[i]
            op = ">" if dir > 0 else "<"
            if oname is None:
                o_expression = self.sql_expression(expression, params, op)
            else:
                o_expression = oname
            skip_expression.append(
                "(%s %s %s" %
                (o_expression,
                 op,
                 params.add_param(
                     self.container.prepare_sql_value(v))))
            ket += 1
            i += 1
            if i < len(self.orderNames):
                # more to come
                if oname is None:
                    # remake the expression
                    o_expression = self.sql_expression(expression, params, '=')
                skip_expression.append(
                    " OR (%s = %s AND " %
                    (o_expression, params.add_param(
                        self.container.prepare_sql_value(v))))
                ket += 1
                continue
            else:
                skip_expression.append(")" * ket)
                break
        where.append(''.join(skip_expression))

    def set_orderby(self, orderby):
        """Sets the orderby rules for this collection.

        We override the default implementation to calculate a list
        of field name aliases to use in ordered queries.  For example,
        if the orderby expression is "tolower(Name) desc" then each SELECT
        query will be generated with an additional expression, e.g.::

                SELECT ID, Name, DOB, LOWER(Name) AS o_1 ...
                    ORDER BY o_1 DESC, ID ASC

        The name "o_1" is obtained from the name mangler using the tuple::

                (entity_set.name,'o_1')

        Subsequent order expressions have names 'o_2', 'o_3', etc.

        Notice that regardless of the ordering expression supplied the
        keys are always added to ensure that, when an ordering is
        required, a defined order results even at the expense of some
        redundancy."""
        self.orderby = orderby
        self.set_page(None)
        self.orderNames = []
        if self.orderby is not None:
            oi = 0
            for expression, direction in self.orderby:
                oi = oi + 1
                oname = "o_%i" % oi
                oname = self.container.mangled_names.get(
                    (self.entity_set.name, oname), oname)
                self.orderNames.append((oname, direction))
        for key in self.entity_set.keys:
            mangled_name = self.container.mangled_names[
                (self.entity_set.name, key)]
            mangled_name = "%s.%s" % (self.table_name, mangled_name)
            self.orderNames.append((mangled_name, 1))
        self._sqlGen = None

    def orderby_clause(self):
        """A utility method to return the orderby clause.

        params
                The :py:class:`SQLParams` object to add parameters to."""
        if self.orderNames:
            orderby = []
            for expression, direction in self.orderNames:
                orderby.append(
                    "%s %s" % (expression, "DESC" if direction < 0 else "ASC"))
            return ' ORDER BY ' + ", ".join(orderby) + ' '
        else:
            return ''

    def orderby_cols(self, column_names, params, force_order=False):
        """A utility to add the column names and aliases for the ordering.

        column_names
            A list of SQL column name/alias expressions

        params
            The :py:class:`SQLParams` object to add parameters to.

        force_order
            Forces the addition of an ordering by key if an orderby
            expression has not been set."""
        oname_index = 0
        if self.orderby is not None:
            for expression, direction in self.orderby:
                oname, odir = self.orderNames[oname_index]
                oname_index += 1
                sql_expression = self.sql_expression(expression, params)
                column_names.append("%s AS %s" % (sql_expression, oname))
        if self.orderby is not None or force_order:
            # add the remaining names (which are just the keys)
            while oname_index < len(self.orderNames):
                oname, odir = self.orderNames[oname_index]
                oname_index += 1
                column_names.append(oname)

    def _mangle_name(self, source_path, prefix=True):
        mangled_name = self.container.mangled_names[source_path]
        if prefix:
            mangled_name = "%s.%s" % (self.table_name, mangled_name)
        return mangled_name

    def insert_fields(self, entity):
        """A generator for inserting mangled property names and values.

        entity
            Any instance of :py:class:`~pyslet.odata2.csdl.Entity`

        The yielded values are tuples of (mangled field name,
        :py:class:`~pyslet.odata2.csdl.SimpleValue` instance).

        Read only fields are never generated, even if they are keys.
        This allows automatically generated keys to be used and also
        covers the more esoteric use case where a foreign key constraint
        exists on the primary key (or part thereof) - in the latter case
        the relationship should be marked as required to prevent
        unexpected constraint violations.

        Otherwise, only selected fields are yielded so if you attempt to
        insert a value without selecting the key fields you can expect a
        constraint violation unless the key is read only."""
        for k, v in entity.data_items():
            source_path = (self.entity_set.name, k)
            if (source_path not in self.container.ro_names and
                    entity.is_selected(k)):
                if isinstance(v, edm.SimpleValue):
                    yield self._mangle_name(source_path, prefix=False), v
                else:
                    for sub_path, fv in self._complex_field_generator(v):
                        source_path = tuple([self.entity_set.name, k] +
                                            sub_path)
                        yield self._mangle_name(source_path, prefix=False), fv

    def auto_fields(self, entity):
        """A generator for selecting auto mangled property names and values.

        entity
            Any instance of :py:class:`~pyslet.odata2.csdl.Entity`

        The yielded values are tuples of (mangled field name,
        :py:class:`~pyslet.odata2.csdl.SimpleValue` instance).

        Only fields that are read only are yielded with the caveat that
        they must also be either selected or keys.  The purpose of this
        method is to assist with reading back automatically generated
        field values after an insert or update."""
        keys = entity.entity_set.keys
        for k, v in entity.data_items():
            source_path = (self.entity_set.name, k)
            if (source_path in self.container.ro_names and (
                    entity.is_selected(k) or k in keys)):
                if isinstance(v, edm.SimpleValue):
                    yield self._mangle_name(source_path), v
                else:
                    for sub_path, fv in self._complex_field_generator(v):
                        source_path = tuple([self.entity_set.name, k] +
                                            sub_path)
                        yield self._mangle_name(source_path), fv

    def key_fields(self, entity):
        """A generator for selecting mangled key names and values.

        entity
            Any instance of :py:class:`~pyslet.odata2.csdl.Entity`

        The yielded values are tuples of (mangled field name,
        :py:class:`~pyslet.odata2.csdl.SimpleValue` instance).
        Only the keys fields are yielded."""
        for k in entity.entity_set.keys:
            v = entity[k]
            source_path = (self.entity_set.name, k)
            yield self._mangle_name(source_path), v

    def select_fields(self, entity, prefix=True):
        """A generator for selecting mangled property names and values.

        entity
            Any instance of :py:class:`~pyslet.odata2.csdl.Entity`

        The yielded values are tuples of (mangled field name,
        :py:class:`~pyslet.odata2.csdl.SimpleValue` instance).
        Only selected fields are yielded with the caveat that the keys
        are always selected."""
        keys = entity.entity_set.keys
        for k, v in entity.data_items():
            source_path = (self.entity_set.name, k)
            if (k in keys or entity.is_selected(k)):
                if isinstance(v, edm.SimpleValue):
                    yield self._mangle_name(source_path, prefix), v
                else:
                    for sub_path, fv in self._complex_field_generator(v):
                        source_path = tuple([self.entity_set.name, k] +
                                            sub_path)
                        yield self._mangle_name(source_path, prefix), fv

    def update_fields(self, entity):
        """A generator for updating mangled property names and values.

        entity
            Any instance of :py:class:`~pyslet.odata2.csdl.Entity`

        The yielded values are tuples of (mangled field name,
        :py:class:`~pyslet.odata2.csdl.SimpleValue` instance).

        Neither read only fields nor key fields are generated.

        For SQL variants that support default values on columns natively
        unselected items are suppressed and are returned instead in
        name-only form by :meth:`default_fields`.

        For SQL variants that don't support defaut values, unselected
        items are yielded here but with either the default value
        specified in the metadata schema definition of the corresponding
        property or as NULL.

        This method is used to implement OData's PUT semantics.  See
        :py:meth:`merge_fields` for an alternative."""
        keys = entity.entity_set.keys
        for k, v in entity.data_items():
            source_path = (self.entity_set.name, k)
            if k in keys or source_path in self.container.ro_names:
                continue
            if not entity.is_selected(k):
                if self.DEFAULT_VALUE:
                    continue
                else:
                    v.set_default_value()
            if isinstance(v, edm.SimpleValue):
                yield self._mangle_name(source_path, prefix=False), v
            else:
                for sub_path, fv in self._complex_field_generator(v):
                    source_path = tuple([self.entity_set.name, k] +
                                        sub_path)
                    yield self._mangle_name(source_path, prefix=False), fv

    def merge_fields(self, entity):
        """A generator for merging mangled property names and values.

        entity
            Any instance of :py:class:`~pyslet.odata2.csdl.Entity`

        The yielded values are tuples of (mangled field name,
        :py:class:`~pyslet.odata2.csdl.SimpleValue` instance).

        Neither read only fields, keys nor unselected fields are
        generated. All other fields are yielded implementing OData's
        MERGE semantics.  See
        :py:meth:`update_fields` for an alternative."""
        keys = entity.entity_set.keys
        for k, v in entity.data_items():
            source_path = (self.entity_set.name, k)
            if (k in keys or
                    source_path in self.container.ro_names or
                    not entity.is_selected(k)):
                continue
            if isinstance(v, edm.SimpleValue):
                yield self._mangle_name(source_path, prefix=False), v
            else:
                for sub_path, fv in self._complex_field_generator(v):
                    source_path = tuple([self.entity_set.name, k] +
                                        sub_path)
                    yield self._mangle_name(source_path, prefix=False), fv

    def default_fields(self, entity):
        """A generator for mangled property names.

        entity
            Any instance of :py:class:`~pyslet.odata2.csdl.Entity`

        The yielded values are the mangled field names that should be
        set to default values.  Neither read only fields, keys nor
        selected fields are generated."""
        if not self.DEFAULT_VALUE:
            # don't yield anything
            return
        keys = entity.entity_set.keys
        for k, v in entity.data_items():
            source_path = (self.entity_set.name, k)
            if (k in keys or
                    source_path in self.container.ro_names or
                    entity.is_selected(k)):
                continue
            if isinstance(v, edm.SimpleValue):
                yield self._mangle_name(source_path, prefix=False)
            else:
                for sub_path, fv in self._complex_field_generator(v):
                    source_path = tuple([self.entity_set.name, k] +
                                        sub_path)
                    yield self._mangle_name(source_path, prefix=False)

    def _complex_field_generator(self, ct):
        for k, v in ct.iteritems():
            if isinstance(v, edm.SimpleValue):
                yield [k], v
            else:
                for source_path, fv in self._complex_field_generator(v):
                    yield [k] + source_path, fv

    def stream_field(self, entity, prefix=True):
        """Returns information for selecting the stream ID.

        entity
            Any instance of :py:class:`~pyslet.odata2.csdl.Entity`

        Returns a tuples of (mangled field name,
        :py:class:`~pyslet.odata2.csdl.SimpleValue` instance)."""
        source_path = (self.entity_set.name, '_value')
        return self._mangle_name(source_path, prefix), \
            edm.EDMValue.from_type(edm.SimpleType.Int64)

    SQLBinaryExpressionMethod = {}
    SQLCallExpressionMethod = {}

    def sql_expression(self, expression, params, context="AND"):
        """Converts an expression into a SQL expression string.

        expression
                A :py:class:`pyslet.odata2.core.CommonExpression` instance.

        params
                A :py:class:`SQLParams` object of the appropriate type for
                this database connection.

        context
                A string containing the SQL operator that provides the
                context in which the expression is being converted, defaults
                to 'AND'. This is used to determine if the resulting
                expression must be bracketed or not.  See
                :py:meth:`sql_bracket` for a useful utility function to
                illustrate this.

        This method is basically a grand dispatcher that sends calls to
        other node-specific methods with similar signatures.  The effect
        is to traverse the entire tree rooted at *expression*.

        The result is a string containing the parameterized expression
        with appropriate values added to the *params* object *in the same
        sequence* that they appear in the returned SQL expression.

        When creating derived classes to implement database-specific
        behaviour you should override the individual evaluation methods
        rather than this method.  All related methods have the same
        signature.

        Where methods are documented as having no default implementation,
        NotImplementedError is raised."""
        if isinstance(expression, core.UnaryExpression):
            raise NotImplementedError
        elif isinstance(expression, core.BinaryExpression):
            return getattr(
                self,
                self.SQLBinaryExpressionMethod[
                    expression.operator])(
                expression,
                params,
                context)
        elif isinstance(expression, UnparameterizedLiteral):
            return self.container.ParamsClass.escape_literal(
                to_text(expression.value))
        elif isinstance(expression, core.LiteralExpression):
            return params.add_param(
                self.container.prepare_sql_value(
                    expression.value))
        elif isinstance(expression, core.PropertyExpression):
            try:
                p = self.entity_set.entityType[expression.name]
                if isinstance(p, edm.Property):
                    if p.complexType is None:
                        field_name = self.container.mangled_names[
                            (self.entity_set.name, expression.name)]
                        return "%s.%s" % (self.table_name, field_name)
                    else:
                        raise core.EvaluationError(
                            "Unqualified property %s "
                            "must refer to a simple type" %
                            expression.name)
            except KeyError:
                raise core.EvaluationError(
                    "Property %s is not declared" % expression.name)
        elif isinstance(expression, core.CallExpression):
            return getattr(
                self,
                self.SQLCallExpressionMethod[
                    expression.method])(
                expression,
                params,
                context)

    def sql_bracket(self, query, context, operator):
        """A utility method for bracketing a SQL query.

        query
                The query string

        context
                A string representing the SQL operator that defines the
                context in which the query is to placed.  E.g., 'AND'

        operator
                The dominant operator in the query.

        This method is used by operator-specific conversion methods.
        The query is not parsed, it is merely passed in as a string to be
        bracketed (or not) depending on the values of *context* and
        *operator*.

        The implementation is very simple, it checks the precedence of
        *operator* in *context* and returns *query* bracketed if
        necessary::

                collection.sql_bracket("Age+3","*","+")=="(Age+3)"
                collection.sql_bracket("Age*3","+","*")=="Age*3" """
        if SQLOperatorPrecedence[context] > SQLOperatorPrecedence[operator]:
            return "(%s)" % query
        else:
            return query

    def sql_expression_member(self, expression, params, context):
        """Converts a member expression, e.g., Address/City

        This implementation does not support the use of navigation
        properties but does support references to complex properties.

        It outputs the mangled name of the property, qualified by the
        table name."""
        name_list = self._calculate_member_field_name(expression)
        context_def = self.entity_set.entityType
        depth = 0
        table_name = self.table_name
        entity_set = self.entity_set
        path = []
        for name in name_list:
            if context_def is None:
                raise core.EvaluationError("Property %s is not declared" %
                                           '/'.join(name_list))
            p = context_def[name]
            if isinstance(p, edm.Property):
                path.append(name)
                if p.complexType is not None:
                    context_def = p.complexType
                else:
                    context_def = None
            elif isinstance(p, edm.NavigationProperty):
                if depth > 0:
                    raise NotImplementedError(
                        "Member expression exceeds maximum navigation depth")
                else:
                    table_name = self.add_join(name)
                    context_def = p.to_end.entityType
                    depth += 1
                    path = []
                    entity_set = entity_set.get_target(name)
        # the result must be a simple property, so context_def must not be None
        if context_def is not None:
            raise core.EvaluationError(
                "Property %s does not reference a primitive type" %
                '/'.join(name_list))
        field_name = self.container.mangled_names[
            tuple([entity_set.name] + path)]
        return "%s.%s" % (table_name, field_name)

    def _calculate_member_field_name(self, expression):
        if isinstance(expression, core.PropertyExpression):
            return [expression.name]
        elif (isinstance(expression, core.BinaryExpression) and
                expression.operator == core.Operator.member):
            return (
                self._calculate_member_field_name(expression.operands[0]) +
                self._calculate_member_field_name(expression.operands[1]))
        else:
            raise core.EvaluationError("Unexpected use of member expression")

    def sql_expression_cast(self, expression, params, context):
        """Converts the cast expression: no default implementation"""
        raise NotImplementedError

    def sql_expression_generic_binary(
            self,
            expression,
            params,
            context,
            operator):
        """A utility method for implementing binary operator conversion.

        The signature of the basic :py:meth:`sql_expression` is extended
        to include an *operator* argument, a string representing the
        (binary) SQL operator corresponding to the expression object."""
        query = []
        query.append(
            self.sql_expression(expression.operands[0], params, operator))
        query.append(' ')
        query.append(operator)
        query.append(' ')
        query.append(
            self.sql_expression(expression.operands[1], params, operator))
        return self.sql_bracket(''.join(query), context, operator)

    def sql_expression_mul(self, expression, params, context):
        """Converts the mul expression: maps to SQL "*" """
        return self.sql_expression_generic_binary(
            expression,
            params,
            context,
            '*')

    def sql_expression_div(self, expression, params, context):
        """Converts the div expression: maps to SQL "/" """
        return self.sql_expression_generic_binary(
            expression,
            params,
            context,
            '/')

    def sql_expression_mod(self, expression, params, context):
        """Converts the mod expression: no default implementation"""
        raise NotImplementedError

    def sql_expression_add(self, expression, params, context):
        """Converts the add expression: maps to SQL "+" """
        return self.sql_expression_generic_binary(
            expression,
            params,
            context,
            '+')

    def sql_expression_sub(self, expression, params, context):
        """Converts the sub expression: maps to SQL "-" """
        return self.sql_expression_generic_binary(
            expression,
            params,
            context,
            '-')

    def sql_expression_lt(self, expression, params, context):
        """Converts the lt expression: maps to SQL "<" """
        return self.sql_expression_generic_binary(
            expression,
            params,
            context,
            '<')

    def sql_expression_gt(self, expression, params, context):
        """Converts the gt expression: maps to SQL ">" """
        return self.sql_expression_generic_binary(
            expression,
            params,
            context,
            '>')

    def sql_expression_le(self, expression, params, context):
        """Converts the le expression: maps to SQL "<=" """
        return self.sql_expression_generic_binary(
            expression,
            params,
            context,
            '<=')

    def sql_expression_ge(self, expression, params, context):
        """Converts the ge expression: maps to SQL ">=" """
        return self.sql_expression_generic_binary(
            expression,
            params,
            context,
            '>=')

    def sql_expression_isof(self, expression, params, context):
        """Converts the isof expression: no default implementation"""
        raise NotImplementedError

    def sql_expression_eq(self, expression, params, context):
        """Converts the eq expression: maps to SQL "=" """
        return self.sql_expression_generic_binary(
            expression,
            params,
            context,
            '=')

    def sql_expression_ne(self, expression, params, context):
        """Converts the ne expression: maps to SQL "<>" """
        return self.sql_expression_generic_binary(
            expression,
            params,
            context,
            '<>')

    def sql_expression_and(self, expression, params, context):
        """Converts the and expression: maps to SQL "AND" """
        return self.sql_expression_generic_binary(
            expression,
            params,
            context,
            'AND')

    def sql_expression_or(self, expression, params, context):
        """Converts the or expression: maps to SQL "OR" """
        return self.sql_expression_generic_binary(
            expression,
            params,
            context,
            'OR')

    def sql_expression_endswith(self, expression, params, context):
        """Converts the endswith function: maps to "op[0] LIKE '%'+op[1]"

        This is implemented using the concatenation operator"""
        percent = edm.SimpleValue.from_type(edm.SimpleType.String)
        percent.set_from_value("'%'")
        percent = UnparameterizedLiteral(percent)
        concat = core.CallExpression(core.Method.concat)
        concat.operands.append(percent)
        concat.operands.append(expression.operands[1])
        query = []
        query.append(
            self.sql_expression(expression.operands[0], params, 'LIKE'))
        query.append(" LIKE ")
        query.append(self.sql_expression(concat, params, 'LIKE'))
        return self.sql_bracket(''.join(query), context, 'LIKE')

    def sql_expression_indexof(self, expression, params, context):
        """Converts the indexof method: maps to POSITION( op[0] IN op[1] )"""
        query = ["POSITION("]
        query.append(self.sql_expression(expression.operands[0], params, ','))
        query.append(" IN ")
        query.append(self.sql_expression(expression.operands[1], params, ','))
        query.append(")")
        return ''.join(query)

    def sql_expression_replace(self, expression, params, context):
        """Converts the replace method: no default implementation"""
        raise NotImplementedError

    def sql_expression_startswith(self, expression, params, context):
        """Converts the startswith function: maps to "op[0] LIKE op[1]+'%'"

        This is implemented using the concatenation operator"""
        percent = edm.SimpleValue.from_type(edm.SimpleType.String)
        percent.set_from_value("'%'")
        percent = UnparameterizedLiteral(percent)
        concat = core.CallExpression(core.Method.concat)
        concat.operands.append(expression.operands[1])
        concat.operands.append(percent)
        query = []
        query.append(
            self.sql_expression(expression.operands[0], params, 'LIKE'))
        query.append(" LIKE ")
        query.append(self.sql_expression(concat, params, 'LIKE'))
        return self.sql_bracket(''.join(query), context, 'LIKE')

    def sql_expression_tolower(self, expression, params, context):
        """Converts the tolower method: maps to LOWER function"""
        return "LOWER(%s)" % self.sql_expression(
            expression.operands[0],
            params,
            ',')

    def sql_expression_toupper(self, expression, params, context):
        """Converts the toupper method: maps to UCASE function"""
        return "UPPER(%s)" % self.sql_expression(
            expression.operands[0],
            params,
            ',')

    def sql_expression_trim(self, expression, params, context):
        """Converts the trim method: maps to TRIM function"""
        return "TRIM(%s)" % self.sql_expression(
            expression.operands[0],
            params,
            ',')

    def sql_expression_substring(self, expression, params, context):
        """Converts the substring method

        maps to SUBSTRING( op[0] FROM op[1] [ FOR op[2] ] )"""
        query = ["SUBSTRING("]
        query.append(self.sql_expression(expression.operands[0], params, ','))
        query.append(" FROM ")
        query.append(self.sql_expression(expression.operands[1], params, ','))
        if len(expression.operands) > 2:
            query.append(" FOR ")
            query.append(
                self.sql_expression(expression.operands[2], params, ','))
        query.append(")")
        return ''.join(query)

    def sql_expression_substringof(self, expression, params, context):
        """Converts the substringof function

        maps to "op[1] LIKE '%'+op[0]+'%'"

        To do this we need to invoke the concatenation operator.

        This method has been poorly defined in OData with the parameters
        being switched between versions 2 and 3.  It is being withdrawn
        as a result and replaced with contains in OData version 4.  We
        follow the version 3 convention here of "first parameter in the
        second parameter" which fits better with the examples and with
        the intuitive meaning::

                substringof(A,B) == A in B"""
        percent = edm.SimpleValue.from_type(edm.SimpleType.String)
        percent.set_from_value("'%'")
        percent = UnparameterizedLiteral(percent)
        rconcat = core.CallExpression(core.Method.concat)
        rconcat.operands.append(expression.operands[0])
        rconcat.operands.append(percent)
        lconcat = core.CallExpression(core.Method.concat)
        lconcat.operands.append(percent)
        lconcat.operands.append(rconcat)
        query = []
        query.append(
            self.sql_expression(expression.operands[1], params, 'LIKE'))
        query.append(" LIKE ")
        query.append(self.sql_expression(lconcat, params, 'LIKE'))
        return self.sql_bracket(''.join(query), context, 'LIKE')

    def sql_expression_concat(self, expression, params, context):
        """Converts the concat method: maps to ||"""
        query = []
        query.append(self.sql_expression(expression.operands[0], params, '*'))
        query.append(' || ')
        query.append(self.sql_expression(expression.operands[1], params, '*'))
        return self.sql_bracket(''.join(query), context, '*')

    def sql_expression_length(self, expression, params, context):
        """Converts the length method: maps to CHAR_LENGTH( op[0] )"""
        return "CHAR_LENGTH(%s)" % self.sql_expression(
            expression.operands[0],
            params,
            ',')

    def sql_expression_year(self, expression, params, context):
        """Converts the year method: maps to EXTRACT(YEAR FROM op[0])"""
        return "EXTRACT(YEAR FROM %s)" % self.sql_expression(
            expression.operands[0],
            params,
            ',')

    def sql_expression_month(self, expression, params, context):
        """Converts the month method: maps to EXTRACT(MONTH FROM op[0])"""
        return "EXTRACT(MONTH FROM %s)" % self.sql_expression(
            expression.operands[0],
            params,
            ',')

    def sql_expression_day(self, expression, params, context):
        """Converts the day method: maps to EXTRACT(DAY FROM op[0])"""
        return "EXTRACT(DAY FROM %s)" % self.sql_expression(
            expression.operands[0],
            params,
            ',')

    def sql_expression_hour(self, expression, params, context):
        """Converts the hour method: maps to EXTRACT(HOUR FROM op[0])"""
        return "EXTRACT(HOUR FROM %s)" % self.sql_expression(
            expression.operands[0],
            params,
            ',')

    def sql_expression_minute(self, expression, params, context):
        """Converts the minute method: maps to EXTRACT(MINUTE FROM op[0])"""
        return "EXTRACT(MINUTE FROM %s)" % self.sql_expression(
            expression.operands[0],
            params,
            ',')

    def sql_expression_second(self, expression, params, context):
        """Converts the second method: maps to EXTRACT(SECOND FROM op[0])"""
        return "EXTRACT(SECOND FROM %s)" % self.sql_expression(
            expression.operands[0],
            params,
            ',')

    def sql_expression_round(self, expression, params, context):
        """Converts the round method: no default implementation"""
        raise NotImplementedError

    def sql_expression_floor(self, expression, params, context):
        """Converts the floor method: no default implementation"""
        raise NotImplementedError

    def sql_expression_ceiling(self, expression, params, context):
        """Converts the ceiling method: no default implementation"""
        raise NotImplementedError

