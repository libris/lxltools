# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import logging
from os import path as P
from datetime import datetime
import hashlib
import json
from collections import namedtuple

import psycopg2


logger = logging.getLogger(__name__)


class Storage:

    def __init__(self, base_table='lddb', database=None, host=None, user=None, password=None,
            get_connection=None):
        self._connection = None
        self.get_gonnection = get_connection or (
                lambda: psycopg2.connect(database=database, host=host,
                        user=user, password=password))
        self.tname = base_table
        self.vtname = "{0}__versions".format(base_table)
        self.versioning = True

    @property
    def connection(self):
        if not self._connection or self._connection.closed:
            self._connection = self.get_gonnection()
        return self._connection

    def disconnect(self):
        if self._connection:
            self._connection.close()

    # Load-methods

    def get_record(self, identifier):# -> Record
        import urlparse
        slug = urlparse.urlparse(identifier).path[1:]
        sql = """
            SELECT id, data, manifest, created, modified FROM {0}
            WHERE id = %(slug)s
                OR manifest->'identifiers' @> %(identifier)s
            """.format(self.tname)
        cursor = self.connection.cursor()
        cursor.execute(sql, {
                'slug': '"%s"' % slug,
                'identifier': '"%s"' % identifier})
        result = cursor.fetchone()
        if result:
            return self._inject_storage_data(result)
        return None

    def find_record_ids(self, identifier):
        """
        Get the record ids containing a description of the given identifier.
        """
        id_query = '{"@id": "%s"}' % identifier
        ids_query = '[%s]' % id_query
        sameas_query = '[{"sameAs": %s}]' % ids_query
        sql = """
            SELECT id FROM {0}
            WHERE manifest->'identifiers' @> %(identifier)s
                OR data->'@graph' @> %(ids_query)s
                OR data->'@graph' @> %(sameas_query)s
            """.format(self.tname)
        cursor = self.connection.cursor()
        cursor.execute(sql, {
                'identifier': '"%s"' % identifier,
                'ids_query': ids_query,
                'sameas_query': sameas_query
                })
        for rec_id, in cursor:
            yield rec_id

    def find_by_relation(self, rel, ref, limit=None, offset=None):
        ref_query = '{"%s": {"@id": "%s"}}' % (rel, ref)
        refs_query = '{"%s": [{"@id": "%s"}]}' % (rel, ref)
        where = """
            data->'@graph' @> %(set_ref_query)s
            OR data->'@graph' @> %(set_refs_query)s
            """
        keys = {'set_ref_query': '[%s]' % ref_query,
                'set_refs_query': '[%s]' % refs_query}
        return self._do_find(where, keys, limit, offset)

    def find_by_quotation(self, identifier, limit=None, offset=None):
        """
        Find records that reference the given identifier by quotation.
        """
        where = """
            quoted @> %(ref_query)s
            OR quoted @> %(sameas_query)s
            """
        keys = {'ref_query': '[{"@graph": {"@id": "%s"}}]' % identifier,
                'sameas_query': '[{"@graph": {"sameAs": [{"@id": "%s"}]}}]' % identifier}
        return self._do_find(where, keys, limit, offset)

    def find_by_value(self, p, value, limit=None, offset=None):
        value_query = '{"%s": "%s"}' % (p, value)
        values_query = '{"%s": ["%s"]}' % (p, value)
        where = """
            data->'@graph' @> %(set_value_query)s
            OR data->'@graph' @> %(set_values_query)s
            """
        keys = {'set_value_query': '[%s]' % value_query,
                'set_values_query': '[%s]' % values_query}
        return self._do_find(where, keys, limit, offset)

    def find_by_example(self, example, limit=None, offset=None):
        value_query = json.dumps(example, ensure_ascii=False, sort_keys=True)
        where = """
            data->'@graph' @> %(set_value_query)s
            """
        keys = {'set_value_query': '[%s]' % value_query}
        return self._do_find(where, keys, limit, offset)

    def find_by_query(self, p, q, limit=None, offset=None):
        # NOTE: ILIKE is *really* slow, if we keep this, index expected property queries
        where = """
            data->'descriptions'->'entry'->>%(p)s ILIKE %(q)s
            """
        keys = {'p': p, 'q': '%'+ q +'%'}
        return self._do_find(where, keys, limit, offset)

    def _do_find(self, where, keys, limit, offset):
        offset = offset or 0
        sql = """
            SELECT id, data, manifest, created, modified FROM {tname}
            WHERE {where}
            LIMIT {limit} OFFSET {offset}
        """.format(tname=self.tname, where=where, limit=limit, offset=offset)
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql, keys)
            result = list(self._assemble_result_list(cursor))
        finally:
            self.connection.commit()
        return result

    def get_all_versions(self, identifier):
        cursor = self.connection.cursor()
        if self.versioning:
            sql = """
                SELECT id, data, manifest, created, modified FROM {0}
                WHERE id = %{identifier}s ORDER BY modified ASC
                """.format( self.vtname)
            cursor.execute(sql, {'identifier': identifier})
            result = list(self._assemble_result_list(cursor))
            self.connection.commit()
        else:
            result = self.get_record(identifier)
        return result

    def _inject_storage_data(self, result):
        """
        Manifested columns such as timestamps aren't redundantly stored within
        the dynamic data. This method injects those details into the result.
        """
        (identifier, data, created, modified) = result
        created = created.isoformat()
        modified = modified.isoformat()
        manifest['created'] = created
        manifest['modified'] = modified
        return Record(identifier, data, manifest)

    def _assemble_result_list(self, results):
        for result in results:
            yield self._inject_storage_data(result)

    def get_record_status(self, identifier):
        cursor = self.connection.cursor()
        sql = """
            SELECT id,created,modified,deleted FROM {0}
            WHERE id = %(identifier)s
            """.format(self.tname)
        cursor.execute(sql, {'identifier': identifier})
        result = cursor.fetchone()
        self.connection.commit()
        if result:
            return {
                'exists': True,
                'created': result[1],
                'modified': result[2],
                'deleted': result[3]
            }
        return {'exists': False}


Record = namedtuple('Record', 'identifier, data, manifest')
