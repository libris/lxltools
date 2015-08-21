# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import psycopg2
import json
import hashlib
from datetime import datetime
import collections


MAX_LIMIT = 1000
DEFAULT_LIMIT = 100


class Storage:

    def __init__(self, base_table, database, host, user, password):
        self.connection = psycopg2.connect(database=database, host=host,
                user=user, password=password)
        self.tname = base_table
        self.vtname = "{0}__versions".format(base_table)
        self.versioning = True


    # Load-methods

    def load(self, identifier):
        """
        Returns a tuple containing the records identifier, data and manifest.
        """
        cursor = self.connection.cursor()
        cursor.execute("""
                SELECT id,data,manifest,created,modified FROM {0}
                WHERE id = '{1}'
                """.format(self.tname, identifier))
        result = cursor.fetchone()
        self.connection.commit()
        if result:
            return self._rule_2(result)
        return None

    def _assemble_result_list(self, results):
        for result in results:
            yield self._rule_2(result)

    def _rule_2(self, result):
        (identifier, data, manifest, created, modified) = result
        # Apply rule no 2!
        created = created.isoformat()
        modified = modified.isoformat()
        data['created'] = created
        data['modified'] = modified
        manifest['created'] = created
        manifest['modified'] = modified
        return (identifier, data, manifest)

    def load_thing(self, identifier):
        """
        Finds the primary record decribing a thing. Returns a tuple containing
        identifier, data and manifest.
        """
        cursor = self.connection.cursor()
        id_query = '[{"@id": "%s"}]' % identifier

        sql = """
            SELECT id,data,manifest,created,modified FROM {0}
            WHERE data->'descriptions'->'items' @> %(id_query)s
            """.format(self.tname)
        cursor.execute(sql, {'id_query': id_query})
        #result = list(self._assemble_result_list(cursor))
        result = cursor.fetchone()
        self.connection.commit()
        if result:
            return self._rule_2(result)
        else:
            return None

    def load_by_relation(self, rel, ref, limit=None, offset=None):
        limit = limit if limit is not None and limit < MAX_LIMIT else DEFAULT_LIMIT
        offset = offset if offset is not None else 0
        cursor = self.connection.cursor()
        ref_query = '{"%s": {"@id": "%s"}}' % (rel, ref)
        refs_query = '{"%s": [{"@id": "%s"}]}' % (rel, ref)
        sql = """
            SELECT id,data,manifest,created,modified FROM {0}
            WHERE data->'descriptions'->'entry' @> %(ref_query)s
                OR data->'descriptions'->'entry' @> %(refs_query)s
                OR data->'descriptions'->'items' @> %(set_ref_query)s
                OR data->'descriptions'->'items' @> %(set_refs_query)s
            LIMIT {1} OFFSET {2}
            """.format(self.tname, limit, offset)
        keys = {'ref_query': ref_query, 'refs_query': refs_query,
                'set_ref_query': '[%s]' % ref_query, 'set_refs_query': '[%s]' % refs_query}
        print(keys)
        cursor.execute(sql, keys)
        result = list(self._assemble_result_list(cursor))
        self.connection.commit()
        return result

    def load_all_versions(self, identifier):
        cursor = self.connection.cursor()
        if self.versioning:
            sql = """
                SELECT id,data,manifest,created,modified FROM {0}
                WHERE id = %{identifier}s ORDER BY modified ASC
                """.format( self.vtname)
            cursor.execute(sql, {'identifier': identifier})
            result = list(self._assemble_result_list(cursor))
            self.connection.commit()
        else:
            result = load(identifier)
        return result

    def load_record_status(self, identifier):
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


    # Store methods

    def _calculate_checksum(self, data):
        return hashlib.md5(json.dumps(data, sort_keys=True).encode('utf-8')).hexdigest()

    def _store(self, cursor, identifier, data, manifest=None):
        data.pop('modified', None) # Shouldn't influence checksum
        data.pop('created', None)
        manifest = manifest or {}
        manifest['checksum'] = self._calculate_checksum(data)
        if self.versioning:
            insert_version_sql = """
                    INSERT INTO {0} (id,checksum,data,manifest)
                    SELECT %(identifier)s,%(checksum)s,%(data)s,%(manifest)s
                    WHERE NOT EXISTS (SELECT 1 FROM {0}
                    WHERE id = %(identifier)s AND checksum = %(checksum)s)
                    """.format(self.vtname)
            cursor.execute(insert_version_sql, {
                    'identifier': identifier,
                    'manifest': json.dumps(manifest),
                    'data': json.dumps(data),
                    'checksum': manifest['checksum']
                })
            print("Row count", cursor.rowcount)

        if not self.versioning or cursor.rowcount > 0:
            upsert = """
                    WITH upsert AS (UPDATE {0}
                                    SET data = %(data)s,
                                    modified = %(modified)s,
                                    manifest = %(manifest)s,
                                    deleted = %(deleted)s
                                    WHERE id = %(identifier)s RETURNING *)
                    INSERT INTO {0} (id, data, manifest, deleted)
                    SELECT %(identifier)s, %(data)s, %(manifest)s, %(deleted)s
                    WHERE NOT EXISTS (SELECT * FROM upsert)
                    """.format(self.tname)
            cursor.execute(upsert, {
                    'identifier': identifier,
                    'data': json.dumps(data),
                    'manifest': json.dumps(manifest),
                    'modified': datetime.now(),
                    'deleted': manifest.get('deleted', False),
                })
        return (identifier, data, manifest)

    def store(self, identifier, data, manifest=None):
        try:
            cursor = self.connection.cursor()
            (identifier, data, manifest) = self._store(cursor, identifier, data, manifest)
            self.connection.commit()

            # Load results from insert
            status = self.load_record_status(identifier)
            data['created'] = status['created']
            data['modified'] = status['modified']
        except Exception as e:
            print("Store failed. Rolling back.", e)
            self.connection.rollback()
            raise

        return data

    def bulk_store(self, items):
        try:
            cursor = self.connection.cursor()
            for item in items:
                self._store(cursor, item[0], item[1], item[2])

            self.connection.commit()
        except Exception as e:
            print("Store failed. Rolling back.", e)
            self.connection.rollback()
            raise e
