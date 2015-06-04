# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import psycopg2
import json
import hashlib
from datetime import datetime
import collections

class Storage:

    def __init__(self, base_table, database, host, user, password):
        self.connection = psycopg2.connect(database=database, user=user, host=host, password=password)
        self.tname = base_table
        self.vtname = "{0}__versions".format(base_table)
        self.versioning = True

    # Load-methods
    def load(self, identifier):
        """Returns a tuple containing the records identifier, data and entry."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT id,data,entry FROM {table} WHERE id = '{identifier}'".format(table=self.tname, identifier=identifier))
        result = cursor.fetchone()
        self.connection.commit()
        if result:
            return (result[0], result[1], result[2])
        return None

    def _assemble_result_list(self, results):
        for result in results:
            yield (result[0], result[1], result[2])

    def load_thing(self, identifier):
        """Finds record(s) decribing a thing. Returns a list of tuples containing identifier, data and entry."""
        cursor = self.connection.cursor()
        json_query = [{ "@id": identifier }]

        sql = "SELECT id,data,entry FROM "+self.tname+" WHERE data->'@graph' @> %(json)s"
        cursor.execute(sql, {'json': json.dumps(json_query)})
        result = list(self._assemble_result_list(cursor))
        self.connection.commit()
        return result

    def load_by_relation(self, relation, identifier):
        cursor = self.connection.cursor()
        json_query = [{relation:{ "@id": identifier }}]
        listed_json_query = [{relation:[{ "@id": identifier }]}]
        print("json_query", json.dumps(json_query))
        sql = "SELECT id,data,entry FROM "+self.tname+" WHERE data->'@graph' @> %(json)s OR data->'@graph' @> %(listed_json)s"
        cursor.execute(sql, {
                'json': json.dumps(json_query),
                'listed_json': json.dumps(listed_json_query)
            }
        )
        result = list(self._assemble_result_list(cursor))
        self.connection.commit()
        return result

    def load_all_versions(self, identifier):
        cursor = self.connection.cursor()
        if self.versioning:
            sql = "SELECT id,data,entry FROM "+self.vtname+" WHERE id = %{identifier}s ORDER BY ts ASC"
            cursor.execute(sql, {'identifier': identifier})
            result = list(self._assemble_result_list(cursor))
            self.connection.commit()
        else:
            result = load(identifier)
        return result


    # Store methods
    def store(self, identifier, data, entry):
        try:
            cursor = self.connection.cursor()
            if self.versioning:
                insert_version_sql = "INSERT INTO {version_table_name} (id,checksum,data,entry,ts) SELECT %(identifier)s,%(checksum)s,%(data)s,%(entry)s,%(ts)s WHERE NOT EXISTS (SELECT 1 FROM {version_table_name} WHERE id = %(identifier)s AND checksum = %(checksum)s)".format(version_table_name = self.vtname)
                cursor.execute(insert_version_sql, {
                        'identifier': identifier,
                        'entry': json.dumps(entry),
                        'data': json.dumps(data),
                        'checksum': entry['checksum'],
                        'ts': datetime.fromtimestamp(entry['modified']),
                    }
                )

            upsert = """WITH upsert AS (UPDATE {table_name} SET data = %(data)s, ts = %(ts)s, entry = %(entry)s, deleted = %(deleted)s
                WHERE id = %(identifier)s RETURNING *)
                INSERT INTO {table_name} (id, data, ts, entry, deleted) SELECT %(identifier)s, %(data)s, %(ts)s, %(entry)s, %(deleted)s
                WHERE NOT EXISTS (SELECT * FROM upsert)""".format(table_name = self.tname)
            cursor.execute(upsert, {
                    'identifier': identifier,
                    'data': json.dumps(data),
                    'entry': json.dumps(entry),
                    'ts': datetime.fromtimestamp(entry['modified']),
                    'deleted': entry.get('deleted', False),
                }
            )

            self.connection.commit()
        except Exception as e:
            print("Store failed. Rolling back.", e)
            self.connection.rollback()
            raise e


