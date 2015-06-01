# -*- coding: utf-8 -*-

import psycopg2
import json
from datetime import datetime

class Storage:
    def __init__(self, base_table, database, host, user, password):
        self.connection = psycopg2.connect(database=database, user=user, host=host, password=password)
        self.tname = base_table

    def load(self, identifier):
        """Returns a tuple containing the records identifier, data and entry."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT identifier,data,entry FROM {table} WHERE identifier = '{identifier}'".format(table=self.tname, identifier=identifier))
        result = cursor.fetchone()
        if result:
            return (result[0], result[1], result[2])
        return None

    def _assemble_thing_list(self, results):
        for result in results:
            yield (result[0], result[1], result[2])

    def load_thing(self, identifier):
        """Finds record(s) decribing a thing. Returns a list of tuples containing identifier, data and entry."""
        cursor = self.connection.cursor()
        sql = "SELECT identifier,data,entry FROM "+self.tname+" WHERE data #> '{graph,@id}' = %(identifier)s"
        cursor.execute(sql, {'identifier': '"%s"' % identifier})
        return list(self._assemble_thing_list(cursor))

    def store(self, identifier, data, entry):
        cursor = self.connection.cursor()
        sql = """WITH upsert AS (UPDATE {table_name} SET data = %(data)s, ts = %(ts)s, entry = %(entry)s, deleted = %(deleted)s
            WHERE identifier = %(identifier)s RETURNING *)
            INSERT INTO {table_name} (identifier, data, ts, entry, deleted) SELECT %(identifier)s, %(data)s, %(ts)s, %(entry)s, %(deleted)s
            WHERE NOT EXISTS (SELECT * FROM upsert)""".format(table_name = self.tname)
        cursor.execute(sql, {
                'identifier': identifier,
                'data': json.dumps(data),
                'entry': json.dumps(entry),
                'ts': datetime.fromtimestamp(entry['modified']),
                'deleted': entry.get('deleted', False),
            }
        )

        self.connection.commit()
        cursor.close()
