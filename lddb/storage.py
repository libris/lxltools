# -*- coding: utf-8 -*-

import psycopg2

class Storage:
    def __init__(self, database, host, user, password):
        self.connection = psycopg2.connect(database=database, user=user, host=host, password=password)



    def load(self, identifier, store):
        """Returns a tuple containing the records identifier, data and entry."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT identifier,data,entry FROM {table} WHERE identifier = '{identifier}'".format(table=store, identifier=identifier))
        result = cursor.fetchone()
        if result:
            return (result[0], result[1], result[2])
        return None

    def _assemble_thing_list(self, results):
        for result in results:
            yield (result[0], result[1], result[2])

    def load_thing(self, identifier, store):
        """Finds record(s) decribing a thing. Returns a list of tuples containing identifier, data and entry."""
        cursor = self.connection.cursor()
        result = _assemble_thing_list(cursor.execute("SELECT identifier,data,entry FROM {table} WHERE data #> = '{graph,@id}' = '{identifier}'".format(table=store, identifier=identifier)))
        return result
