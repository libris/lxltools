#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Transfers records from old style pgsql tables to the new model.

import argparse
import psycopg2
import json
from lddb import ld
from datetime import datetime


def transfer(**args):
    con = psycopg2.connect(database=args['database'], user=args['user'], host=args['host'])
    readcur = con.cursor()
    writecur = con.cursor()

    query = "SELECT identifier,data,entry,meta FROM %s where not entry @> '{\"deleted\":true}'" % args['fromtable']

    readcur.execute(query)
    print("Query executed, start reading rows.")

    counter = 0
    while True:
        results = readcur.fetchmany(2000)

        if not results:
            break

        values = []

        for row in results:
            counter += 1
            try:
                identifier = row[0]
                data = ld.flatten(json.loads(bytes(row[1]).decode("utf-8")))
                entry = row[2]
                entry['extraData'] = row[3]
                deleted = entry.get('deleted', False)
                created = datetime.fromtimestamp(int(round(entry.get('created')/1000)))
                ts = datetime.fromtimestamp(int(round(entry.get('modified')/1000)))
                entry.pop('timestamp')

                values.append((identifier, json.dumps(data), json.dumps(entry), created, ts, deleted))

            except Exception as e:
                print("Failed to convert row {0} to json".format(row[0]), e)
                raise

            arg_str = ",".join(bytes(writecur.mogrify("(%s,%s,%s,%s,%s,%s)", x)).decode("utf-8") for x in values)
            writecur.execute("INSERT INTO "+args['totable']+" (id,data,entry,created,modified,deleted) VALUES " + arg_str)
            values = []
        con.commit()


    print("All {0} rows read.".format(counter))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Transfers to LDDB')
    parser.add_argument('--database', help='The name of the postgresql database schema. Defaults to "whelk"', default='whelk')
    parser.add_argument('--user', help='Username for the postgresql database. Defaults to "whelk"', default='whelk')
    parser.add_argument('--host', help='The postgresql host to connect to. Defaults to "localhost"', default='localhost')
    parser.add_argument('--password', help='Password for the postgresql database.')
    parser.add_argument('--fromtable', help='Which table to read from', required=True)
    parser.add_argument('--totable', help='Which table to save to', required=True)

    args = vars(parser.parse_args())

    transfer(**args)


