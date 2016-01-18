import os
import sys
import json
import logging
from .storage import Storage
from .ld.keys import *


logger = logging.getLogger(__name__)


def load_datasets(storage, datasets):
    i = 0
    for dataset in datasets:
        for doc in get_documents(dataset):
            doc_id = get_doc_id(doc)
            if not doc_id:
                logger.warn("Found no doc ID in %s", dataset)
                continue
            logger.info("[%s] Loading %s", i, doc_id)
            storage.store(doc_id, doc)
            i += 1

def get_documents(source):
    if not os.path.isfile(source):
        logger.warn("DB source %s does not exist", source)
        return
    logger.debug("Loading lines from: %s", source)
    with open(source) as f:
        if source.endswith('.lines'):
            for l in f:
                yield json.loads(l)
        else:
            yield json.load(f)

def get_doc_id(doc):
    if 'descriptions' in doc:
        return doc['descriptions']['entry'][ID]
    elif GRAPH in doc:
        graph = doc[GRAPH]
        if isinstance(graph, list):
            return graph[0][ID]
        else:
            return graph[ID]
    else:
        return doc[ID]


if __name__ == '__main__':
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    import argparse
    argp = argparse.ArgumentParser()
    argp.add_argument('-n', '--db-name')
    argp.add_argument('-H', '--host')
    argp.add_argument('-u', '--user')
    argp.add_argument('-p', '--password')
    argp.add_argument('--setup', action='store_true', help="Create tables and indexes")
    argp.add_argument('datasets', metavar='DATASET', nargs='*')
    args = argp.parse_args()

    if not args.db_name:
        argp.print_usage()
        sys.exit(2)

    storage = Storage('lddb', args.db_name, args.host, args.user, args.password)
    if args.setup:
        storage.setup('tables')
        storage.setup('indexes')
    load_datasets(storage, args.datasets)
