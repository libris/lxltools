from __future__ import unicode_literals, print_function
__metaclass__ = type
import argparse
from collections import OrderedDict
try:
    from pathlib import Path
except ImportError:
    from pathlib2 import Path
from urlparse import urlparse, urljoin
import urllib2
import sys
import json
import csv
import time

from rdflib import ConjunctiveGraph, Graph, RDF, URIRef
from rdflib_jsonld.serializer import from_rdf
from rdflib_jsonld.parser import to_rdf

from . import lxlslug


class Compiler:

    def __init__(self,
                 base_dir=None,
                 dataset_id=None,
                 context=None,
                 record_thing_link='mainEntity',
                 system_iri_base=None,
                 union='all.jsonld.lines'):
        self.datasets = {}
        self.base_dir = Path(base_dir)
        self.dataset_id = dataset_id
        self.system_iri_base = system_iri_base
        self.record_thing_link = record_thing_link
        self.context = context
        self.cachedir = None
        self.union = union

    def main(self):
        argp = argparse.ArgumentParser(
                description="Available datasets: " + ", ".join(self.datasets),
                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        arg = argp.add_argument
        arg('-o', '--outdir', type=str, default=self.path("build"), help="Output directory")
        arg('-c', '--cache', type=str, default=self.path("cache"), help="Cache directory")
        arg('-l', '--lines', action='store_true',
                help="Output a single file with one JSON-LD document per line")
        arg('datasets', metavar='DATASET', nargs='*')

        args = argp.parse_args()
        if not args.datasets and args.outdir:
            args.datasets = list(self.datasets)

        self._configure(args.outdir, args.cache, use_union=args.lines)
        self._run(args.datasets)

    def _configure(self, outdir, cachedir=None, use_union=False):
        self.outdir = Path(outdir)
        self.cachedir = cachedir
        if use_union:
            union_fpath = self.outdir / self.union
            union_fpath.parent.mkdir(parents=True, exist_ok=True)
            self.union_file = union_fpath.open('wb')
        else:
            self.union_file = None

    def _run(self, names):
        try:
            self._compile_datasets(names)
        finally:
            if self.union_file:
                self.union_file.close()

    def dataset(self, func):
        self.datasets[func.__name__] = func, True
        return func

    def handler(self, func):
        self.datasets[func.__name__] = func, False
        return func

    def path(self, pth):
        return self.base_dir / pth

    def to_jsonld(self, graph):
        return _to_jsonld(graph,
                         ("../" + self.context, str(self.path(self.context))))

    def _compile_datasets(self, names):
        for name in names:
            build, as_dataset = self.datasets[name]
            if len(names) > 1:
                print("Dataset:", name)
            result = build()
            if as_dataset:
                base, created_time, data = result

                created_time, ms = created_time.rsplit('.', 1)
                if ms.endswith('Z'):
                    ms = ms[:-1]
                created_ms = int(time.mktime(time.strptime(created_time,
                                             "%Y-%m-%dT%H:%M:%S"))
                                 * 1000 + int(ms))

                if isinstance(data, Graph):
                    data = self.to_jsonld(data)

                context, resultset = _partition_dataset(urljoin(self.dataset_id, base), data)

                for key, node in resultset.items():
                    node = self._to_node_description(node,
                            created_ms,
                            dataset=self.dataset_id,
                            source='/dataset/%s' % name)
                    self.write(node, key)
            print()

    def _to_node_description(self, node, datasource_created_ms, dataset=None, source=None):
        # TODO: overhaul these? E.g. mainEntity with timestamp and 'datasource'.
        assert self.record_thing_link not in node

        #print(dataset, source)

        def faux_offset(s):
            return sum(ord(c) * ((i+1) ** 2)  for i, c in enumerate(s))

        created_ms = datasource_created_ms + faux_offset(node['@id'])
        slug = lxlslug.librisencode(created_ms, lxlslug.checksum(node['@id']))

        record_id = self.system_iri_base + slug
        record = OrderedDict()
        record['@id'] = record_id
        record[self.record_thing_link] = {'@id': node['@id']}
        #if dataset:
        #    node['inDataset'] = {'@id': dataset}
        #if source:
        #    node['wasDerivedFrom'] = {'@id': source}

        items = [record, node]

        return {'@graph': items}

    def write(self, node, name):
        node_id = node.get('@id')
        if node_id:
            assert not node_id.startswith('_:')
        if self.union_file:
            print(json.dumps(node), file=self.union_file)
        # TODO: else: # don't write both to union_file and separate file
        pretty_repr = _serialize(node)
        if pretty_repr:
            outfile = self.outdir / ("%s.jsonld" % name)
            print("Writing:", outfile)
            outfile.parent.mkdir(parents=True, exist_ok=True)
            with outfile.open('wb') as fp:
                fp.write(pretty_repr)
        else:
            print("No data")

    def get_cached_path(self, url):
        return self.cachedir / urllib2.quote(url, safe="")

    def cache_url(self, url):
        path = self.get_cached_path(url)
        if not path.exists():
            with path.open('wb') as fp:
                r = urllib2.urlopen(url)
                while True:
                    chunk = r.read(1024 * 8)
                    if not chunk: break
                    fp.write(chunk)
        return path

    def cached_rdf(self, fpath):
        source = Graph()
        http = 'http://'
        if not self.cachedir:
            print("No cache directory configured", file=sys.stderr)
        elif fpath.startswith(http):
            remotepath = fpath
            fpath = self.cachedir / (remotepath[len(http):] + '.ttl')
            if not fpath.is_file():
                fpath.parent.mkdir(parents=True, exist_ok=True)
                source.parse(remotepath)
                source.serialize(str(fpath), format='turtle')
                return source
            else:
                return source.parse(str(fpath), format='turtle')
        source.parse(str(fpath))
        return source

    def load_json(self, fpath):
        with self.path(fpath).open() as fp:
            return json.load(fp)

    def read_csv(self, fpath, **kws):
        return _read_csv(self.path(fpath), **kws)

    def construct(self, sources, query=None):
        return _construct(self, sources, query)


def _serialize(data):
    if isinstance(data, (list, dict)):
        data = json.dumps(data, indent=2, sort_keys=True,
                separators=(',', ': '), ensure_ascii=False)
    if isinstance(data, unicode):
        data = data.encode('utf-8')
    return data


CSV_FORMATS = {'.csv': 'excel', '.tsv': 'excel-tab'}

def _read_csv(fpath, encoding='utf-8'):
    csv_dialect = CSV_FORMATS.get(fpath.suffix)
    assert csv_dialect
    with fpath.open('rb') as fp:
        reader = csv.DictReader(fp, dialect=csv_dialect)
        for item in reader:
            yield {k: v.decode(encoding).strip()
                            for (k, v) in item.items() if v}


def _construct(compiler, sources, query=None):
    dataset = ConjunctiveGraph()
    if not isinstance(sources, list):
        sources = [sources]
    for sourcedfn in sources:
        source = sourcedfn['source']
        graph = dataset.get_context(URIRef(sourcedfn.get('dataset') or source))
        if isinstance(source, (dict, list)):
            context_data = sourcedfn['context']
            if not isinstance(context_data, list):
                context_data = compiler.load_json(context_data )['@context']
            context_data = [compiler.load_json(ctx)['@context']
                            if isinstance(ctx, unicode) else ctx
                            for ctx in context_data]
            to_rdf(source, graph, context_data=context_data)
        elif isinstance(source, Graph):
            graph += source
        else:
            graph += compiler.cached_rdf(source)
    if not query:
        return graph
    with compiler.path(query).open() as fp:
        result = dataset.query(fp.read())
    g = Graph()
    for spo in result:
        g.add(spo)
    return g


def _to_jsonld(source, contextref, contextobj=None):
    contexturi, contextpath = contextref
    context = [contextpath, contextobj] if contextobj else contextpath
    data = from_rdf(source, context_data=context)
    data['@context'] = [contexturi, contextobj] if contextobj else contexturi
    _embed_singly_referenced_bnodes(data)
    return data


def _embed_singly_referenced_bnodes(data):
    graph_index = {item['@id']: item for item in data.pop('@graph')}
    bnode_refs = {}

    def collect_refs(node):
        for values in node.values():
            if not isinstance(values, list):
                values = [values]
            for value in values:
                if isinstance(value, dict):
                    if value.get('@id', '').startswith('_:'):
                        bnode_refs.setdefault(value['@id'], []).append(value)
                    collect_refs(value)

    for node in graph_index.values():
        collect_refs(node)

    for refid, refs in bnode_refs.items():
        if len(refs) == 1:
            refs[0].update(graph_index.pop(refid))
            refs[0].pop('@id')

    data['@graph'] = sorted(graph_index.values(), key=lambda node: node['@id'])


def _partition_dataset(base, data):
    resultset = OrderedDict()
    for node in data.pop('@graph'):
        nodeid = node['@id']
        # TODO: Absence caused by mismatch between external id and local mapping
        if not nodeid:
            print("Missing id for:", node)
            continue
        if not nodeid.startswith(base):
            print("Missing mapping of <%s> under base <%s>" % (nodeid, base))
            continue
        rel_path = urlparse(nodeid).path[1:]
        resultset[rel_path] = node
    return data.get('@context'), resultset
