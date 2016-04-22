# -*- coding: UTF-8 -*-
from __future__ import unicode_literals, print_function
__metaclass__ = type

from collections import OrderedDict, namedtuple
import re
from urllib import quote as url_quote

from .util import as_iterable
from .ld.keys import *
from .ld.frame import autoframe


MAX_LIMIT = 4000
DEFAULT_LIMIT = 200


class DataView:

    def __init__(self, vocab, storage, elastic, es_index):
        self.vocab = vocab
        self.storage = storage
        self.elastic = elastic
        self.es_index = es_index
        self.rev_limit = 4000
        self.chip_keys = {ID, TYPE, 'focus', 'mainEntity', 'sameAs'} | set(self.vocab.label_keys)
        self.reserved_parameters = ['q', 'limit', 'offset', 'p', 'o', 'value']

    def get_record_data(self, item_id):
        record = self.storage.get_record(item_id)
        return record.data if record else None

    def find_record_ids(self, item_id):
        record_ids = self.storage.find_record_ids(item_id)
        return list(record_ids)

    def find_same_as(self, item_id):
        # TODO: only get identifier
        records = self.storage.find_by_relation('sameAs', item_id, limit=1)
        if records:
            return records[0].identifier

    def get_search_results(self, req_args, make_find_url, site_base_uri=None):
        #s = req_args.get('s')
        p = req_args.get('p')
        o = req_args.get('o')
        value = req_args.get('value')
        #language = req_args.get('language')
        #datatype = req_args.get('datatype')
        q = req_args.get('q')
        limit, offset = self._get_limit_offset(req_args)
        if not isinstance(offset, (int, long)):
            offset = 0

        total = None
        records = []
        items = []
        stats = None
        page_params = {'p': p, 'o': o, 'value': value, 'q': q, 'limit': limit}

        def get_term_chip(termkey):
            termdfn = self.vocab.index.get(termkey)
            if not termdfn:
                return None
            return {ID: termdfn.get(ID) or termkey, 'label': termdfn['label']}

        mappings = [
            {
                'variable': 'q',
                'predicate': get_term_chip('textQuery'),
                'value': q
            }
        ]

        # TODO: unify find_by_relation and find_by_example, support the latter form here too
        if p:
            if o:
                records = self.storage.find_by_relation(p, o, limit, offset)
            elif value:
                records = self.storage.find_by_value(p, value, limit, offset)
            elif q:
                records = self.storage.find_by_query(p, q, limit, offset)
        elif o:
            records = self.storage.find_by_quotation(o, limit, offset)
        elif q and not p:
            # Search in elastic

            musts = [
                {"query_string": { "query": "{0}".format(q) }}
            ]

            for param, paramvalue in req_args.items():
                if param.startswith('_') or param in self.reserved_parameters:
                    continue
                musts.append({"match": {param: paramvalue}})
                page_params.setdefault(param, []).append(paramvalue)

                if param == TYPE or param.endswith(ID):
                    valueprop = 'object'
                    termkey = param[:-4]
                    value = {ID: paramvalue} # TODO: self.lookup(paramvalue, chip=True)
                else:
                    valueprop = 'value'
                    termkey = param
                    value = paramvalue

                termchip = get_term_chip(termkey)

                mappings.append({'variable': param, 'predicate': termchip, valueprop: value})

            dsl = {
                "query": {
                    "bool": {
                        "must": musts,
                    }
                }
            }
            if site_base_uri:
                dsl['query']['bool'].update(self._make_site_filter(site_base_uri))

            statstree = {'@type': []}
            if statstree:
                dsl["aggs"] = self.build_agg_query(statstree)

            # TODO: only ask ES for chip properties instead of post-processing
            es_results = self.elastic.search(body=dsl, size=limit, from_=offset,
                             index=self.es_index)
            hits = es_results.get('hits')
            total = hits.get('total')
            items = [self.to_chip(r.get('_source')) for r in
                     hits.get('hits')]
            if statstree:
                stats = self.build_stats(es_results, make_find_url, req_args)

        for rec in records:
            chip = self.to_chip(self.get_decorated_data(rec.data, include_quoted=False))
            items.append(chip)

        def ref(link): return {ID: link}

        results = OrderedDict({'@type': 'PartialCollectionView'})
        results['@id'] = make_find_url(offset=offset, **page_params)
        #results['itemsPerPage'] = limit
        #if total is not None:
        results['itemOffset'] = offset
        results['totalItems'] = total

        for mapping in mappings[1:]:
            params = page_params.copy()
            params.pop(mapping['variable'])
            mapping['up'] = {ID: make_find_url(offset=offset, **params)}
        results['search'] = {'mapping': mappings}

        results['value'] = value

        results['first'] = ref(make_find_url(**page_params))

        offsets = compute_offsets(total, limit, offset)

        results['last'] = ref(make_find_url(offset=offsets.last, **page_params))

        if offsets.prev is not None:
            if offsets.prev == 0:
                results['previous'] = results['first']
            else:
                results['previous'] = ref(make_find_url(offset=offsets.prev, **page_params))

        if offsets.next is not None:
            results['next'] = ref(make_find_url(offset=offsets.next, **page_params))

        # hydra:member
        results['items'] = items

        if stats:
            results['stats'] = stats

        return results

    def _get_limit_offset(self, args):
        limit = args.get('limit')
        offset = args.get('offset')
        if limit and limit.isdigit():
            limit = int(limit)
        if offset and offset.isdigit():
            offset = int(offset)
        return self.get_real_limit(limit), offset

    def get_real_limit(self, limit=None):
        return DEFAULT_LIMIT if limit is None or limit > MAX_LIMIT else limit

    def get_index_stats(self, slicetree, make_find_url, site_base_uri):
        slicetree = slicetree or {'@type':[]}
        dsl = {
            "size": 0,
            "query" : {},
            "aggs": self.build_agg_query(slicetree)
        }
        if site_base_uri:
            dsl['query']['bool'] = self._make_site_filter(site_base_uri)
        else:
            dsl['query']['match_all'] = {}

        results = self.elastic.search(body=dsl, size=dsl['size'],
                index=self.es_index)
        stats = self.build_stats(results, make_find_url, {'limit': self.get_real_limit()})

        return {TYPE: 'DataCatalog', ID: site_base_uri, 'statistics': stats}

    def build_agg_query(self, tree, size=1000):
        query = {}
        for key in tree:
            query[key] = {
                'terms': {'field': key, 'size': size}
            }
            if isinstance(tree, dict):
                query[key]['aggs'] = self.build_agg_query(tree[key], size)
        return query

    def build_stats(self, results, make_find_url, req_args):
        def add_slices(stats, aggregations, base):
            slice_map = {}

            for agg_key, agg in aggregations.items():
                observations= []
                slice_node = {
                    'dimension': agg_key.replace('.'+ID, ''),
                    'observation': observations
                }

                for bucket in agg['buckets']:
                    item_id = bucket.pop('key')
                    search_page_url = "{base}&{param}={value}".format(
                            base=base,
                            param=agg_key,
                            value=url_quote(item_id))

                    observation = {
                        'totalItems': bucket.pop('doc_count'),
                        'view': {ID: search_page_url},
                        'object': self.lookup(item_id)
                    }
                    observations.append(observation)

                    add_slices(observation, bucket, search_page_url)

                if observations:
                    slice_map[agg_key] = slice_node

            if slice_map:
                stats['sliceByDimension'] = slice_map

        stats = {}
        add_slices(stats, results['aggregations'],
                base=make_find_url(**req_args))

        return stats

    def _make_site_filter(self, site_base_uri):
        return {
            "should": [
                {"prefix" : {"@id": site_base_uri}},
                {"prefix" : {"sameAs.@id": site_base_uri}}
            ],
            "minimum_should_match": 1
        }

    def lookup(self, item_id):
        if item_id in self.vocab.index:
            return self.vocab.index[item_id]
        else:
            data = self.get_record_data(item_id)
            if data:
                return get_descriptions(data).entry if data else None
        return {ID: item_id, 'label': item_id}

    def find_ambiguity(self, request):
        kws = dict(request.args)
        rtype = kws.pop('type', None)
        q = kws.pop('q', None)
        if q:
            q = " ".join(q)
            #parts = _tokenize(q)
        example = {}
        if rtype:
            rtype = rtype[0]
            example['@type'] = rtype
        if q:
            example['label'] = q
        if kws:
            example.update({k: v[0] for k, v in kws.items()})

        def pick_thing(rec):
            for item in rec.data[GRAPH]:
                if rtype in as_iterable(item[TYPE]):
                    return item

        maybes  = [pick_thing(rec) #self.get_decorated_data(rec)
                   for rec in self.storage.find_by_example(example,
                           limit=MAX_LIMIT)]

        some_id = '%s?%s' % (request.path, request.query_string)
        item = {
            "@id": some_id,
            "@type": "Ambiguity",
            "label": q or ",".join(example.values()),
            "maybe": maybes
        }

        references = self._get_references_to(item)

        if not maybes and not references:
            return None

        return {GRAPH: [item] + references}

    def get_decorated_data(self, data, add_references=False, include_quoted=True):
        entry, other, quoted = get_descriptions(data)

        main_item = entry if entry else other.pop(0) if other else None
        main_id = main_item.get(ID) if main_item else None

        items = []
        if entry:
            items.append(entry)
            # TODO: fix this in source and/or handle in view
            #if 'prefLabel_en' in entry and 'prefLabel' not in entry:
            #    entry['prefLabel'] = entry['prefLabel_en']
        if other:
            items += other

        if quoted and include_quoted:
            unquoted = [dict(ngraph[GRAPH], quotedFromGraph={ID: ngraph.get(ID)})
                    for ngraph in quoted]
            items += unquoted

        framed = autoframe({GRAPH: items}, main_id)
        if framed:
            refs = self._get_references_to(main_item) if add_references else []
            # NOTE: workaround for autoframing frailties
            refs = [ref for ref in refs if ref[ID] != main_id]
            framed.update(autoframe({GRAPH: [{ID: main_id}] + refs}, main_id))
            return framed
        else:
            return data


    def getlabel(self, item):
        # TODO: get and cache chip for item (unless already quotedFrom)...
        return self.vocab.get_label_for(item) or ",".join(v for k, v in item.items()
                if k[0] != '@' and isinstance(v, unicode)) or item[ID]
                #or getlabel(self.get_chip(item[ID]))

    def to_chip(self, item, *keep_refs):
        return {k: v for k, v in item.items()
                if k in self.chip_keys or k.endswith('ByLang')
                   or has_ref(v, *keep_refs)}

    def _get_references_to(self, item):
        item_id = item[ID]
        # TODO: send choice of id:s to find_by_quotation?
        ids = [item_id]
        same_as = item.get('sameAs')
        if same_as:
            ids.append(same_as[0].get(ID))

        references = []
        for quoted_id in ids:
            if references:
                break
            for quoting in self.storage.find_by_quotation(quoted_id, limit=200):
                qdesc = get_descriptions(quoting.data)
                if quoted_id != item_id:
                    _fix_refs(item_id, quoted_id, qdesc)
                references.append(self.to_chip(qdesc.entry, item_id, quoted_id))
                for it in qdesc.items:
                    references.append(self.to_chip(it, item_id, quoted_id))

        return references


Descriptions = namedtuple('Descriptions', 'entry, items, quoted')

def get_descriptions(data):
    if 'descriptions' in data:
        return Descriptions(**data['descriptions'])
    elif GRAPH in data:
        items, quoted = [], []
        for item in data[GRAPH]:
            if GRAPH in item:
                quoted.append(item)
            else:
                items.append(item)
        entry = items.pop(0)
        return Descriptions(entry, items, quoted)
    else:
        return Descriptions(data, [], [])

# FIXME: quoted id:s are temporary and should be replaced with canonical id (or
# *at least* sameAs id) in stored data
def _fix_refs(real_id, ref_id, descriptions):
    entry, items, quoted = descriptions
    alias_map = {}
    for quote in quoted:
        item = quote[GRAPH]
        alias = item[ID]
        if alias == ref_id:
            alias_map[alias] = real_id
        else:
            for same_as in as_iterable(item.get('sameAs')):
                if same_as[ID] == ref_id:
                    alias_map[alias] = real_id

    _fix_ref(entry, alias_map)
    for item in items:
        _fix_ref(item, alias_map)

def _fix_ref(item, alias_map):
    for vs in item.values():
        for v in as_iterable(vs):
            if isinstance(v, dict):
                v_id = v.get(ID)
                if isinstance(v_id, list):
                    # WARN: "Encountered array as ID value
                    v_id = v[ID] = v_id[0]

                mapped = alias_map.get(v_id)
                if mapped:
                    v[ID] = mapped


def has_ref(vs, *refs):
    """
    >>> has_ref({ID: '/item'}, '/item')
    True
    >>> has_ref({ID: '/other'}, '/item')
    False
    >>> has_ref({ID: '/other'}, '/item', '/other')
    True
    >>> has_ref([{ID: '/item'}], '/item')
    True
    """
    for v in as_iterable(vs):
        if isinstance(v, dict) and v.get(ID) in refs:
            return True
    return False


def _tokenize(stuff):
    """
    >>> print(" ".join(_tokenize("One, Any (1911-)")))
    1911 any one
    """
    return sorted(set(
        re.sub(r'\W(?u)', '', part.lower(), flags=re.UNICODE)
        for part in stuff.split(" ")))


Offsets = namedtuple('Offsets', 'prev, next, last')

def compute_offsets(total, limit, offset):
    """
    >>> compute_offsets(total=52, limit=20, offset=0)
    Offsets(prev=None, next=20, last=40)

    >>> compute_offsets(total=52, limit=20, offset=20)
    Offsets(prev=0, next=40, last=40)

    >>> compute_offsets(total=52, limit=20, offset=40)
    Offsets(prev=20, next=None, last=40)

    >>> compute_offsets(total=50, limit=10, offset=40)
    Offsets(prev=30, next=None, last=40)
    """

    o_prev = offset - limit
    if o_prev < 0:
        o_prev = None

    o_next = offset + limit
    if o_next >= total:
        o_next = None
    elif not offset:
        o_next = limit

    if (offset + limit) >= total:
        o_last = offset
    else:
        o_last = total - (total % limit)

    return Offsets(o_prev, o_next, o_last)
