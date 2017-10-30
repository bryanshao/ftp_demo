"""
EXAMPLE USAGE:
import os
import requests
from parsekit.river import RiverClient
RIVER_API_KEY = os.getenv('RIVER_API_KEY')
RIVER_BASE_URL = os.getenv('RIVER_BASE_URL')
SCHEMA = [{'name':'col1', 'type':'string'},  # uses parsekit datatypes
          {'name':'col2', 'type':'numeric'}]
YEARS = range(2010, 2015)
def main():
    parse_name = 'test.example.river'
    # create river client
    river = RiverClient(RIVER_BASE_URL, RIVER_API_KEY)
    # create collection
    collection = river.create_collection(parse_name)
    for year in YEARS:
        datapath = '{}.{}'.format(parse_name, year)
        # create dataset
        dataset = collection.create_dataset(
            metadata={'datapath': datapath},
            schema=SCHEMA,
            name='example test river {}'.format(year),
            source={'format':'csv',
                    'header':True,
                    'null':""})
        # upload to S3
        dataset.upload_data('{}.csv'.format(year))
        # tell river that it can start copying data
        dataset.begin_copy()
if __name__ == '__main__':
    main()
"""

import json
import re
from urlparse import urljoin

import requests


class RiverServerError(Exception):
    pass


class RiverClient(object):
    """This class represents a connection to a River server."""

    def __init__(self, host, apikey):
        self.host = host
        self.api_key = apikey

    def _resource_url(self, *parts):
        """Generate a River URL."""
        return urljoin(self.host, '/'.join(parts))

    def __interpret_response(self, resp):
        if resp.status_code >= 400:
            error_msg = "Unknown server error: %r" % resp.text
            try:
                resp_body = resp.json()
                error_msg = resp_body.get('message', "Unknown server error.")
                if resp_body['detail']:
                    error_msg += "\ndetail:" + resp_body['detail']
            except ValueError:
                pass
            raise RiverServerError(error_msg)
        # decode the body, and return it
        resp_body = resp.json()
        return resp_body

    def __populate_header(self, headers):
        headers['content-type'] = 'application/json'
        headers['accept'] = 'application/json'
        headers['Authorization'] = 'Bearer ' + self.api_key
        return headers

    def get(self, *args, **kwargs):
        headers = kwargs.get('headers', {})
        kwargs['headers'] = self.__populate_header(headers)
        resp = requests.get(*args, **kwargs)
        return self.__interpret_response(resp)

    def put(self, *args, **kwargs):
        headers = kwargs.get('headers', {})
        kwargs['headers'] = self.__populate_header(headers)
        resp = requests.put(*args, **kwargs)
        return self.__interpret_response(resp)

    def post(self, *args, **kwargs):
        headers = kwargs.get('headers', {})
        kwargs['headers'] = self.__populate_header(headers)
        resp = requests.post(*args, **kwargs)
        return self.__interpret_response(resp)

    def delete(self, *args, **kwargs):
        headers = kwargs.get('headers', {})
        kwargs['headers'] = self.__populate_header(headers)
        resp = requests.delete(*args, **kwargs)
        return self.__interpret_response(resp)

    def create_collection(self, name):
        """Create a new collection on the server."""
        collection = RiverCollection(self, name)
        collection.create()
        return collection

    def collection_by_id(self, id_):
        collection_url = self._resource_url('v2/collections', id_)
        data = self.get(collection_url)

        collection = RiverCollection(self, **data)
        return collection


class RiverCollection(object):
    """
    This object represents a River collection. Most attributes are read only.
    """

    def __init__(self, server, name, id=None, created_by_user_id=None,
                 created_at=None, modified_at=None):
        self.server = server
        self._name = name
        self._id = id
        self._created_by_user_id = created_by_user_id
        self._created_at = created_at
        self._modified_at = modified_at

    @property
    def name(self):
        return self._name

    @property
    def id(self):
        return self._id

    @property
    def created_by_user_id(self):
        return self._created_by_user_id

    @property
    def created_at(self):
        return self._created_at

    @property
    def modified_at(self):
        return self._modified_at

    def create(self):
        # create a collection
        data = {'name': self._name}
        collections_url = self.server._resource_url('v2/collections/')
        collection = self.server.post(collections_url, data=json.dumps(data))
        keys = ['name', 'id', 'created_by_user_id', 'created_at', 'modified_at']
        for key in keys:
            setattr(self, '_'+key, collection[key])

    def create_dataset(self, **kwargs):
        kwargs['collection_id'] = self._id
        dataset = RiverDataset(self.server, **kwargs)
        dataset.create()
        return dataset

    @property
    def datasets(self):
        datasets_url = self.server._resource_url('v2/collections', self._id, 'datasets/')
        datasets_data = self.server.get(datasets_url)
        datasets = []
        for dataset_data in datasets_data:
            dataset = RiverDataset(self.server, **dataset_data)
            datasets.append(dataset)
        return datasets

    def __repr__(self):
        return "RiverCollection(id={0}, name={1})".format(self._id, self._name)


class RiverDataset(object):

    def __init__(self, server, **kwargs):
        self.server = server
        # fields needed for API
        self.archived_at = kwargs.get('archived_at', None)
        self.collection_id = kwargs.get('collection_id', None)
        self.created_at = kwargs.get('created_at', None)
        self.created_by_user_id = kwargs.get('created_by_user_id', None)
        self.delta_dataset_id = kwargs.get('delta_dataset_id', None)
        self.error_msg = kwargs.get('error_msg', None)
        self.id = kwargs.get('id', None)
        self.metadata = kwargs.get('metadata', {})
        self.modified_at = kwargs.get('modified_at', None)
        self.name = kwargs.get('name', self.metadata.get('datapath'))
        self.schema = kwargs.get('schema', [])
        self.source = kwargs.get('source', {
            'format': 'csv',
            'header': False,
        })
        self.state = kwargs.get('state', "awaiting_upload")
        self.table = kwargs.get('table', None)

        self._source_url = None
        self._dataset_url = None

        # if there already is an id, don't do checks
        if self.id:
            return

        datapath = self.metadata.get('datapath')
        if not datapath:
            raise ValueError("datapath must not be empty")
        if re.match(r'([a-z0-9\-\.]+)', datapath) is None:
            raise ValueError("datapath must be composed of chars: [a-z0-9\\-\\.]")
        if not self.schema:
            raise ValueError("River datasets must include a schema")

    @property
    def dataset_url(self):
        if self.id is None:
            raise Exception()
        if self._dataset_url is None:
            self._dataset_url = self.server._resource_url(
                'v2/collections',
                self.collection_id,
                'datasets',
                self.id)
        return self._dataset_url

    @property
    def upload_url(self):
        return self._source_url

    def create(self):
        """Create a new dataset on the server."""
        datasets_url = self.server._resource_url(
            'v2/collections',
            self.collection_id,
            'datasets/')
        data = {'name': self.name,
                'metadata': self.metadata,
                'schema': self.schema,
                'source': self.source}
        if self.delta_dataset_id:
            data['delta_dataset_id'] = self.delta_dataset_id

        dataset = self.server.post(datasets_url, data=json.dumps(data))
        # hydrate self
        for key in dataset:
            setattr(self, key, dataset[key])

        self._source_url = self.source['upload_url']
        # delete River-generated upload opportunity
        del self.source['upload_url']

    def save(self):
        """Save the dataset's attributes on the server with a PUT."""
        data = self.serialize()
        self.server.put(self.dataset_url, data=json.dumps(data))

    def load(self):
        dataset = self.server.get(self.dataset_url)
        # hydrate self
        for key in dataset:
            setattr(self, key, dataset[key])

    def begin_copy(self):
        """
        Notify River server that the upload finished by setting the source_url.
        """
        self.source['url'] = self._source_url
        self.save()

    def delete(self):
        self.server.delete(self._dataset_url)

    def upload_data(self, filepath):
        with open(filepath, 'rb') as fh:
            resp = requests.put(self.upload_url, fh,
                                headers={'Expect': '100-continue'})
        resp.raise_for_status()

    def serialize(self):
        """
        Return a dict version of the dataset. Only include fields needed for
        the River API.
        """
        keys = [
            'archived_at',
            'collection_id',
            'created_at',
            'created_by_user_id',
            'delta_dataset_id',
            'error_msg',
            'id',
            'metadata',
            'modified_at',
            'name',
            'schema',
            'source',
            'state',
            'table'
        ]
        data = {}
        for key in keys:
            data[key] = getattr(self, key)
        return data
