__author__ = 'max'

from mock import MagicMock
from time import time


def create_mock_riak_object(key=None, update_time=None):
    riak_object = MagicMock()
    riak_object.key = key
    riak_object.exists = True
    riak_object.data = None
    riak_object.encoded_data = None
    riak_object.content_type = 'application/json'
    if not update_time:
        update_time = time()
    riak_object.last_modified = update_time
    riak_object.siblings = []
    riak_object.store.return_value = riak_object

    return riak_object


def create_mock_riak_bucket(data=None, exists=True, cache=None):
    if cache is None:
        cache = {}

    get_record = {}
    for key in cache:
        get_record[key] = create_mock_riak_object(key)
        get_record[key].data = cache[key]

    def get_side_effect(key):
        if key in riak_bucket._get_record:
            return riak_bucket._get_record[key]

        instance = create_mock_riak_object(key)
        instance.data = data
        instance.exists = exists
        riak_bucket._get_record[key] = instance
        return instance

    def new_side_effect(key=None, data=data, *args, **kwargs):
        if key is None:
            key = 'autogen_key'
        instance = get_side_effect(key)
        instance.data = data
        return instance

    def multiget_side_effect(keys):
        return [get_side_effect(k) for k in keys]

    riak_bucket = MagicMock()
    riak_bucket.get.side_effect = get_side_effect
    riak_bucket.new.side_effect = new_side_effect
    riak_bucket.multiget.side_effect = multiget_side_effect
    riak_bucket._get_record = get_record

    return riak_bucket


def create_mock_riak_client():
    riak_client = MagicMock()
    riak_type_mock = MagicMock()
    bucket = create_mock_riak_bucket()

    riak_client.bucket_type.return_value = riak_type_mock
    riak_type_mock.bucket.return_value = bucket
    riak_client.bucket.return_value = bucket

    return riak_client
