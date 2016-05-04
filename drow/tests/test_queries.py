__author__ = 'max'

from riak import RiakError
from unittest import TestCase
from mock import patch
from mock import MagicMock
import jsonpatch
from copy import deepcopy
from mockriak import create_mock_riak_client
from mockriak import create_mock_riak_object
from drow import models
from drow.errors import DoesNotExist
from drow.errors import InvalidPatch
from drow.queryset import validate_patch


def get_nonexistant_object(key):
    obj = create_mock_riak_object(key)
    obj.exists = False
    return obj


class FakeModelContext(object):
    def __enter__(self):
        self.patcher = patch.object(models, 'settings')
        settings = self.patcher.start()
        settings.RIAK_CLIENT = create_mock_riak_client()

        class MyModel(models.Model):
            class Meta:
                content_type = 'application/x.content'
                index = 'my_search_index'
                bucket_name = 'my_bucket'
                bucket_type_name = 'my_bucket_type'
                creation_validator = MagicMock()
                storage_validator = MagicMock()
                decoder = MagicMock()
                encoder = MagicMock()

        return MyModel, settings

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.patcher.stop()
        return False


class TestQuerySet(TestCase):
    def test_search(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            client = settings.RIAK_CLIENT
            bucket = client.bucket()

            long_key = 'd' * 100

            def search_side_effect(*args, **kwargs):
                return {
                    'num_found': 5,
                    'docs': [
                        {'_yz_rk': 'a'},
                        {'_yz_rk': 'b'},
                        {'_yz_rk': 'c'},
                        {'_yz_rk': long_key},
                        {'_yz_rk': 'a'}  # duplicate keys should be omitted
                    ]
                }

            bucket.search.side_effect = search_side_effect

            results = MyModel.objects.search('awesome_query', start=5, rows=30)
            self.assertEquals(len(repr(results)), 126)

            self.assertEqual(MyModel.objects._state.bucket, bucket)
            self.assertEqual(len(results), 4)
            self.assertEqual(results.num_found, 5)
            self.assertEqual(results.start, 5)
            self.assertIs(results.model, MyModel)
            self.assertEquals(results[0].key, 'a')

            keys = {r.key for r in results}
            self.assertIn('a', keys)
            self.assertIn('b', keys)
            self.assertIn('c', keys)
            self.assertIn(long_key, keys)

            bucket.search.assert_called_once_with(
                'awesome_query',
                index='my_search_index',
                start=5,
                rows=30
            )

            self.assertEqual(bucket.multiget.call_count, 1)

            def shorter_search_side_effect(*args, **kwargs):
                return {
                    'num_found': 5,
                    'docs': [
                        {'_yz_rk': 'a'},
                        {'_yz_rk': 'b'},
                        {'_yz_rk': 'c'},
                        {'_yz_rk': 'a'}  # duplicate keys should be omitted
                    ]
                }

            bucket.search.side_effect = shorter_search_side_effect

            # test non-truncated repr path
            repr(MyModel.objects.search('query'))

            def bad_search_side_effect(*args, **kwargs):
                raise RiakError('Query unsuccessful check the logs.')

            bucket.search.side_effect = bad_search_side_effect

            self.assertRaises(
                MyModel.SearchError, MyModel.objects.search, 'query')

            def bad_connection_side_effect(*args, **kwargs):
                raise RiakError('Authentication Error')

            bucket.search.side_effect = bad_connection_side_effect

            self.assertRaises(RiakError, MyModel.objects.search, 'query')

    def test_active_get(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            instance = MyModel.objects.get('abcd', active=True)
            self.assertTrue(instance._state.riak_object)
            self.assertEqual(instance.key, 'abcd')

    def test_not_exists(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            MyModel.objects._state.bucket.get.side_effect = \
                get_nonexistant_object

            with self.assertRaises(DoesNotExist):
                MyModel.objects.get('abcd', active=True)

            MyModel.objects.get('abcd')

    def test_lazy_get(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            instance = MyModel.objects.get('abcd')
            self.assertEqual(instance.key, 'abcd')
            self.assertFalse(instance._state.riak_object)

            # Activate lazy get
            instance.data
            self.assertTrue(instance._state.riak_object)

    def test_create(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            client = settings.RIAK_CLIENT
            bucket = client.bucket()

            data = {'a': 'a'}
            MyModel.objects.create(data)

            self.assertEqual(MyModel.objects._state.bucket, bucket)
            MyModel._meta.creation_validator.assert_called_once_with(data)
            bucket.new.assert_called_once_with(
                data=data,
                content_type='application/x.content'
            )
            MyModel._meta.storage_validator.assert_called_once_with(data)
            self.assertEqual(len(bucket._get_record), 1)
            self.assertEqual(
                bucket._get_record['autogen_key'].store.call_count, 1)

    @patch.object(models, 'settings')
    def test_no_validator(self, settings):
        """
        Assert no validation is not an error
        """
        settings.RIAK_CLIENT = create_mock_riak_client()

        class MyModel(models.Model):
            class Meta:
                bucket_name = 'test_bucket'
                bucket_type_name = 'test_type'

        MyModel.objects.create({'a': 'a'})

        MyModel.objects.put('hi', {'a': 'a'})

    def test_patch(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            client = settings.RIAK_CLIENT
            bucket = client.bucket()
            old_data = {
                'a': 'a'
            }
            new_data = {
                'b': 'b'
            }

            patch_data = jsonpatch.make_patch(old_data, new_data)

            old_get_side_effect = bucket.get.side_effect

            def get_with_data(key):
                instance = old_get_side_effect(key)
                instance.data = old_data
                return instance

            bucket.get.side_effect = get_with_data

            instance = MyModel('test_key')
            result = instance.patch(patch_data)

            self.assertEqual(MyModel._meta.storage_validator.call_count, 1)
            self.assertEqual(result._state.riak_object.store.call_count, 1)
            self.assertEqual(len(result.data), 1)
            self.assertEqual(result.data, new_data)

    def test_put(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            client = settings.RIAK_CLIENT
            bucket = client.bucket()

            data_to_store = {'a': 'a'}

            instance = MyModel('test_key')
            instance.data = data_to_store
            result = instance.save()

            bucket.get.assert_called_with('test_key')
            self.assertEqual(MyModel._meta.creation_validator.call_count, 1)
            self.assertEqual(bucket.get.call_count, 2)
            self.assertIs(result.data, data_to_store)
            MyModel._meta.storage_validator.assert_called_once_with(
                data_to_store)
            self.assertEqual(result._state.riak_object.store.call_count, 1)

    def test_put_for_non_existant_object(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            client = settings.RIAK_CLIENT
            bucket = client.bucket()
            bucket.get.side_effect = \
                get_nonexistant_object

            data_to_store = {'a': 'a'}

            instance = MyModel('test_key')
            instance.data = data_to_store
            result = instance.save()

            bucket.get.assert_called_with('test_key')
            MyModel._meta.creation_validator.called_once_with(data_to_store)
            self.assertEqual(bucket.get.call_count, 2)
            self.assertIs(result.data, data_to_store)
            MyModel._meta.storage_validator.assert_called_once_with(
                data_to_store)
            self.assertEqual(result._state.riak_object.store.call_count, 1)

    def test_bad_patch(self):
        # looks like jsonpatch/jsonpointer cannot handle unicode keys :/
        # bug report:
        # https://github.com/stefankoegl/python-json-pointer/issues/18
        old_data = {u'\xee': u'\xee'}
        new_data = deepcopy(old_data)
        patch = jsonpatch.make_patch({}, new_data)

        with self.assertRaises(InvalidPatch):
            validate_patch(patch, old_data)

        incomplete_patch = [{}]

        with self.assertRaises(InvalidPatch):
            validate_patch(incomplete_patch, old_data)

        # Non-unicode
        with self.assertRaises(InvalidPatch):
            validate_patch([{'path': '/ee', 'op': 'add'}], {'ee': 'ee'})

    def test_delete(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            instance = MyModel('to_be_deleted')
            instance.save()
            instance.delete()
            self.assertEqual(instance._state.riak_object.delete.call_count, 1)
