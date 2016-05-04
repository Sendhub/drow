__author__ = 'max'

from unittest import TestCase
from mock import patch
from mock import MagicMock
import jsonpatch
from mockriak import create_mock_riak_client
from drow import models
from drow.fields import AutoDateField
from drow.fields import DefaultFalseField


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

            create_date = AutoDateField(only_on_creation=True)
            update_date = AutoDateField(name='update_date2')
            default_false = DefaultFalseField()

        return MyModel, settings

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.patcher.stop()
        return False


class TestFields(TestCase):
    def test_auto_date_field(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            instance = MyModel.objects.create({})

            self.assertEqual(len(instance.data), 3)
            self.assertIn('update_date2', instance.data)

    def test_default_false_field(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            instance = MyModel.objects.create({})
            self.assertIs(instance.data['default_false'], False)

    def test_override_default_false_field(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            instance = MyModel.objects.create({'default_false': True})
            self.assertIs(instance.data['default_false'], True)

    def test_create_only(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            instance = MyModel.objects.create({})
            old_update = instance.data['update_date2']
            old_create = instance.data['create_date']

            instance.save()

            self.assertEqual(instance.data['create_date'], old_create)
            self.assertNotEqual(instance.data['update_date2'], old_update)

    def test_usage_in_patch(self):
        with FakeModelContext() as context:
            MyModel, settings = context
            old_data = {
                'a': 'a'
            }
            new_data = {
                'b': 'b',
                'update_date2': 'asdf',
                'create_date': 'asdf'
            }

            instance = MyModel.objects.create(old_data)
            old_update = instance.data['update_date2']
            old_create = instance.data['create_date']

            patch_data = jsonpatch.make_patch(
                instance.data, new_data)

            result = instance.patch(patch_data)

            self.assertEqual(result.data['create_date'], old_create)
            self.assertNotEqual(result.data['update_date2'], old_update)
            self.assertNotEqual(result.data['update_date2'], 'asdf')
