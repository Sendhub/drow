__author__ = 'max'

from unittest import TestCase
from mock import patch
from mockriak import create_mock_riak_client
from drow import models
from drow.errors import InvalidConfig


def decoder(value):
    pass


def encoder(value):
    pass


def creation_validator(data):
    pass


def storage_validator(data):
    pass


def resolver(riak_object):
    pass

content_type = 'application/x.content'


class TestModelInitialization(TestCase):
    @patch.object(models, 'settings')
    def test_config_switches(self, settings):
        settings.RIAK_CLIENT = create_mock_riak_client()
        client = settings.RIAK_CLIENT
        bucket = client.bucket()

        class MyModel(models.Model):
            class Meta:
                content_type = content_type
                decoder = decoder
                encoder = encoder
                index = 'test_index'
                bucket_name = 'test_bucket'
                bucket_type_name = 'test_type'
                resolver = resolver
                creation_validator = creation_validator
                storage_validator = storage_validator

        self.assertEqual(MyModel.objects._state.bucket, bucket)
        client.bucket_type.assert_called_once_with('test_type')
        client.bucket_type.return_value.bucket.assert_called_once_with(
            'test_bucket')
        self.assertEqual(MyModel._meta.content_type, content_type)
        self.assertEqual(MyModel._meta.index, 'test_index')
        self.assertEqual(MyModel._meta.creation_validator, creation_validator)
        self.assertEqual(MyModel._meta.storage_validator, storage_validator)
        bucket.set_decoder.assert_called_once_with(content_type, decoder)
        bucket.set_encoder.assert_called_once_with(content_type, encoder)
        self.assertEqual(bucket.resolver, resolver)

    @patch.object(models, 'settings')
    def test_predefined_queryset(self, settings):
        from drow.queryset import QuerySet
        settings.RIAK_CLIENT = create_mock_riak_client()

        queryset = QuerySet()

        class MyModel(models.Model):
            class Meta:
                bucket_name = 'test_bucket'
                bucket_type_name = 'test_type'

            objects = queryset

        self.assertIs(MyModel.objects, queryset)

    @patch.object(models, 'settings')
    def test_decoder_required_for_custom_content_type(self, settings):
        with self.assertRaises(InvalidConfig):
            class MyModel(models.Model):
                class Meta:
                    content_type = content_type
                    bucket_name = 'test_bucket'
                    bucket_type_name = 'test_type'
                    encoder = encoder

    @patch.object(models, 'settings')
    def test_encoder_required_for_custom_content_type(self, settings):
        with self.assertRaises(InvalidConfig):
            class MyModel(models.Model):
                class Meta:
                    content_type = content_type
                    bucket_name = 'test_bucket'
                    bucket_type_name = 'test_type'
                    decoder = decoder

    @patch.object(models, 'settings')
    def test_bucket_name_required(self, settings):
        with self.assertRaises(InvalidConfig):
            class MyModel(models.Model):
                class Meta:
                    bucket_type_name = 'test_type'

    @patch.object(models, 'settings')
    def test_bucket_type_required(self, settings):
        with self.assertRaises(InvalidConfig):
            class MyModel(models.Model):
                class Meta:
                    bucket_name = 'test_bucket'

    @patch.object(models, 'settings')
    def test_callable_restriction(self, settings):
        with self.assertRaises(InvalidConfig):
            class MyModel(models.Model):
                class Meta:
                    bucket_name = 'test_bucket'
                    bucket_type_name = 'test_type'
                    creation_validator = 'test'

        with self.assertRaises(InvalidConfig):
            class MyModel(models.Model):  # noqa
                class Meta:
                    bucket_name = 'test_bucket'
                    bucket_type_name = 'test_type'
                    storage_validator = 'test'

        with self.assertRaises(InvalidConfig):
            class MyModel(models.Model):  # noqa
                class Meta:
                    bucket_name = 'test_bucket'
                    bucket_type_name = 'test_type'
                    get_bucket = 'test'

        # Make sure the base declaration works fine, so we're not detecting
        # some other cause for an InvalidConfig error

        class MyModel(models.Model):  # noqa
            class Meta:
                bucket_name = 'test_bucket'
                bucket_type_name = 'test_type'

    @patch.object(models, 'settings')
    def test_repr(self, settings):
        settings.RIAK_CLIENT = create_mock_riak_client()

        class MyModel(models.Model):
            class Meta:
                content_type = content_type
                decoder = decoder
                encoder = encoder
                index = 'test_index'
                bucket_name = 'test_bucket'
                bucket_type_name = 'test_type'
                resolver = resolver
                creation_validator = creation_validator
                storage_validator = storage_validator

        # Make sure none of these raises an exception.  Reprs should be able
        # to handle unicode and any repr errors should not cause a program
        # to crash
        repr(MyModel.objects.get('this_is_a_repor'))
        repr(MyModel.objects.get(u'\xee'))
        repr(MyModel.objects.get(u'\xee'.encode('utf-8')))
        repr(MyModel.objects.get(u'\xee'.encode('utf-16')))

    @patch.object(models, 'settings')
    def test_empty_metaclass(self, settings):
        with self.assertRaises(InvalidConfig):
            class MyModel(models.Model):
                pass
