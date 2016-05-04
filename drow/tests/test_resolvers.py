__author__ = 'max'

from unittest import TestCase
from time import time
from mockriak import create_mock_riak_object


class TestDefaultResolver(TestCase):
    def test_json_resolver(self):
        from ..models import resolve_json

        root_object = create_mock_riak_object()
        now = time()

        siblings = []
        for i in xrange(4):
            sibling = create_mock_riak_object()
            sibling.last_modified = now - i  # most recent first tests sort
            siblings.append(sibling)

        siblings[3].data = {
            'a': 'a',
            'b': {
                'c': 'c',
                'd': 'd'
            },
            'h': None,
            'i': [1, 2, 3],
            'j': False
        }

        siblings[2].data = {}

        siblings[1].data = {
            'a': 'a2',
            'b': {
                'c': 'c2',
                'e': {
                    'f': 'f'
                }
            },
            'h': {'hi': 'mom'},
            'i': ['a', 'b', 'c'],
            'j': True
        }

        siblings[0].data = {
            'a': 'a3',
            'b': {
                'e': {
                    'g': 'g'
                }
            }
        }

        root_object.siblings = siblings

        resolve_json(root_object)

        # The resolver should reduce the siblings count for the object to 1
        self.assertEqual(len(root_object.siblings), 1)
        r = root_object.siblings[0].data

        # Expected resolution
        v = {
            'a': 'a3',
            'b': {
                'c': 'c2',
                'd': 'd',
                'e': {
                    'f': 'f',
                    'g': 'g'
                }
            },
            'h': {'hi': 'mom'},
            'i': ['a', 'b', 'c'],
            'j': True
        }

        # Validate resolution agrees with expected results
        self.assertEqual(r, v)

    def test_empty_case(self):
        from ..models import resolve_json

        root_object = create_mock_riak_object()
        now = time()

        siblings = []
        for i in xrange(4):
            sibling = create_mock_riak_object()
            sibling.last_modified = now - i  # most recent first tests sort
            sibling.data = {}
            siblings.append(sibling)

        root_object.siblings = siblings

        resolve_json(root_object)

        self.assertEqual(len(root_object.siblings), 1)
        r = root_object.siblings[0].data

        # Combining a bunch of empty dictionaries should yield an empty
        # dictionary
        self.assertEqual(r, {})


class TestSetResolver(TestCase):
    def test_json_resolver(self):
        from ..models import resolve_json_as_set

        root_object = create_mock_riak_object()
        now = time()

        siblings = []
        for i in xrange(4):
            sibling = create_mock_riak_object()
            sibling.last_modified = now - i  # most recent first tests sort
            siblings.append(sibling)

        siblings[3].data = {
            'a': 'a',
            'b': {
                'c': 'c',
                'd': 'd'
            },
            'h': None,
            'i': [1, 2, 3],
            'j': False
        }

        siblings[2].data = {}

        siblings[1].data = {
            'a': 'a2',
            'b': {
                'c': 'c2',
                'e': {
                    'f': 'f'
                }
            },
            'h': {'hi': 'mom'},
            'i': ['a', 'b', 'c', 3],
            'j': True
        }

        siblings[0].data = {
            'a': 'a3',
            'b': {
                'e': {
                    'g': 'g'
                }
            }
        }

        root_object.siblings = siblings

        resolve_json_as_set(root_object)

        # The resolver should reduce the siblings count for the object to 1
        self.assertEqual(len(root_object.siblings), 1)
        r = root_object.siblings[0].data
        r['i'] = sorted(r['i'])

        # Expected resolution
        v = {
            'a': 'a3',
            'b': {
                'c': 'c2',
                'd': 'd',
                'e': {
                    'f': 'f',
                    'g': 'g'
                }
            },
            'h': {'hi': 'mom'},
            'i': sorted([1, 2, 3, 'a', 'b', 'c']),
            'j': True
        }

        # Validate resolution agrees with expected results
        self.assertEqual(r, v)

    def test_empty_case(self):
        from ..models import resolve_json_as_set

        root_object = create_mock_riak_object()
        now = time()

        siblings = []
        for i in xrange(4):
            sibling = create_mock_riak_object()
            sibling.last_modified = now - i  # most recent first tests sort
            sibling.data = {}
            siblings.append(sibling)

        root_object.siblings = siblings

        resolve_json_as_set(root_object)

        self.assertEqual(len(root_object.siblings), 1)
        r = root_object.siblings[0].data

        # Combining a bunch of empty dictionaries should yield an empty
        # dictionary
        self.assertEqual(r, {})
