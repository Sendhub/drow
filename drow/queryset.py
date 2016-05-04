__author__ = 'max'

from riak import RiakError

from errors import InvalidPatch
from errors import DoesNotExist
from errors import SearchError


class QuerySet(object):
    """
    Queries the database and returns instances of the associated Model
    """
    def search(self, query, start=0, rows=20):
        """
        Search the Solr index using the given query, return the results

        :param str query: The Solr query text
        :param int start: The index at which to start returning results
        :param int rows: The number of rows to return **NOTE** if there are any
                     siblings, fewer rows than requested will be returned,
                     as siblings are de-duplicated
        :return: The search results
        :rtype: SearchResults<Model>
        """
        bucket = self._state.bucket
        index = self._state.model._meta.index

        try:
            solr_results = bucket.search(
                query, index=index, start=start, rows=rows)

        except RiakError as e:
            if isinstance(e.value, basestring) and \
                    e.value.startswith('Query unsuccessful'):
                raise SearchError(u'Bad Query: {}'.format(query))
            else:
                raise

        # set() is needed here as Riak search will return multiple results for
        # a given key if it has siblings, whereas we want unique results
        keys = {r['_yz_rk'] for r in solr_results['docs']}

        objects = [self._state.model(o.key, o) for o in bucket.multiget(keys)]

        return SearchResults(
            objects, start, solr_results['num_found'], self._state.model)

    def _store(self, riak_object):
        """
        Central access point for writing to the Riak bucket

        :param RiakObject riak_object: The Riak object to be saved
        :return: RiakObject
        """
        validator = self._state.model._meta.storage_validator
        if validator is not None:
            validator(riak_object.data)

        return riak_object.store()

    def _active_get(self, instance, must_exist=True):
        """
        Fill the instance with riak_object data

        :param Model instance: An instance of the Model class
        :param bool must_exist: True if a missing object should be an error,
                                default is True
        """
        bucket = self._state.bucket
        instance._state.riak_object = bucket.get(instance._state.key)

        if must_exist and not instance._state.riak_object.exists:
            raise DoesNotExist('{} "{}" does not exist!'.format(
                self._state.model.__name__,
                instance._state.key
            ))

    def get(self, key, active=False, must_exist=True):
        """
        Retrieve an object from the database
        :param str key: The database key to retrieve
        :param bool active: Whether the data should be lazily or aggressively
                            retrieved
        :param bool must_exist: True if the object not existing should be an
                                error.  Only works if active is True. Default
                                is True
        :return: An instance of the Model object
        """
        instance = self._state.model(key)

        if active:
            self._active_get(instance, must_exist)

        return instance

    def create(self, data):
        """
        Create/store an instance of Model with the given data, relying on
        Riak to provide the object's key

        :param data: The data to be stored under the object
        :return: A Model instance
        :rtype: Model
        """
        bucket = self._state.bucket

        creation_validator = self._state.model._meta.creation_validator
        if creation_validator is not None:
            creation_validator(data)

        fields = self._state.model._meta.fields
        for field_name in fields:
            data[field_name] = fields[field_name].new_value(
                'create', data.get(field_name, None))

        riak_object = bucket.new(
            data=data,
            content_type=self._state.model._meta.content_type
        )

        self._store(riak_object)

        return self._state.model(riak_object.key, riak_object)

    def patch(self, key, patch):
        """
        Apply the provided patch to the data stored at the given key

        :param str key: The key in Riak to be patched
        :param patch: The patch obj to be applied (must have apply method)
        :return: A Model instance
        :rtype: Model
        """
        instance = self.get(key, active=True)

        old_values = {}
        fields = self._state.model._meta.fields
        for field_name in self._state.model._meta.fields:
            old_values[field_name] = instance.data.get(field_name, None)

        validate_patch(patch, instance.data)
        patch.apply(instance._state.riak_object.data, in_place=True)

        # Enforce field constraints
        for field_name in fields:
            instance._state.riak_object.data[field_name] = \
                fields[field_name].new_value(
                    'patch',
                    instance._state.riak_object.data.get(field_name, None),
                    old_values[field_name]
            )

        self._store(instance._state.riak_object)
        return instance

    def put(self, key, data):
        """
        Update an existing object/create a new object at the specified key

        :param str key: The key at which data will be stored
        :param data: The data to be stored
        :return: A Model instance
        :rtype: Model
        """
        instance = self.get(key, active=True, must_exist=False)

        creation_validator = self._state.model._meta.creation_validator

        if creation_validator is not None:
            creation_validator(data)

        if not instance._state.riak_object.exists:
            instance._state.riak_object.content_type = \
                instance._meta.content_type

        if instance.data is None:
            instance.data = {}

        old_values = {}
        fields = self._state.model._meta.fields
        for field_name in self._state.model._meta.fields:
            old_values[field_name] = instance.data.get(field_name, None)

        instance._state.riak_object.data = data

        # Enforce field constraints
        method = 'put'
        if not instance._state.riak_object.exists:
            method = 'create'
        for field_name in fields:
            instance._state.riak_object.data[field_name] = \
                fields[field_name].new_value(
                    method, data.get(field_name, None), old_values[field_name])

        self._store(instance._state.riak_object)
        return instance

    def delete(self, key):
        """
        Delete an existing object from Riak

        :param str key: The key to delete
        """
        bucket = self._state.bucket
        return bucket.new(key).delete()


class SearchResults(object):
    """
    Convenience class that wraps around search results.  It collates all
    the vital statistics and displays them in the Python shell with little
    work.
    """
    def __init__(self, objects, start, num_found, model):
        """
        :param list<Model> objects: The objects returned by the search
        :param int start: The index at which results began to return
        :param int num_found: The total number of objects found in the list
        :param Type model: The Model class these instances belong to
        """
        self.objects = objects
        self.start = start
        self.num_found = num_found
        self.model = model

    def __iter__(self):
        """
        Allow the user to iterate over search results
        """
        return self.objects.__iter__()

    def __getitem__(self, key):
        """
        Allow the user to index the results list directly from this class
        """
        return self.objects[key]

    def __len__(self):
        """
        The length should be the total number of results in this result set
        """
        return len(self.objects)

    def __repr__(self):
        """
        Customize the display of this object in the Python shell
        """
        list_repr = repr(self.objects)
        if len(list_repr) > 100:
            list_repr = list_repr[:96] + '...]'
        return '<{} Search [{}:{}]/{}: {}>'.format(
            self.model.__name__,
            self.start,
            self.start + len(self.objects),
            self.num_found,
            list_repr
        )


class QuerySetState(object):
    """
    Class that holds the instance state for a QuerySet object
    """
    pass


def validate_patch(patch, data):
    """
    We don't want to allow the "add" operation to replace existing elements
    as this could clobber data that gets out of sync, clients should use the
    "replace" operation to signify intent.

    :param JsonPatch patch: The JsonPatch object to validate
    :param data: The data the JsonPatch will be applied to
    :raises InvalidPatch: If an "add" operation in the patch would clobber data
    """
    try:
        for action in patch:
            if action['op'] != 'add':
                continue

            path = action['path'].split('/')[1:]

            valid = False
            ref = data
            for entry in path:
                unescaped_entry = entry.replace('~1', '/').replace('~0', '~')
                try:
                    ref = ref[unescaped_entry]
                except (KeyError, IndexError):
                    valid = True
                    break

            if not valid:
                path = action['path']
                if isinstance(path, unicode):
                    path = path.encode('utf-8')

                error_text = \
                    'Cannot add a JSON key that already exists: {}'.format(
                        path)
                raise InvalidPatch(error_text)
    except KeyError:
        raise InvalidPatch('Missing required jsonpatch parameter')
