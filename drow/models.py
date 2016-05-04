__author__ = 'max'

from conf import settings
from queryset import QuerySet
from queryset import QuerySetState
from errors import InvalidConfig
from errors import DoesNotExist
from errors import InvalidPatch
from errors import SearchError
from fields import ModelField

DEFAULT_CONTENT_TYPE = 'application/json'

REQUIRED_SETTINGS = [
    'bucket_type_name',
    'bucket_name'
]


def resolve_json_as_set(riak_object):
    """
    Like the resolve_json, except lists are treated like sets such
    that the result of list resolution is union of all lists.

    Do not use this resolver if your JSON has a list of anything other than
    primitives (int, string, float, bool, etc.), the result of merging such
    data models would be unpredictable.

    :param RiakObject riak_object: The riak object to resolve
    """
    def recursive_update(resolution, sibling):
        """
        This sub-function does the actual merging.

        :param dict resolution: The dictionary into which the results will
                                be merged
        :param dict sibling: The dictionary to be integrated into the results
        """
        resolution_keys = set(resolution.keys())
        sibling_keys = set(sibling.keys())
        new_keys = sibling_keys - resolution_keys

        # deep copy is not necessary here as the siblings will be destroyed
        # anyways
        for key in new_keys:
            resolution[key] = sibling[key]

        overlapping_keys = resolution_keys.intersection(sibling_keys)

        for key in overlapping_keys:
            if isinstance(resolution[key], dict):
                recursive_update(resolution[key], sibling[key])
            elif isinstance(resolution[key], list):
                list_resolution = set(resolution[key])
                list_resolution.update(sibling[key])
                resolution[key] = list(list_resolution)
            else:
                resolution[key] = sibling[key]

    oldest_version_first = sorted(
        riak_object.siblings, key=lambda s: s.last_modified)

    resolution = oldest_version_first[0]
    for sibling in oldest_version_first[1:]:
        recursive_update(resolution.data, sibling.data)

    riak_object.siblings = [resolution]


def resolve_json(riak_object):
    """
    This is a Riak sibling resolver that tries to merge dictionaries with
    the following logic:

        1) For any value other than a dictionary, the most recent version
           of that value wins.
        2) For dictionaries, all keys will be merged.  In the case of key
           conflicts, rule (1) will apply for non-dictionary values.  For
           dictionary values, rule (2) will be recursively applied.

    :param RiakObject riak_object: The Riak object to resolve
    """
    def recursive_update(resolution, sibling):
        """
        This sub-function does the actual merging.

        :param dict resolution: The dictionary into which the results will
                                be merged
        :param dict sibling: The dictionary to be integrated into the results
        """
        resolution_keys = set(resolution.keys())
        sibling_keys = set(sibling.keys())
        new_keys = sibling_keys - resolution_keys

        # deep copy is not necessary here as the siblings will be destroyed
        # anyways
        for key in new_keys:
            resolution[key] = sibling[key]

        overlapping_keys = resolution_keys.intersection(sibling_keys)

        for key in overlapping_keys:
            if isinstance(resolution[key], dict):
                recursive_update(resolution[key], sibling[key])
            else:
                resolution[key] = sibling[key]

    oldest_version_first = sorted(
        riak_object.siblings, key=lambda s: s.last_modified)

    resolution = oldest_version_first[0]
    for sibling in oldest_version_first[1:]:
        recursive_update(resolution.data, sibling.data)

    riak_object.siblings = [resolution]


class ModelState(object):
    """
    Class that holds the state for an actual instance of the model.
    This includes things like the database key and the RiakObject
    """
    pass


class Options(object):
    """
    This class holds the options for a Model.  Any attribute/method
    defined here is a default and can be overridden by defining it on
    a model's Meta class
    """
    def __init__(self, meta=None):
        """
        :param class meta: The "Meta" class defined on a model
        """
        # create an empty metaclass if none exists
        if not meta:
            meta = type('Meta', (object,), {})
        self.meta = meta

        # These arguments must be functions
        functions = {
            'creation_validator',
            'storage_validator',
            'get_bucket'
        }

        # overwrite defaults with attributes from Meta config class
        for key in meta.__dict__:
            if not key.startswith('_'):
                if key in functions:
                    if not callable(meta.__dict__[key]):
                        raise InvalidConfig("{} must be callable!".format(key))
                setattr(self, key, meta.__dict__[key])

    # ####### Defaults #######

    def get_bucket(self):
        """
        Return the Riak bucket associated with this model

        :return: A Riak bucket
        :rtype: RiakBucket
        """
        bucket_type = settings.RIAK_CLIENT.bucket_type(self.bucket_type_name)
        return bucket_type.bucket(self.bucket_name)

    # abstract classes are inheritable, not full Riak models
    abstract = False

    # the content type of serialized data
    content_type = DEFAULT_CONTENT_TYPE

    # sibling conflict resolver
    resolver = None

    # object to deserialize raw data
    decoder = None

    # object to serialize raw data
    encoder = None

    # the Solr search index to search over
    index = None

    # function validating data provided for creation
    creation_validator = None

    # function that validates the correct data is being stored in Riak
    storage_validator = None


class ModelMetaclass(type):
    """
    A metaclass that customizes model creation.  Basically, anytime we
    create a class (not an instance) that inherits from Model, this sets
    it up so it references the correct Riak bucket, etc.
    """
    def __new__(mcs, name, bases, dct):
        """
        Combine the settings in Meta with the defaults in the Options class
        """
        meta = dct.get('Meta', None)
        _meta = Options(meta)
        dct['_meta'] = _meta

        if not getattr(_meta, 'abstract', False):
            missing = [o for o in REQUIRED_SETTINGS if not hasattr(_meta, o)]
            if missing:
                raise InvalidConfig(
                    "Missing configuration parameters: {}".format(missing)
                )

        return super(ModelMetaclass, mcs).__new__(
            mcs,
            name,
            bases,
            dct
        )

    def __init__(cls, name, bases, dct):
        """
        Set up the QuerySet object, so the user can access the database
        """
        if not getattr(cls._meta, 'abstract', False):
            if not hasattr(cls, 'objects'):
                cls.objects = QuerySet()
            cls.objects._state = QuerySetState()
            cls.objects._state.model = cls
            cls.objects._state.bucket = cls._meta.get_bucket()

            # Set encoder/decoder if content type is non-standard
            if cls._meta.content_type != DEFAULT_CONTENT_TYPE:
                if not cls._meta.decoder:
                    raise InvalidConfig(
                        "You must specify a decoder if using a non-standard "
                        "content type"
                    )
                if not cls._meta.encoder:
                    raise InvalidConfig(
                        "You must specify an encoder if using a non-standard "
                        "content type"
                    )

                cls.objects._state.bucket.set_decoder(
                    cls._meta.content_type,
                    cls._meta.decoder
                )
                cls.objects._state.bucket.set_encoder(
                    cls._meta.content_type,
                    cls._meta.encoder
                )

            if cls._meta.resolver:
                # Accessing resolver via class __dict__ prevents it from
                # returning an unbound method (a result of the fact that
                # functions are descriptors in python), and will instead
                # return the raw function.  The disadvantage of this is it
                # does not work with inheritance as __dict__ does not resolve
                # attributes of parent classes.
                cls.objects._state.bucket.resolver = \
                    cls._meta.__dict__['resolver']
            else:
                cls.objects._state.bucket.resolver = resolve_json

            cls._meta.fields = {}
            for name in dir(cls):
                attribute = getattr(cls, name)
                if not isinstance(attribute, ModelField):
                    continue

                if attribute.name is None:
                    attribute.name = name

                cls._meta.fields[attribute.name] = attribute

        super(ModelMetaclass, cls).__init__(name, bases, dct)


class Model(object):
    """
    The abstract base class from which all other models should be derived
    """
    __metaclass__ = ModelMetaclass

    class Meta:
        abstract = True

    DoesNotExist = DoesNotExist
    InvalidPatch = InvalidPatch
    SearchError = SearchError

    def __init__(self, key, riak_object=None):
        """
        Used to create a specific instance of a model.  This is not meant
        to be called directly by the user, but by the QuerySet object.

        :param str key: The object's Riak key
        :param RiakObject riak_object: The data as stored by Riak
        """
        self._state = ModelState()
        self._state.riak_object = riak_object
        self._state.key = key

        # hide the objects manager, as it should not be accessed from an
        # active instance
        self._state.objects = self.objects
        self.objects = None

    def __repr__(self):
        """
        Display objects in Python shell by class name and key, rather than
        by memory address

        :return: A readable representation of the class
        :rtype: str
        """
        # Currently Riak client does not support unicode keys, but you
        # never know...
        try:
            u = unicode(self._state.key)
        except UnicodeDecodeError:
            try:
                u = unicode(self._state.key, 'utf-8')
            except UnicodeDecodeError:
                u = '[BAD UNICODE]'

        repr_ = u'<{}: {}>'.format(self.__class__.__name__, u)
        return repr_.encode('utf-8')

    @property
    def data(self):
        """
        Get the data stored under the key

        :return: The data stored under the key
        :rtype: dict
        """
        if not self._state.riak_object:
            self._state.objects._active_get(self)
        return self._state.riak_object.data

    @data.setter
    def data(self, value):
        """
        Allow the data dict to be replaced

        :param value: The value with which to replace the data dict
        """
        if not self._state.riak_object:
            self._state.objects._active_get(self, must_exist=False)
        self._state.riak_object.data = value

    @property
    def key(self):
        """
        Get the Riak key.  Probably best if this is non-modifiable
        :return: The key
        :rtype: str
        """
        return self._state.key

    def save(self):
        """
        Save an object to the database, Model.objects.patch is the preferred
        method for swapping data that doesn't need to be examined beforehand
        as it requires one query of the database as opposed to two.
        """
        return self._state.objects.put(self.key, self.data)

    def patch(self, patch_data):
        """
        Patch an object that already exists in the database.

        :param str patch_data: The patch to be applied
        """
        return self._state.objects.patch(self.key, patch_data)

    def delete(self):
        """
        Delete the object from Riak
        """
        return self._state.objects.delete(self.key)
