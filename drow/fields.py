__author__ = 'max'

from datetime import datetime


def solr_now():
    """
    Get the current time in UTC and format it in a way that Solr can digest.

    :return: A Solr-digestible time format
    :rtype: str
    """
    return datetime.utcnow().isoformat() + 'Z'


class ModelField(object):
    pass


class AutoDateField(ModelField):
    """
    A field that will automatically inject time information into the object.
    If any part of a request wants to overwrite this data, that part of the
    request will be ignored.
    """
    def __init__(self, name=None, only_on_creation=False):
        """
        :param str name: The name of the JSON field to store the data under
        :param only_on_creation: Only assert a value on object creation
        """
        self.name = name
        self.apply_on_create = True
        self.apply_on_update = not only_on_creation

    def new_value(self, operation, proposed_value=None, old_value=None):
        """
        Return the computed value for a given storage operation based on
        the value of the old field.

        :param str operation: One of "patch", "create", "put"
        :param proposed_value: The value proposed by the save operation
        :param old_value: The original value
        :return: The value to be stored
        """
        if not self.apply_on_update and operation != 'create':
            return old_value
        return solr_now()


class DefaultFalseField(ModelField):
    """
    A field that will default to False if not otherwise declared
    """
    def __init__(self, name=None):
        """
        :param str name: The name of the JSON field to store the data under
\       """
        self.name = name

    def new_value(self, operation, proposed_value=None, old_value=None):
        """
        Return the computed value for a given storage operation based on
        whether or not the proposed value is defined
        :param str operation: One of "patch", "create", "put"
        :param proposed_value: The value proposed by the save operation
        :param old_value: The original value
        :return: The value to be stored
        """

        if proposed_value is not None:
            return proposed_value
        else:
            return False
