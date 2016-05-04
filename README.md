Drow (Django Riak Object Writer)
========

The idea of this project is to create a simple Django-style ORM for Riak that makes little to no assumptions about
what data you're trying to work with or how it's structured (though some convenience features currently assume JSON).


Configuration
=============

There is very little to set up.  Just make sure the Riak python client is enabled and stored under the global variable
`RIAK_CLIENT` in a file called settings.py that must be importable (generally this means it is somewhere on sys.path).

Here is an example settings.py file:

```python
    credentials = {
        'username': 'my_username',
        'password': 'my_password',
        'cacert_file': 'my_cacert.pem'
    }
    
    RIAK_CLIENT = riak.RiakClient(credentials=credentials)
```


Usage
=====


Model Declaration
-----------------

At its simplest, one need only declare a model.  Here is some sample Python code showing how:

```python
    import json
    
    from drow.models import Model
    from drow.fields import AutoDateField


    def airplane_creation_validator(data):
        """
        Called when an object is created, before any automatic
        field modifications are done.

        Raise an exception if there is a problem.
        """
        return

    
    def airplane_storage_validator(data):
        """
        Called immediately before persisting data to Riak.

        Raise an exception if if there is a problem.
        """
        return
    
    
    class Airplane(Model):
        class Meta:
            content_type = 'application/x.airplane-json'
            decoder = json.loads
            encoder = json.dumps
            index = 'airplanes'
            bucket_type_name = 'airplaness'
            bucket_name = 'airplanes'
            creation_validator = airplane_creation_validator
            storage_validator = airplane_storage_validator
    
        createdTs = AutoDateField(only_on_creation=True)
        modifiedTs = AutoDateField(name='modifiedTs')
```

Just like in Django, the class `Airplane` inherits from `Model` and there is an internal class called `Meta` that
defines options for the model.  The options are as follows:

 * content_type: The content type Riak will store with the data (default is application/json)
 * decoder: A function that, when fed the serialized data, will return a Python object
 * encoder: A function that, when fed a Python object, will serialize it in a manner suitable for storing in Riak
 * index: The Solr search index to use with this model
 * bucket_type_name: The bucket type this model will be stored under
 * bucket_name: The bucket this model will be stored under
 * creation_validator: A function that will take the data provided to a creation method, such as `put` or `create`
                       and will raise an exception if there are any issues
 * storage_validator: A function that will validate the data about to be saved to Riak, raising an exception if
                      there are any problems
 
Note that both of the validator functions expect full Python objects, not data in the serialized form.

Also note the `createdTs` and `modifiedTs` class members.  The `AutoDateField` type will automatically insert date
information into the object, either at creation time, or every time the object is saved (the default).


Model Members
-------------

Models are fairly simple things, there are only two members:

 * data: The stored data as a Python object (deserialized)
 * key: The key the object is stored under
 
There are also a few helper methods for writing to the database.  Their usage is described below:

 * save
 * patch
 * delete


Storing/Retrieving Data
-----------------------


### Retrieving Data

Much like in Django, data is stored and retrieved through a model's `objects` property.  For example:

```python
    my_airplane = Airplane.objects.get('abc')
```

would retrieve the Airplane object stored at key 'abc' and put it into the `my_airplane` variable.  As a shortcut, one
could also bypass the objects property entirely:

```python
    my_airplane = Airplane('abc')
```


### Modifying Data

Suppose I wanted to modify the first name of my airplane and save the results.  I could do so as follows:

```python
    my_airplane.data['firstName'] = 'Nobody'
    my_airplane.save()
```

If I have some JSONPatch string I would like to apply, it is a bit more efficient to just apply the patch directly:

```python
    my_airplane.patch(patch_data)
    Airplane.objects.get('abc').patch(patch_data)
    Airplane.objects.patch('abc', patch_data)
```
    
Each of the above three lines does exactly the same thing.  Note that the patch is applied directly to the data as
stored on the database.  This means that if you have any un-saved modifications to your object, they will be clobbered.


### Creating New Data

There are three ways to create new data:

```python
    my_airplane = Airplane.objects.create(my_airplane_data)
    
    my_airplane = Airplane.objects.put('def', my_airplane_data)
    
    my_airplane = Airplane('def')
    my_airplane.data = my_airplane_data
    my_airplane.save()
```

Using `create` will cause Riak to auto-generate the key, which can be recovered from the object stored under my_airplane.
Using `put` allows you to specify the key under which the data will be stored.  It will also clobber any data that might
already be stored under that key.  The final method will also work, but be slightly less efficient as there will be two
queries to the Riak database instead of one.


### Deleting Data

There are two ways to delete data, either one will work:

```python
    my_airplane.delete()
    Airplane.objects.delete('def')
```


### Searching Data

If your model has a Solr index it is searchable using the `search` method.  The argument passed to `search` is the
Solr query you want to run.  The return value will be a special result set that is effectively a list of objects.

```python
    search_results = Airplane.objects.search('firstName:Joh*')
```

The default number of rows returned is 20.  You can change this by providing a different value to the `rows` argument
for search.  You can also page through results by providing a new starting value to the `offset` argument, whose default
value is 0.

Note that Riak will return multiple results for the same object if that object has siblings.  This behavior
is not desirable, so the ORM will deduplicate any results for the same key.  This means you may receive fewer than
`rows` results even though there is overflow to page through.

You can get the total number of results as returned by Riak from the `num_found` member of the results set:

```python
    search_results.num_found
```

to figure out if you need to continue paging, check if `offset + rows < num_found`