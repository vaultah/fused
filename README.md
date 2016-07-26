# fused

Lightweight ORM-ish convenience wrapper around redis-py.

[![Build Status](https://travis-ci.org/vaultah/fused.svg?branch=master)](https://travis-ci.org/vaultah/fused)


**α**

Still pretty raw.


----------


##Fields

There're 2 main types of fields: *embedded* and *standalone*.
###Embedded fields

They are stored in a Redis hash linked to the primary key. Fused converts the value of an embedded field to string, due to limitations of Redis hashes. Values of missing embedded fields are `None`.

###Standalone fields

Each standalone field occupies a separate key. A standalone field can either be a proxy or an "auto" field. Values of missing proxy fields are proxy objects. Values of missing auto fields are empty objects of corresponding Python types (i.e. `''` for `String`s and `[]` for `List`s.

####Proxy fields

A proxy field returns a special proxy object that pass the fully qualified name of the corresponding key as the first argument to all redis-py methods you invoke:

    model.field.set('abc') # ≡ redis_connection.set(qualified_name, 'abc')
    model.field.get() -> 'abc' # ≡ redis_connection.get(qualified_name)

####Auto fields

Auto fields accept and return instances of Python objects e.g. `dict`, `set`, `int`, etc. You can only assign to an auto field, access the value it holds, or delete it from Redis.


##Encoding & return types

Fused decodes all strings coming from Redis (including individual elements/values/keys of auto fields) except for

 - values of `Bytes` fields, because that would defeat the purpose of `Bytes` fields
 - strings returned by methods of proxy objects, because it would be hard, and because proxy fields simply return what redis-py returns

