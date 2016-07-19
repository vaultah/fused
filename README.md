# fused
Small convenience wrapper around redis-py

[![Build Status](https://travis-ci.org/vaultah/fused.svg?branch=master)](https://travis-ci.org/vaultah/fused)

α

Still pretty raw.


----------

There're 2 main types of fields: embedded and standalone.

Embedded fields are stored in a Redis hash linked to the primary key. Fused converts the value of an embedded field to string, due to limitations of Redis hashes. Values of missing embedded fields are `None`.

Standalone fields are stored as a separate key. A standalone field can either be a proxy or an "auto" field. Values of missing proxy fields are proxy objects. Values of missing auto fields are empty objects of corresponding Python types (i.e. `''` for `String`s and `[]` for `List`s.

