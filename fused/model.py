import redis
import json
import time
from abc import ABCMeta
from collections.abc import Mapping
from itertools import chain
from . import utils, exceptions
# All subclasses of Field and Field itself
from .fields import *

# Store instances of classes created by MetaModel
_registry = {}


def _rec_bases(o):
    bases = [x for x in o.__bases__ if len(x.__bases__) > 0]
    yield from (x for b in bases for x in _rec_bases(b))
    yield from ((k, v) for k, v in vars(o).items() if isinstance(v, Field))


class MetaModel(ABCMeta):

    def __new__(mcs, model_name, bases, attrs):
        mappings = ('_fields', '_unique_keys', '_unique_fields',
                    '_required_fields', '_plain_fields', '_standalone_proxy',
                    '_standalone_auto', '_scripts', '_foreign')
        for m in mappings:
            attrs[m] = {}
        cls = super().__new__(mcs, model_name, bases, attrs)
        cls._pk = None
        _registry[cls.__name__] = cls

        # Allow the use of common fields defined in base models
        for name, field in dict(_rec_bases(cls)).items():
            field.name, field.model_name = name, model_name
            cls._fields[name] = field
            if isinstance(field, PrimaryKey):
                cls._primary_key = name

            if isinstance(field, Foreign):
                cls._foreign[name] = field

            if field.unique:
                cls._unique_fields[name] = field
                cls._unique_keys[name] = cls.qualified(name)
            elif field.standalone:
                if field.auto:
                    cls._standalone_auto[name] = field
                else:
                    cls._standalone_proxy[name] = field
            else:
                cls._plain_fields[name] = field

            if field.required:
                cls._required_fields[name] = field

        cls._standalone = dict(cls._standalone_proxy, **cls._standalone_auto)
        cls._plain = dict(cls._unique_fields, **cls._plain_fields)  

        try:
            cls.__redis__ = cls.redis
        except AttributeError:
            # Base model class, ignore it
            pass
        else:
            # We can register those now and change the connection later
            scripts = ['primary_key']
            if cls._unique_fields:
                scripts.append('unique')
            
            for s in scripts:
                cls._scripts[s] = cls.__redis__.register_script(utils.SCRIPTS[s])
            # Get some information from the connection instance
            # We need encoding and/or decode_responses to handle conversion
            _params = cls.redis.connection_pool.connection_kwargs
            cls._redis_encoding = _params.get('encoding')

        return cls
                    

class Model(metaclass=MetaModel):

    _field_sep = ':'

    def __init__(self, *, data=None, **ka):
        self._field_cache = {}
        self.__context_depth__ = 0
        self.data = {}
        # If the PK is present, we assume that the rest of fields
        # are there as well
        if data is None or self._primary_key not in data and not 'primary_key' in data:
            if len(ka) > 1:
                raise ValueError('You can only search by 1 unique field')
            # Will only search by one pair
            field, value = ka.popitem()
            if field in {self._primary_key, 'primary_key'}:
                raw = self._get_raw_by_pk(value)
            elif field not in self._unique_fields:  
                raise TypeError('Attempted to get by non-unique'
                                ' field {!r}'.format(field))
            else:
                raw = self._get_raw_by_unique(field, value)

            self.data.update(self._process_raw(raw))

        self.data.update(data or {})
        self._prepare()

    @classmethod
    def _get_raw_pk_by_unique(cls, field, value, connection=None):
        ''' Retrieve the primary key by one of the unique fields
            Exactly one action to make it usable with pipes. '''
        # TIL: Pipelines may be False in boolean context.
        conn = connection if connection is not None else cls.__redis__
        return conn.hget(cls.qualified(field), value)

    @classmethod
    def _get_raw_by_pk(cls, pk, connection=None):
        ''' Retrieve the HASH stored at cls.qualified(pk=pk).
            Exactly one action to make it usable with pipes. '''
        # TIL: Pipelines may be False in boolean context.
        conn = connection if connection is not None else cls.__redis__
        return conn.hgetall(cls.qualified(pk=cls.decode(PrimaryKey, pk)))

    @classmethod
    def _get_raw_by_unique(cls, field, value):
        ''' Get the primary key by one of the unique fields and return the 
            result of passing it to _get_raw_by_pk '''
        pk = cls._get_raw_pk_by_unique(field, value)
        return cls._get_raw_by_pk(cls.decode(PrimaryKey, pk))

    @classmethod
    def _process_raw(cls, raw):
        ''' Iterate over raw mapping received t to _get_raw_by_pk, convert
            each value using appropriate from_redis conversion, and
            return the result '''
        rv = {}
        for key, value in raw.items():
            decoded = cls.decode(String, key)
            ob = cls._plain[decoded]
            rv[decoded] = cls.decode(ob, value)
        return rv

    def _prepare(self):
        ''' Initialize all 'foreign' fields (create instances of corresponding
            classes) '''
        for field, ob in self._foreign.items():
            if field not in self.data:
                continue
            if isinstance(ob.foreign, type):
                ft = ob.foreign
            else:
                ft = _registry[ob.foreign]
            fv = self.data[field]
            if not isinstance(fv, ft):
                ff = ft.get_foreign(type(self))
                self.data[field] = ft(data=dict.fromkeys(ff, self), primary_key=fv)


    @classmethod
    def _write_unique(cls, data, pk):
        # Must have the same order
        keys, values, fields = [], [], []
        for k, v in data.items():
            keys.append(cls._unique_keys[k])
            fields.append(k)
            values.append(v)

        kwargs = {'args': [pk, json.dumps(values)], 'keys': keys}
        res = cls._scripts['unique'](**kwargs)
        # 0 for success
        # 1 ... len(fields) is an error
        #       (position of the first duplicate field from 'fields')
        if res:
            res -= 1
            raise exceptions.DuplicateEntry(fields[res], values[res])

    @classmethod
    def _write_pk(cls, pk, score=None):
        if score is None:
            score = time.time()
        result = cls._scripts['primary_key'](
            args=[score, pk], keys=[cls.qualified('_records')])

        if not result:
            raise exceptions.DuplicateEntry
        else:
            return result

    @classmethod
    def _remove_pk(cls, pk, connection=None):
        conn = connection if connection is not None else cls.__redis__
        return conn.zrem(cls.qualified('_records'), pk)

    def _update_plain(self, new_data):
        save = new_data.copy()
        for k, v in save.items():
            save[k] = self.encode(self._plain[k], v)
        self.redis.hmset(self.qualified(pk=self.primary_key), save)
        self.data.update(new_data)

    def _update_unique(self, new_data):
        self._write_unique(new_data, self.primary_key)
        self._update_plain(new_data)

    def _delete_plain(self, fields):
        if fields:
            self.redis.hdel(self.qualified(pk=self.primary_key), *fields)

    def _delete_unique(self, fields):
        for f in fields:
            self.redis.hdel(self.qualified(f), self.data[f])
        self._delete_plain(fields)
        
    @classmethod
    def encode(cls, ob, value):
        ''' Equivalent to ob.to_redis(value, cls._redis_encoding) but shorter '''
        return ob.to_redis(value, cls._redis_encoding)

    @classmethod
    def decode(cls, ob, value):
        ''' Equivalent to ob.from_redis(value, cls._redis_encoding) but shorter '''
        return ob.from_redis(value, cls._redis_encoding)

    @classmethod
    def get_foreign(cls, fm=None):
        ''' Obtain a list of 'foreign' fields with name equal to fm.__name__ if
            fm is a type and fm otherwise '''
        if fm is None:
            return list(cls._foreign)
        if isinstance(fm, type):
            fm = fm.__name__
        rv = []
        for k, v in cls._foreign.items():
            name = v.foreign.__name__ if isinstance(v.foreign, type) else v.foreign
            if name == fm:
                rv.append(k)
        return rv

    @classmethod
    def get_pipeline(cls):
        ''' Return a Pipeline instance for the specified Redis connection '''
        return cls.__redis__.pipeline()

    @classmethod
    def count(cls):
        ''' Return the number of elements in 'model_name : _records' '''
        return cls.__redis__.zcard(cls.qualified('_records'))

    @property
    def primary_key(self):
        return self.data[self._primary_key]

    def good(self):
        return bool(self.data)

    @classmethod
    def qualified(cls, *args, pk=None):
        ''' Return a fully qualified name. `cls._field_sep` is used as a separator.
            Example for ':':    model_name : ... : ... : ...
            If `pk` is not `None`, it will follow the model name immediately. '''
        parts = [cls.__name__]
        if pk is not None:
            parts.append(pk)
        parts.extend(args)
        return cls._field_sep.join(parts)

    @classmethod
    def instances(cls, *args):
        ''' Return a generator object converting iterable `it` on the fly and 
            yielding instances of `cls` '''
        if isinstance(args[0], Mapping):
            yield from (cls(data=x) for x in args)
        elif isinstance(args[0], cls):
            yield from args
        else:
            yield from (cls(primary_key=x) for x in args)

    @classmethod
    def new(cls, **ka):
        ''' Create and store a new instance of this model.
            All of the required fields must be provided. '''
        if cls._required_fields.keys() - ka.keys():
            raise exceptions.MissingFields
            
        if cls._primary_key not in ka:
            raise exceptions.NoPrimaryKey

        pk = ka[cls._primary_key]
        if isinstance(pk, tuple):
            score, pk = pk
            ka[cls._primary_key] = pk
        else:
            score = None

        main_key = cls.qualified(pk=pk)
        
        cls._write_pk(pk, score)

        if cls._unique_fields:
            data = {k: ka[k] for k in 
                     cls._unique_keys.keys() & ka.keys()}
            cls._write_unique(data, pk)

        # Set the rest of fields

        # All standalone fields
        with cls.get_pipeline() as pipe:
            for field, ob in cls._standalone.items():
                try:
                    value = ka[field]
                except KeyError:
                    continue
                else:
                    ob.type.save(cls.qualified(field, pk=pk), pipe, value)
            pipe.execute()

        # Unique and plain fields
        save, data = {cls._primary_key: pk}, {cls._primary_key: pk}
        for field, ob in cls._plain.items():
            try:
                value = ka[field]
            except KeyError:
                continue
            else:
                save[field] = cls.encode(ob, value)
                data[field] = value

        cls.__redis__.hmset(main_key, save)
        return cls(data=data)

    def delete(self):
        '''  Completely delete the instance of this Model from Redis '''
        if not self.good():
            raise ValueError

        with self:
            for name, ob in self._standalone.items():
                ob.type.delete(self.qualified(name, pk=self.primary_key), self.redis)

            self._delete_unique(self._unique_fields)
            self._remove_pk(self.primary_key, connection=self.redis)
            self.redis.delete(self.qualified(pk=self.primary_key))

        self.data.clear()

    @classmethod
    def get(cls, pks=None, start=None, stop=None, offset=None, limit=None, **ka):
        ''' Fetch a number of instances from Redis.

            1) `pks` is an iterable of primary keys
            2) If any of `start`, `stop`, `offset`, `limit` are provided it will 
            first load primary keys, using ZRANGEBYSCORE where
                min is `start` (or '-inf', if `start` is None)
                max is `stop` (or '+inf', if `stop` is None)
                start is `offset` (or 0, if `offset` is None)
                count is `limit` and it will be passed to redis-py without modifications
            3) get also supports keyword arguments. You're expected to provide exactly
            one `key` -> `value` pair where `key` is one of the unique fields defined for
            this model and `value` is an iterable of values to search for.

            These groups of parameters are mutually exclusive.
        '''

        z = any(x is not None for x in (start, stop, offset, limit))
        if sum((z, pks is not None, len(ka) == 1)) != 1:
            raise ValueError
            
        key = cls.qualified('_records')

        if z:
            if start is None:
                start = '-inf'

            if stop is None:
                stop = '+inf'

            pks = [cls.decode(PrimaryKey, x) for x in
                   cls.__redis__.zrangebyscore(key, start, stop, start=offset,
                                               num=limit)]

        if pks is not None:
            it = pks
        else:
            field, values = ka.popitem()
            with cls.get_pipeline() as pipe:
                for v in values:
                    cls._get_raw_pk_by_unique(field, v, pipe)
                it = (cls.decode(PrimaryKey, x) for x in pipe.execute())

        with cls.get_pipeline() as pipe:
            for x in it:
                cls._get_raw_by_pk(x, pipe)
            raw = pipe.execute()

        yield from cls.instances(*(cls._process_raw(r) for r in raw))
        
    def __enter__(self):
        if not self.__context_depth__:
            pipe = self.get_pipeline()
            self.redis = pipe.__enter__()
        self.__context_depth__ += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Must be set at the beginning of this method
        self.__context_depth__ -= 1
        if not self.__context_depth__:
            self.redis.execute()
            self.redis.__exit__(exc_type, exc_value, traceback)
            self.redis = self.__redis__

    def __repr__(self):
        return ("<{0.__name__}/{0._primary_key}={1!r} instance"
                " at {2:#x}>").format(type(self), self.primary_key, id(self))

    def __eq__(self, other):
        if type(self) is not type(other):
            return NotImplemented
        return self.primary_key == other.primary_key

    def __hash__(self):
        # Make it hashable (since I defined the __eq__ method)
        # The default hash makes more sense to me
        return super().__hash__()
