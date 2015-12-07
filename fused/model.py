from . import fields, utils, exceptions
from abc import ABCMeta
import redis
import json
import time


class MetaModel(ABCMeta):

    def __new__(mcs, model_name, bases, attrs):
        mappings = ('_fields', '_unique_keys', '_unique_fields',
                    '_required_fields', '_plain_fields', '_standalone_proxy',
                    '_standalone_auto', '_scripts', '_foreign')
        for m in mappings:
            attrs[m] = {}
        cls = super().__new__(mcs, model_name, bases, attrs)
        cls._pk = None

        field_attrs = ((k, v) for k, v in attrs.items()
                         if isinstance(v, fields.Field))

        for name, field in field_attrs:
            field.name, field.model_name = name, model_name
            cls._fields[name] = field
            if isinstance(field, fields.PrimaryKey):
                cls._primary_key = name

            if isinstance(field, fields.Foreign):
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
            cls._redis_encoding = _params['encoding']

        return cls
                    

class Model(metaclass=MetaModel):

    _field_sep = ':'

    def __init__(self, data=None, **ka):
        self.__context_depth__ = 0
        self.data = {}
        # If the PK is present, we assume that the rest of fields
        # are there as well
        if data is None or self._primary_key not in data:
            if len(ka) > 1:
                raise ValueError('You can only search by 1 unique field')
            # Will only search by one pair
            field, value = ka.popitem()
            if field == self._primary_key:
                self.data.update(self._get_by_pk(value))
            elif field not in self._unique_fields:  
                raise TypeError('Attempted to get by non-unique'
                                ' field {!r}'.format(field))
            else:
                self.data.update(self._get_unique(field, value))

        self.data.update(data or {})
        self._prepare()

    @property
    def primary_key(self):
        return self.data[self._primary_key]

    @classmethod
    def _process_raw(cls, result):
        rv = {}
        for key, value in result.items():
            decoded = fields.String.from_redis(key, cls._redis_encoding)
            ob = cls._plain[decoded]
            rv[decoded] = ob.from_redis(value, cls._redis_encoding)
        return rv
        
    @classmethod            
    def _get_by_pk(cls, pk, connection=None):
        if connection is None:
            res = cls.__redis__.hgetall(cls.qualified(pk=pk))
        else:
            res = connection.hgetall(cls.qualified(pk=pk))
        return res

    @classmethod
    def _get_unique(cls, field, value, connection=None):
        if connection is None:
            pk = cls.__redis__.hget(cls.qualified(field), value)
        else:
            pk = connection.hget(cls.qualified(field), value)
        decoded = fields.PrimaryKey.from_redis(pk, cls._redis_encoding)
        return cls._get_by_pk(decoded)

    def _update_plain(self, new_data):
        save = new_data.copy()
        for k, v in save.items():
            save[k] = self._plain[k].to_redis(v, self._redis_encoding)
        self.redis.hmset(self.qualified(pk=self.primary_key), save)
        self.data.update(new_data)

    def _update_unique(self, new_data):
        self._write_unique(new_data, self.primary_key, self.redis)
        self._update_plain(new_data)

    # TODO: Delete

    @classmethod
    def get_foreign(cls, name=None):
        if name is None:
            return list(cls._foreign)
        return [k for k, v in cls._foreign.items() if v.foreign == name]

    def _prepare(self):
        for field, ob in self._foreign.items():
            if field not in self.data:
                continue
            # TODO: There should be a better way
            gen = (t for t in Model.__subclasses__()
                      if t.__name__ == ob.foreign)
            ft, fv = next(gen), self.data[field]
            if not isinstance(fv, ft):
                ff = ft.get_foreign(type(self).__name__)
                self.data[field] = ft(data=dict.fromkeys(ff, self),
                                      **{ft._primary_key: fv})
    @classmethod
    def get_pipeline(cls):
        return cls.__redis__.pipeline()

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

    @classmethod
    def qualified(cls, *args, pk=None):
        parts = [cls.__name__]
        if pk is not None:
            parts.append(pk)
        parts.extend(args)
        return cls._field_sep.join(parts)

    @classmethod
    def _write_unique(cls, data, pk, connection=None):
        # Must have the same order
        keys, values, fields = [], [], []
        for k, v in data.items():
            keys.append(cls._unique_keys[k])
            fields.append(k)
            values.append(v)

        kwargs = {'args': [pk, json.dumps(values)], 'keys': keys}
        if connection is not None:
            kwargs['client'] = connection
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
        result = cls._scripts['primary_key'](args=[pk, score],
                                             keys=[cls.qualified('_records')])
        if not result:
            raise exceptions.DuplicateEntry

    @classmethod
    def new(cls, **ka):
        if cls._required_fields.keys() - ka.keys():
            raise exceptions.MissingFields
            
        if cls._primary_key not in ka:
            raise exceptions.NoPrimaryKey

        pk = ka[cls._primary_key]
        main_key = cls.qualified(pk=pk)
        
        # TODO: PK score
        cls._write_pk(pk)
            
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
                save[field] = ob.to_redis(value, cls._redis_encoding)
                data[field] = value

        cls.__redis__.hmset(main_key, save)
        return cls(data)

    @classmethod
    def get(cls, pks=None, start=None, stop=None, offset=None, limit=None, **ka):
        # TODO: Checking

        key = cls.qualified('_records')

        if any(x is not None for x in (start, stop, offset, limit)):
            if start is None:
                start = '-inf'

            if stop is None:
                stop = '+inf'

            pks = self.__redis__.zrevrangebyscore(key, start, stop,
                                                  start=offset, num=limit)

        if pks is not None:
            it = ({cls._primary_key: pk} for pk in pks)
        else:
            field, values = ka.popitem()
            it = ({field: v} for v in values)

        with cls.get_pipeline() as pipe:
            # TODO: Use get_by_pk and get_unique
            pass
            
    @classmethod
    def count(cls):
        return cls.__redis__.zcard(cls.qualified('_records'))

    def __repr__(self):
        return ("<{0.__name__}/{0._primary_key}={1!r} instance"
                " at {2:#x}>").format(type(self), self.primary_key, id(self))

    def __eq__(self, other):
        if type(self) is not type(other):
            return NotImplemented
        return self.primary_key == other.primary_key

    def __hash__(self):
        # Make it hashable (since I defined the __eq__ method)
        return super().__hash__()