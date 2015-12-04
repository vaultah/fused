from . import fields, utils, exceptions
from abc import ABCMeta
import redis
import json


class MetaModel(ABCMeta):

    def __new__(mcs, model_name, bases, attrs):
        cls = super().__new__(mcs, model_name, bases, attrs)
        cls._pk = None
        cls._fields = {}
        cls._foreign = {}
        # Pre-generated DB keys (they're constant)
        cls._unique_keys = {}
        cls._unique_fields = {}
        cls._required_fields = {}
        cls._plain_fields = {}
        cls._standalone_proxy = {}
        cls._standalone_auto = {}
        cls._scripts = {}

        field_attrs = ((k, v) for k, v in attrs.items()
                         if isinstance(v, fields.Field))


        for name, field in field_attrs:
            field.name, field.model_name = name, model_name
            cls._fields[name] = field
            if isinstance(field, fields.PrimaryKey):
                cls._pk = name

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
            if cls._unique_fields:
                cls._scripts['unique'] = cls.__redis__.register_script(
                                                utils.SCRIPTS['unique'])
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
        if data is None or self._pk not in data:
            if len(ka) > 1:
                raise ValueError('You can only search by 1 unique field')
            # Will only search by one pair
            field, value = ka.popitem()
            if field == self._pk:
                self.data.update(self._get_by_pk(value))
            elif field not in self._unique_fields:  
                raise TypeError('Attempted to get by non-unique'
                                ' field {!r}'.format(field))
            else:
                self.data.update(self._get_unique(field, value))

        self.data.update(data or {})
        self._prepare()

    @classmethod            
    def _get_by_pk(cls, pk):
        res = cls.__redis__.hgetall(cls.qualified(pk=pk))
        rv = {}
        for key, value in res.items():
            decoded = fields.String.from_redis(key, cls._redis_encoding)
            ob = cls._plain[decoded]
            rv[decoded] = ob.from_redis(value, cls._redis_encoding)
        return rv

    @classmethod
    def _get_unique(cls, field, value):
        pk = cls.__redis__.hget(cls.qualified(field), value)
        decoded = fields.PrimaryKey.from_redis(pk, cls._redis_encoding)
        return cls._get_by_pk(decoded)

    def _update_plain(self, new_data):
        save = new_data.copy()
        for k, v in save.items():
            save[k] = self._plain[k].to_redis(v, self._redis_encoding)
        self.redis.hmset(self.qualified(pk=self.data[self._pk]), save)
        self.data.update(new_data)

    def _update_unique(self, new_data):
        self._write_unique(new_data, self.data[self._pk], self.redis)
        self._update_plain(new_data)

    # TODO
    def _delete_plain(self, *fields):
        self.redis.hdel(*fields)

    @classmethod
    def get_foreign(cls, name=None):
        if name is None:
            return list(cls._foreign)
        return [k for k, v in cls._foreign.items() if v.model_name == name]

    def _prepare(self):
        for field, ob in self._foreign.items():
            gen = (t for t in type(self).__subclasses__()
                            if t.__name__ == foreign)
            ft, fv = next(gen), self.data[field]
            if not isinstance(fv, ft):
                ff = ft.get_foreign(type(self).__name__)
                self.data[field] = ft(data=dict.fromkeys(ff, self),
                                      **{ft._pk: fvalue})

    def __enter__(self):
        if not self.__context_depth__:
            pipe = self.__redis__.pipeline()
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
    def new(cls, **ka):
        if cls._required_fields.keys() - ka.keys():
            raise exceptions.MissingFields
            
        if cls._pk not in ka:
            raise exceptions.NoPrimaryKey

        pk = ka[cls._pk]
        
        if cls._unique_fields:
            data = {k: ka[k] for k in 
                     cls._unique_keys.keys() & ka.keys()}
            cls._write_unique(data, pk)

        main_key = cls.qualified(pk=pk)
        
        # Set the rest of fields

        # All standalone fields
        with cls.__redis__.pipeline() as pipe:
            for field, ob in cls._standalone.items():
                try:
                    value = ka[field]
                except KeyError:
                    continue
                else:
                    ob.type.save(cls.qualified(field, pk=pk), pipe, value)
            pipe.execute()

        # Unique and plain fields
        save, data = {cls._pk: pk}, {cls._pk: pk}
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