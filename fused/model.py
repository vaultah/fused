from . import fields, utils, exceptions
import redis
from abc import ABCMeta
import json


class MetaModel(ABCMeta):

    def __new__(mcs, model_name, base, attrs):
        cls = super().__new__(mcs, model_name, base, attrs)
        cls._pk = None
        cls._fields = {}
        # Pre-generated DB keys (they're constant)
        cls._unique_keys = {}
        cls._unique_fields = {}
        cls._required_fields = {}
        cls._plain_fields = {}
        cls._standalone_proxy = {}
        cls._standalone_auto = {}
        cls._scripts = {}

        field_attrs = ((k, v) for k, v in attrs.items()
                         if isinstance(v, fields.BaseField))

        for name, field in field_attrs:
            field.name, field.model_name = name, model_name
            cls._fields[name] = field
            if isinstance(field, fields.PrimaryKey):
                cls._pk = name

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

        return cls
                    

class Model(metaclass=MetaModel):

    _field_sep = ':'

    def __init__(self, data=None, **ka):
        self.__context_depth__ = 0
        if data is not None:
            self.data = data.copy()
        else:
            # Will only search by one pair
            if len(ka) > 1:
                raise ValueError('You can only search by 1 unique field')
            field, value = ka.popitem()
            if field == self._pk:
                self.data = self._get_by_pk(value)
            elif field not in self._unique_fields:  
                raise TypeError('Attempted to get by non-unique'
                                ' field {!r}'.format(field))
            else:
                self.data = self._get_unique(field, value)

    def _get_by_pk(self, pk):
        res = self.__redis__.hgetall(self.qualified(pk=pk))
        rv = {}
        for field, value in self._plain.items():
            rkey = fields.String.to_redis(field)
            try:
                rv[field] = value.from_redis(res[rkey])
            except KeyError as e:
                continue
        return rv

    def _get_unique(self, field, value):
        pk = self.__redis__.hget(self.qualified(field), value)
        return self._get_by_pk(fields.PrimaryKey.from_redis(pk))

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
    def new(cls, **ka):
        if cls._required_fields.keys() - ka.keys():
            raise exceptions.MissingFields
            
        if cls._pk not in ka:
            raise exceptions.NoPrimaryKey

        pk = ka[cls._pk]
        
        if cls._unique_fields:
            # Must have the same order
            keys, values, fields = [], [], []
            for k in cls._unique_keys.keys() & ka.keys():
                keys.append(cls._unique_keys[k])
                fields.append(k)
                values.append(ka[k])

            res = cls._scripts['unique'](args=[pk, json.dumps(values)],
                                         keys=keys)
            # 0 for success
            # 1 ... len(fields) is an error
            #       (position of the first duplicate field from 'fields')
            if res:
                res -= 1
                raise exceptions.DuplicateEntry(fields[res], values[res])

        main_key = cls.qualified(pk=pk)
        
        # Set the rest of fields

        # All standalone fields
        for field, ob in cls._standalone.items():
            try:
                value = ka[field]
            except KeyError:
                continue
            else:
                ob.type.save(cls.qualified(field, pk=pk), cls.__redis__, value)

        # Unique and plain fields
        save, data = {cls._pk: pk}, {cls._pk: pk}
        for field, ob in cls._plain.items():
            try:
                value = ka[field]
            except KeyError:
                continue
            else:
                save[field], data[field] = ob.to_redis(value), value

        cls.__redis__.hmset(main_key, save)
        return cls(data)