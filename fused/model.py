from . import fields, utils
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
        cls._standalone_proxy = {}
        cls._standalone_auto = {}
        cls._plain_fields = {}
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
                    

class BaseModel(metaclass=MetaModel):

    _field_sep = ':'

    def __init__(self, data=None, **ka):
        self.__context_depth__ = 0
        if data is not None:
            self.data = data.copy()
        else:
            # Will only search by one pair
            if len(ka) > 1:
                raise ValueError('Attempted to search by multiple fields;'
                                 'use get_by for that')
            field, value = ka.popitem()
            if field not in self._unique_fields:  
                raise TypeError('Attempted to get by non-unique'
                                ' field {!r}'.format(field))
            self.data = self._get_unique(field, value)

    def _get_unique(self, field, value):
        pk = self.__redis__.hget(self.qualified(field), value)
        res = self.__redis__.hgetall(self.qualified(pk=pk))
        for field, value in cls._plain_fields.items():
            res[field] = value.from_redis(res[field])

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
            raise Exception # TODO
        if cls._pk not in ka:
            raise Exception('No PK') # TODO

        # Must have the same order
        keys, values = [], []
        for k in cls._unique_keys.keys() & ka.keys():
            keys.append(cls._unique_keys[k])
            values.append(ka[k])

        cls._scripts['unique'](args=[ka[cls._pk], json.dumps(values)],
                               keys=keys)

        # Set the rest of fields
        