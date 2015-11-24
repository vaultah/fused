from . import fields
from abc import ABCMeta


class MetaModel(ABCMeta):

    def __new__(mcs, model_name, base, attrs):
        cls = super().__new__(mcs, model_name, base, attrs)
        cls._fields = {}
        # Pre-generated DB keys (they're constant)
        cls._unique_keys = {}
        cls._unique_fields = {}
        cls._required_fields = {}
        cls._standalone_proxy = {}
        cls._standalone_auto = {}

        field_attrs = ((k, v) for k, v in attrs.items()
                         if isinstance(v, fields.BaseField))
        for name, field in field_attrs:
            field.name, field.model_name = name, model_name
            cls._fields[name] = field

            if field.unique:
                cls._unique_fields[name] = field
                cls._unique_keys[name] = cls.qualified(name)

            if field.required:
                cls._required_fields[name] = field

        return cls
                    

class BaseModel(metaclass=MetaModel):

    _field_sep = ':'

    def __init__(self, **ka):
        self.__context_depth__ = 0
        self.__redis__ = self.redis
        for field in self._fields.values():
            field.model = self
        if ka:
            # Will only search by one pair
            if len(ka) > 1:
                raise ValueError('Attempted to search by multiple fields;'
                                 'use get_by for that')
            field, value = ka.popitem()
            if field not in self._unique_fields:  
                raise TypeError('Attempted to get by non-unique'
                                ' field {!r}'.format(field))
            self._data = self._get_unique(field, value)

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
