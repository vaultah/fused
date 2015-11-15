from . import fields
from abc import ABCMeta


class MetaModel(ABCMeta):

    def __new__(mcs, model_name, base, attrs):
        cls = super().__new__(mcs, model_name, base, attrs)
        cls._fields = {}
        cls._unique_fields = {}
        cls._indexable_fields = {}
        cls._required_fields = {}

        field_attrs = ((k, v) for k, v in attrs.items()
                         if isinstance(v, fields.BaseField))
        for name, field in field_attrs:
            field.name, field.model_name = name, model_name
            cls._fields[name] = field

            if field.unique:
                cls._unique_fields[name] = field

            if field.required:
                cls._required_fields[name] = field

            if field.indexable:
                cls._indexable_fields[name] = field
        return cls
                    

class BaseModel(metaclass=MetaModel):

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
            field = next(iter(ka))
            if field not in self._unique_fields:  
                raise TypeError('Attempted to get by non-unique'
                                ' field {!r}'.format(field))
            self._data = self._get_unique(**ka)

    def __enter__(self):
        if not self.__context_depth__:
            self.redis = self.__redis__.pipeline()
        self.__context_depth__ += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Must be set at the beginning of this method
        self.__context_depth__ -= 1
        if not self.__context_depth__:
            self.redis.execute()
            self.redis = self.__redis__

    def _in_cm(self):
        return self.__context_depth__ != 0