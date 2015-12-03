from . import exceptions, utils, proxies, auto
from abc import ABCMeta
from collections import defaultdict
from weakref import WeakKeyDictionary
import ast


class MetaField(ABCMeta):
    def __new__(mcs, field_name, bases, attrs):
        if 'type' in attrs:
            attrs.setdefault('to_redis', attrs['type'].to_redis)
            attrs.setdefault('from_redis', attrs['type'].from_redis)
        cls = super().__new__(mcs, field_name, bases, attrs)
        return cls


class Field(metaclass=MetaField):

    _cache = defaultdict(WeakKeyDictionary)

    def __init__(self, *, unique=False, standalone=False, auto=False,
                          required=False):

        if auto:
            standalone = True

        if unique:
            required = True

        self.unique = unique
        self.required = required
        self.standalone = standalone
        self.auto = auto

    def __get__(self, model, model_type):
        if model is None:
            raise TypeError('Field.__get__ requires instance of '
                            '{!r}'.format(self.model_name))
        key = model.qualified(self.name, pk=model.data[model._pk])
        if not self.standalone:
            # Return an instance of the corresponding Python type
            return model.data[self.name]

        try:
            return self._cache[self.name][model]
        except KeyError as e:
            # TODO: Optimize auto fields by looking at model.data?
            if self.auto:
                rv = self.type(key, model)
                rv.field = self 
            else:
                rv = proxies.commandproxy(key, model)
            self.update_instance(model, rv)
            return rv

    def __set__(self, model, value):
        if model is None:
            raise TypeError('Field.__set__ requires instance of '
                            '{!r}'.format(self.model_name))
        if self.unique:
            model._update_unique({self.name: value})
        elif self.standalone:
            if not isinstance(value, self.type):
                # Update the DB
                key = model.qualified(self.name, pk=model.data[model._pk])
                self.type.save(key, model.__redis__, value)
                new = self.type(key, model, value)
            else:
                # Simply update the cache
                new = value
            self.update_instance(model, new)
        else:
            model._update_plain({self.name: value})

    def __delete__(self, model):
        if model is None:
            raise TypeError('Field.__delete__ requires instance of '
                            '{!r}'.format(self.model_name))
        if not self.standalone:
            model._delete_plain(self.name)
        else:
            model._delete_standalone(self.name)

    def update_instance(self, model, new):
        self._cache[self.name][model] = new


class String(Field):
    type = auto.auto_str


class List(Field):
    type = auto.auto_list


class Set(Field):
    type = auto.auto_set


class Bytes(Field):

    # There's no auto.auto_bytes... yet?

    @classmethod
    def from_redis(cls, value, encoding=None):
        # Return value unchanged
        return value

    @classmethod
    def to_redis(cls, value, encoding=None):
        # Return the value unchanged
        return value


class Integer(Field):
    type = auto.auto_int


class PrimaryKey(String):
    pass


__all__ = ['Field'] + [s.__name__ for s in Field.__subclasses__()]