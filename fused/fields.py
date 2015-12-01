from . import exceptions, utils
from collections import defaultdict
from weakref import WeakKeyDictionary
import ast
from . import proxies, auto


class Field:

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
        except KeyError:
            # TODO: Optimize auto fields by looking at model.data?
            if self.auto:
                rv = self.type(key, model)
            else:
                rv = proxies.commandproxy(key, model)
            self._cache[self.name][model] = rv
            return rv

    def __set__(self, model, value):
        if model is None:
            raise TypeError('Field.__set__ requires instance of '
                            '{!r}'.format(self.model_name))
        if self.unique:
            model._update_unique({self.name: value})
        elif self.standalone:
            self.type.save(model.qualified(self.name, pk=model.data[model._pk]),
                           model.__redis__, value)
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


class String(Field):
    type = auto.auto_str
    to_redis = type.to_redis
    from_redis = type.from_redis


class List(Field):
    type = auto.auto_list
    to_redis = type.to_redis
    from_redis = type.from_redis


class Set(Field):
    type = auto.auto_set
    to_redis = type.to_redis
    from_redis = type.from_redis


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
    to_redis = type.to_redis
    from_redis = type.from_redis


class PrimaryKey(String):
    pass


__all__ = ['Field'] + [s.__name__ for s in Field.__subclasses__()]