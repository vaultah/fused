from . import exceptions, utils
from collections import defaultdict
from weakref import WeakKeyDictionary
import ast


class BaseField:
    pass


class Field(BaseField):

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
            rv = self.type(key, model) if self.auto else commandproxy(key, model)
            self._cache[self.name][model] = rv
            return rv

    def __set__(self, model, value):
        if model is None:
            raise TypeError('Field.__set__ requires instance of '
                            '{!r}'.format(self.model_name))
        if not self.standalone:
            raise TypeError('Field.__set__ only works for standalone fields')
        self.type.save(model.qualified(self.name, pk=model.data[model._pk]),
                       model.__redis__, value)

    def __delete__(self, model):
        if model is None:
            raise TypeError('Field.__delete__ requires instance of '
                            '{!r}'.format(self.model_name))
        # TODO

    @classmethod
    def from_redis(cls, value):
        # redis-py returns bytes
        return value.decode()

    @classmethod
    def to_redis(cls, value):
        # Create
        return str(value).encode()


# Proxy classes

class callproxy:

    __slots__ = ('key', 'attr', 'method')

    def __init__(self, key, model, attr):
        self.key, self.attr = key, attr
        self.method = getattr(model.redis, attr)

    def __call__(self, *a, **ka):
        return self.method(self.key, *a, **ka)

    def __repr__(self):
        return '<{!r} proxy for {!r} at {:#x}>'.format(
                    self.attr.upper(), self.key, id(self))


class commandproxy:

    _cache = defaultdict(WeakKeyDictionary)

    def __init__(self, key, model):
        self.key, self.model = key, model

    def __getattr__(self, attr):
        try:
            return self._cache[attr][self.model]
        except KeyError:
            rv = callproxy(self.key, self.model, attr)
            self._cache[attr][self.model] = rv
            return rv

    def __repr__(self):
        return '<command proxy for {!r} at {:#x}>'.format(
                    self.key, id(self))


class autotype(commandproxy):

    def __init__(self, key, model):
        super().__init__(key, model)

# Types

class _Set(set, autotype):

    def __init__(self, key, model):
        autotype.__init__(self, key, model)
        set.__init__(self, self.fetch())

    def fetch(self):
        # Fetches the data immediately
        return self.model.__redis__.smembers(self.key)

    @classmethod
    def save(cls, key, connection, value):
        # TODO: Is there a better way?
        # TODO: Should these be pipelined?
        connection.delete(key)
        connection.sadd(key, *value)

    def add(self, elem):
        self.sadd(elem)
        return super().add(elem)

    def clear(self):
        self.delete()
        return super().clear()

    def pop(self):
        elem = super().pop()
        self.srem(elem)
        return elem

    def remove(self, elem):
        self.srem(elem)
        return super().remove(elem)

    def discard(self, elem):
        self.srem(elem)
        return super().discard(elem)

    def update(self, *other):
        union = set.union(*other)
        self.sadd(*union)
        return super().update(union)

    def symmetric_difference_update(self, other):
        # (self ∪ other) ∖ (self ∩ other)
        # TODO: Should these be pipelined?
        self.sadd(*other)
        self.srem(*self.intersection(other))
        return super().symmetric_difference_update(other)

    def intersection_update(self, *other):
        # self \ (self \ (self.intersection(*other)))
        diff = self - self.intersection(*other)
        self.srem(*diff)
        return super().difference_update(diff)

    def difference_update(self, *other):
        union = set.union(*other)
        self.srem(*union)
        return super().difference_update(union)

    def __ixor__(self, other):
        self.symmetric_difference_update(other)
        return self

    def __ior__(self, other):
        self.update(other)
        return self

    def __iand__(self, other):
        self.intersection_update(other)
        return self

    def __repr__(self):
        return str([x for x in self])


class _List(list, autotype):

    # TODO: Better support for list methods?

    def __init__(self, key, model):
        # TODO:
        autotype.__init__(self, key, model)
        list.__init__(self, self.fetch())

    def fetch(self):
        # Fetches the data immediately
        return self.model.__redis__.lrange(self.key, 0, -1)

    @classmethod
    def save(cls, key, connection, value):
        # TODO: Is there a better way?
        # TODO: Should these be pipelined?
        connection.delete(key)
        connection.rpush(key, *value)

    def append(self, elem):
        return self.extend((elem,))

    def extend(self, iterable):
        self.rpush(*iterable)
        return super().extend(iterable)

    def clear(self):
        self.delete()
        return super().clear()

    def remove(self, elem):
        self.lrem(elem, 1)
        return super().remove(elem)

    def pop(self, index=-1):
        if not index:
            self.lpop()
        elif index == -1:
            self.rpop()
        else:
            raise exceptions.UnsupportedOperation
        return super().pop(index)

    def insert(self):
        raise exceptions.UnsupportedOperation

    def sort(self):
        raise exceptions.UnsupportedOperation

    def __delitem__(self, index):
        raise exceptions.UnsupportedOperation

    def __setitem__(self, index):
        raise exceptions.UnsupportedOperation

    def __iadd__(self, other):
        self.extend(other)
        return self

    def __imul__(self, n):
        self.extend(self * (n - 1))
        return self



class List(Field):
    type = _List

    # Default to_redis is fine, default from_redis isn't
    @classmethod
    def from_redis(cls, value):
        return ast.literal_eval(value.decode())


class Set(Field):
    type = _Set

    # Default to_redis is fine, default from_redis isn't
    @classmethod
    def from_redis(cls, value):
        return ast.literal_eval(value.decode())


class PrimaryKey(Field):
    pass

class String(Field):
    pass

class Integer(Field):
    pass


__all__ = ['Field'] + [s.__name__ for s in Field.__subclasses__()]