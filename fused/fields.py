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

    def __get__(self, this, type):
        if this is None:
            raise TypeError('Expected instance of {!r}, '
                            'None found'.format(self.model_name))
        key = this.qualified(self.name, pk=this.data[this._pk])
        if not self.standalone:
            # Coerce the value and return an instance of the corresponding
            # Python type
            return this.data[self.name]

        try:
            return self._cache[self.name][this]
        except KeyError:
            # TODO: Optimize auto fields by looking at this.data?
            rv = self.type(key, this) if self.auto else commandproxy(key, this)
            self._cache[self.name][this] = rv
            return rv

    def __set__(self, this, value):
        if not self.standalone:
            raise TypeError('Field.__set__ only works for standalone fields')
        if this is None:
            raise TypeError('Expected instance of {!r}, '
                            'None found'.format(self.model_name))
        self.type.save(this.qualified(self.name, pk=this.data[this._pk]),
                       this.__redis__, value)

    def __delete__(self, this):
        # TODO
        pass

    @classmethod
    def from_redis(cls, value):
        return value.decode()

    @classmethod
    def to_redis(cls, value):
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

    def __init__(self, key, model):
        self.key, self.model = key, model

    def __getattr__(self, attr):
        return callproxy(self.key, self.model, attr)

    def __repr__(self):
        return '<command proxy for {!r} at {:#x}>'.format(
                    self.key, id(self))


class autotype(commandproxy):

    def __init__(self, key, model):
        super().__init__(key, model)

# Types

class _Set(autotype, set):

    def __init__(self, key, model):
        super().__init__(key, model)
        set.__init__(self, self.fetch())

    def fetch(self):
        # Fetches the data immediately+
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



class _List(autotype, list):

    def __init__(self, key, model):
        super().__init__(key, model)
        list.__init__(self, self.fetch())

    def fetch(self):
        # Fetches the data immediately
        return self.model.__redis__.lrange(self.key, 0, -1)

    @classmethod
    def save(cls, key, connection, value):
        # TODO: Is there a better way?
        # TODO: Should these be pipelined?
        connection.delete(key)
        connection.lpush(key, *value)

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
        elem = super().pop(index)
        if not index:
            self.lpop()
        elif index == -1:
            self.rpop()
        else:
            pass
        # TODO: *sigh*
        return elem

    def insert(self):
        # TODO: *sigh*
        return super().insert()

    def sort(self):
        # TODO: I don't know how to do this D:
        pass

    def __delitem__(self, index):
        # TODO: Slice delete
        pass

    def __setitem__(self, index):
        # TODO: Slice assignment
        pass

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