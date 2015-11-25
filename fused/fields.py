from . import exceptions, utils
from collections import defaultdict
from weakref import WeakKeyDictionary


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
        key = this.qualified(self.name, pk='')
        if not self.standalone:
            # Coerce the value and return an instance of the corresponding
            # Python type
            return 'TODO'

        try:
            return self._cache[self.name][this]
        except KeyError:
            rv = self.type(key, this) if self.auto else commandproxy(key, this)
            self._cache[self.name][this] = rv
            return rv

    def __set__(self, this, value):
        if not self.standalone:
            raise TypeError('Field.__set__ only works for standalone fields')
        if this is None:
            raise TypeError('Expected instance of {!r}, '
                            'None found'.format(self.model_name))
        self.type.save(this.qualified(self.name, pk=''), this, value)

    def __delete__(self, this):
        # TODO
        pass


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
        set.__init__(self.fetch())

    def fetch(self):
        # Fetches the data immediately
        return self.model.__redis__.smembers(self.key)

    @classmethod
    def save(cls, key, model, value):
        # TODO: Is there a better way?
        # TODO: Should these be pipelined?
        model.__redis__.delete(key)
        model.__redis__.sadd(key, *value)

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
        return self.symmetric_difference_update(other)

    def __ior__(self, other):
        return self.update(other)

    def __iand__(self, other):
        return self.intersection_update(other)


class _List(autotype, list):

    def __init__(self, key, model):
        super().__init__(key, model)
        list.__init__(self.fetch())

    def fetch(self):
        # Fetches the data immediately
        return self.model.__redis__.lrange(self.key, 0, -1)

    def sort(self):
        # TODO: I don't know how to do this D:
        pass

    def append(self, ):
        # self.
        return super().append()

    def clear(self, ):
        # self.
        return super().clear()

    def extend(self, ):
        # self.
        return super().extend()

    def insert(self, ):
        # self.
        return super().insert()

    def pop(self, ):
        # self.
        return super().pop()

    def remove(self, ):
        # self.
        return super().remove()



class List(Field):
    type = _List

class Set(Field):
    type = _Set


class PrimaryKey(Field):
    pass