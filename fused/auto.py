from . import proxies, exceptions
import ast


class autotype(proxies.commandproxy):

    def __init__(self, key, model):
        super().__init__(key, model)

    @classmethod
    def from_redis(cls, value, encoding=None):
        if isinstance(value, bytes) and encoding is not None:
            return value.decode(encoding)
        else:
            return value

    @classmethod
    def to_redis(cls, value, encoding=None):
        if encoding is not None:
            return str(value).encode(encoding)
        else:
            return str(value)


class auto_set(set, autotype):

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

    # Default to_redis is fine, default from_redis isn't
    @classmethod
    def from_redis(cls, value, encoding=None):
        return ast.literal_eval(auto_str.from_redis(value, encoding))


class auto_list(list, autotype):

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

    # Default to_redis is fine, default from_redis isn't
    @classmethod
    def from_redis(cls, value, encoding=None):
        print(value, encoding)
        return ast.literal_eval(auto_str.from_redis(value, encoding))


class auto_int(int, autotype):

    def __init__(self, key, model):
        autotype.__init__(self, key, model)
        int.__init__(self, self.fetch())

    def fetch(self):
        return int(self.model.__redis__.get(self.key))

    @classmethod
    def save(cls, key, connection, value):
        return connection.set(key, value)

    # TODO: __iadd__ for efficient INCR?

    @classmethod
    def from_redis(value, encoding=None):
        # Both str and bytes are supported
        return int(value)

    @classmethod
    def to_redis(value, encoding=None):
        # Convert it to str and then encode
        return auto_str.to_redis(str(value), encoding)


class auto_str(str, autotype):

    def __init__(self, key, model):
        autotype.__init__(self, key, model)
        str.__init__(self, self.fetch())

    def fetch(self):
        return self.model.__redis__.get(self.key)

    @classmethod
    def save(cls, key, connection, value):
        return connection.set(key, value)