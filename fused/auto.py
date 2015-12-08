from . import proxies, exceptions
import ast


class autotype(proxies.commandproxy):

    def __init__(self, key, model):
        super().__init__(key, model)

    @classmethod
    def delete(cls, key, connection):
        connection.delete(key)

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

    def __init__(self, key, model, data=None):
        if data is None:
            data = self.fetch(key, model.__redis__)
        set.__init__(self, data)
        autotype.__init__(self, key, model)

    @classmethod
    def fetch(cls, key, connection):
        # Fetches the data immediately
        return connection.smembers(key)

    @classmethod
    def save(cls, key, connection, value):
        connection.sadd(key, *value)

    def add(self, elem):
        self.sadd(elem)
        return super().add(elem)

    def clear(self):
        self.delete(self.key, self.model.redis)
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

    def __init__(self, key, model, data=None):
        if data is None:
            data = self.fetch(key, model.__redis__)
        list.__init__(self, data)
        autotype.__init__(self, key, model)

    @classmethod
    def fetch(cls, key, connection):
        # Fetches the data immediately
        return connection.lrange(key, 0, -1)

    @classmethod
    def save(cls, key, connection, value):
        connection.rpush(key, *value)

    def append(self, elem):
        return self.extend((elem,))

    def extend(self, iterable):
        self.rpush(*iterable)
        return super().extend(iterable)

    def clear(self):
        self.delete(self.key, self.model.redis)
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

    def insert(self, index, value):
        if not index:
            self.lpush(value)
        elif index == len(self):
            self.rpush(value)
        else:
            raise exceptions.UnsupportedOperation
        return super().insert(index, value)

    def sort(self):
        raise exceptions.UnsupportedOperation

    def reverse(self):
        raise exceptions.UnsupportedOperation

    def __delitem__(self, index):
        return self.pop(index)

    def __setitem__(self, index, value):
        # TODO: Is there a better way?
        # TODO: Should these be pipelined?
        if not index:
            self.lpop()
            self.lpush(value)
        elif index == -1:
            self.rpop()
            self.rpush(value)
        else:
            raise exceptions.UnsupportedOperation
        return super().__setitem__(index, value)

    def __iadd__(self, other):
        self.extend(other)
        return self

    def __imul__(self, n):
        self.extend(self * (n - 1))
        return self

    # Default to_redis is fine, default from_redis isn't
    @classmethod
    def from_redis(cls, value, encoding=None):
        return ast.literal_eval(auto_str.from_redis(value, encoding))


class auto_int(int, autotype):

    def __new__(cls, key, model, data=None):
        if data is None:
            data = cls.fetch(key, model.__redis__)
        return super().__new__(cls, data)
        
    def __init__(self, key, model, data=None):
        autotype.__init__(self, key, model)

    @classmethod
    def fetch(cls, key, connection):
        return int(connection.get(key) or 0)

    @classmethod
    def save(cls, key, connection, value):
        return connection.set(key, value)

    @classmethod
    def from_redis(value, encoding=None):
        # Both str and bytes are supported
        return int(value)

    @classmethod
    def to_redis(value, encoding=None):
        # Convert it to str and then encode
        return auto_str.to_redis(str(value), encoding)

    def __iadd__(self, other):
        self.incr(other)
        new = type(self)(self.key, self.model, self + other)
        return new


class auto_str(str, autotype):

    def __new__(cls, key, model, data=None):
        if data is None:
            data = cls.fetch(key, model.__redis__)
        return super().__new__(cls, data)

    def __init__(self, key, model, data=None):
        autotype.__init__(self, key, model)

    @classmethod
    def fetch(cls, key, connection):
        return connection.get(key)

    @classmethod
    def save(cls, key, connection, value):
        return connection.set(key, value)

    # TODO: iadd