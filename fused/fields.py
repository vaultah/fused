from . import exceptions, utils, proxies
import abc
import ast
import redis

class Field(metaclass=abc.ABCMeta):

    def __init__(self, *, unique=False, standalone=False, auto=False,
                          required=False):

        if auto:
            standalone = True

        self.unique = unique
        self.required = required
        self.standalone = standalone
        self.auto = auto

    def __get__(self, model, model_type):
        if model is None:
            raise TypeError('Field.__get__ requires instance of '
                            '{!r}'.format(self.model_name))
        if not model.good():
            return None

        if not self.standalone:
            return model.data.get(self.name)

        try:
            return self._get_instance(model)
        except KeyError as e:
            key = model.qualified(self.name, pk=model.primary_key)
            # TODO: Optimize auto fields by looking at model.data? No.
            if self.auto:
                # Return an instance of the corresponding Python type
                rv = self.fetch(key, model.__redis__, model.encoding)
            else:
                rv = proxies.commandproxy(key, model)
            self._set_instance(model, rv)
            return rv

    def __set__(self, model, value):
        if model is None:
            raise TypeError('Field.__set__ requires instance of '
                            '{!r}'.format(self.model_name))
        if self.unique:
            model._update_unique({self.name: value})
        elif self.standalone:
            if not self.auto:
                raise AttributeError("Can't assign to proxy fields")
            key = model.qualified(self.name, pk=model.primary_key)
            # Containers can't be reliably updated using just one command

            # Use the primary pipeline if we can
            if isinstance(model.redis, redis.client.Pipeline):
                pipe = model.redis
            else:
                pipe = model.get_pipeline()

            with pipe:
                self.save(key, pipe, value)
                pipe.execute()

            self._set_instance(model, value) # TODO copy?
        else:
            model._update_plain({self.name: value})

    def __delete__(self, model):
        if model is None:
            raise TypeError('Field.__delete__ requires instance of '
                            '{!r}'.format(self.model_name))
        if self.unique:
            model._delete_unique([self.name])
            model.data.pop(self.name, None)
        elif not self.standalone:
            model._delete_plain([self.name])
            model.data.pop(self.name, None)
        else:
            key = model.qualified(self.name, pk=model.primary_key)
            # TODO: Move this to a separate method?
            model.redis.delete(key)
            # TODO: Handle auto fields. The fuck you mean, past me?
            # TODO: replace with a default to avoid another DB request?
            model._field_cache.pop(self.name, None)

    def _set_instance(self, model, new):
        model._field_cache[self.name] = new

    def _get_instance(self, model):
        return model._field_cache[self.name]


class String(Field):

    @staticmethod
    def serialize(value, encoding=None):
        if encoding is not None:
            return str(value).encode(encoding)
        else:
            return str(value)
    
    @staticmethod
    def deserialize(value, encoding=None):
        if isinstance(value, bytes) and encoding is not None:
            return value.decode(encoding)
        else:
            return value

    @staticmethod
    def fetch(key, connection, encoding):
        # Fetches the data immediately
        return String.deserialize(connection.get(key) or '', encoding)

    @staticmethod
    def save(key, connection, value):
        connection.set(key, value)


class List(Field):

    serialize = staticmethod(String.serialize)

    @staticmethod
    def deserialize(value, encoding=None):
        return ast.literal_eval(String.deserialize(value, encoding))

    @staticmethod
    def fetch(key, connection, encoding):
        # Fetches the data immediately
        res = connection.lrange(key, 0, -1)
        return [String.deserialize(x, encoding) for x in res]

    @staticmethod
    def save(key, connection, value):
        connection.delete(key)
        connection.rpush(key, *value)


class Set(Field):

    serialize = staticmethod(String.serialize)

    @staticmethod
    def deserialize(value, encoding=None):
        return ast.literal_eval(String.deserialize(value, encoding))

    @staticmethod
    def fetch(key, connection, encoding): 
        # Fetches the data immediately
        res = connection.smembers(key)
        return {String.deserialize(x, encoding) for x in res}

    @staticmethod
    def save(key, connection, value):
        connection.delete(key)
        connection.sadd(key, *value)


class Bytes(Field):

    @staticmethod
    def serialize(value, encoding=None):
        # Return the value unchanged
        return value

    @staticmethod
    def deserialize(value, encoding=None):
        # Return the value unchanged
        return value

    @staticmethod
    def fetch(key, connection, encoding):
        return connection.get(key) or b''

    @staticmethod
    def save(key, connection, value):
        connection.set(key, value)



class Integer(Field):

    serialize = staticmethod(String.serialize)

    @staticmethod
    def deserialize(value, encoding=None):
        return int(value)

    @staticmethod
    def fetch(key, connection, encoding):
        return Integer.deserialize(connection.get(key) or 0)

    save = staticmethod(String.save)


class Hash(Field):

    serialize = staticmethod(String.serialize)

    @staticmethod
    def deserialize(value, encoding=None):
        return ast.literal_eval(String.deserialize(value, encoding))

    @staticmethod
    def fetch(key, connection, encoding):
        res = connection.hgetall(key)
        dm = lambda x: String.deserialize(x, encoding)
        return {dm(k): dm(v) for k, v in res.items()}

    @staticmethod
    def save(key, connection, value):
        if not isinstance(value, dict):
            value = dict(value)
        connection.delete(key)
        connection.hmset(key, value)


class SortedSet(Field):

    serialize = staticmethod(String.deserialize)

    @staticmethod
    def deserialize(value, encoding=None):
        return ast.literal_eval(String.deserialize(value, encoding))

    @staticmethod
    def fetch(key, connection, encoding):
        res = connection.zrange(key, start=0, end=-1, withscores=True)
        return {String.deserialize(k, encoding): v for k, v in res}

    @staticmethod
    def save(key, connection, value):
        if not isinstance(value, dict):
            value = dict(value)
        connection.delete(key)
        connection.zadd(key, **value)


# TODO: Add Integer (AI) field?
class PrimaryKey(String):
    pass


class Foreign(String):

    def __init__(self, foreign, **ka):
        self.foreign = foreign
        super().__init__(**ka)