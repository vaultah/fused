from . import exceptions

class BaseField:

    _namesep = ':'

    def qualified(self, arg=None):
        parts = [self.model_name, self.name]
        if arg is not None:
            parts.append(arg)
        return self._namesep.join(parts)


class Field(BaseField):

    def __init__(self, unique=False, indexable=False, required=False,
                       auto=False):
        if unique or auto:
            indexable = True

        self.unique = unique
        self.indexable = indexable
        self.required = required
        self.auto = auto

    def __get__(self, this, type):
        if this is None:
            raise TypeError('Expected instance, None found')
        key = self.qualified()
        if self.auto:
            return self._auto(key, this)
        else:
            return commandproxy(key, this, self)

    def __set__(self, this, value):
        if not isinstance(value, autotype):
            raise TypeError('Field.__set__ only works for Auto fields')
        self._auto.save(self.qualified(), this, value)



# Proxy classes

class callproxy:

    def __init__(self, key, model, attr):
        self.key, self.model, self.attr = key, model, attr

    def __call__(self, *a, **ka):
        meth = getattr(self.model.redis, self.attr)
        return meth(self.key, *a, **ka)

    def __repr__(self):
        return '<{!r} proxy for {!r} at {:#x}>'.format(
                    self.attr.upper(), self.key, id(self))


class commandproxy:

    def __init__(self, key, model, field):
        self.key, self.model, self.field = key, model, field

    def __getattr__(self, attr):
        if attr not in self.field._allowed_commands:
            raise exceptions.InvalidCommand('{!r} is an invalid command'
                                            ' for {!r}'.format(self.key))
        return callproxy(self.key, self.model, attr)

    def __repr__(self):
        return '<command proxy for {!r} at {:#x}>'.format(
                    self.key, id(self))


class autotype:

    def __init__(self, key, model):
        self.key, self.model = key, model
        super().__init__(self._get())


# Types

class _Set(autotype, set):
    def _get(self):
        return self.model.__redis__.smembers(self.key)
    @classmethod
    def save(cls, key, model, value):
        pass


class _List(autotype, list):
    def _get(self):
        return self.model.__redis__.lrange(self.key, 0, -1)
    @classmethod
    def save(cls, key, model, value):
        pass


class _String(autotype, str):
    def _get(self):
        return self.model.__redis__.get(self.key)
    @classmethod
    def save(cls, key, model, value):
        pass


class _Integer(autotype, int):
    def _get(self):
        return "TODO"
    @classmethod
    def save(cls, key, model, value):
        pass


class _Hash(autotype, dict):
    def _get(self):
        return {}
    @classmethod
    def save(cls, key, model, value):
        pass


class Set(Field):
    _auto = _Set
    _allowed_commands = frozenset('SADD SCARD SDIFF SDIFFSTORE SINTER '
                                  'SINTERSTORE SISMEMBER SMEMBERS SMOVE SPOP '
                                  'SRANDMEMBER SREM SUNION SUNIONSTORE '
                                  'SSCAN'.lower().split())

class List(Field):
    _auto = _List
    _allowed_commands = frozenset('BLPOP BRPOP BRPOPLPUSH LINDEX LINSERT LLEN '
                                  'LPOP LPUSH LPUSHX LRANGE LREM LSET LTRIM '
                                  'RPOP RPOPLPUSH RPUSH RPUSHX '.lower().split())

class Integer(Field):
    _auto = _Integer

class String(Field):
    _auto = _String

class Hash(Field):
    _auto = _Hash
