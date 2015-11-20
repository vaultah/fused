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
        return self._type(key, this) if self.auto else attrproxy(key, this)


# Proxy classes

class callproxy:

    def __init__(self, key, model, attr):
        self.key, self.model, self.attr = key, model, attr

    def __call__(self, *a, **ka):
        meth = getattr(self.model.redis, self.attr)
        return meth(self.key, *a, **ka)


class attrproxy:

    def __init__(self, key, model):
        self.key, self.model = key, model

    def __getattr__(self, attr):
        return callproxy(self.key, self.model, attr)


class autotype:

    def __init__(self, key, model):
        self.key, self.model = key, model
        super().__init__(self._get())


# Types

class _Set(autotype, set):
    def _get(self):
        return self.model.__redis__.smembers(self.key)


class _List(autotype, list):
    def _get(self):
        return self.model.__redis__.lrange(self.key, 0, -1)


class _String(autotype, str):
    def _get(self):
        return self.model.__redis__.get(self.key)

class _Integer(autotype, int):
    def _get(self):
        return "TODO"

class _Hash(autotype, dict):
    def _get(self):
        return {}

class Set(Field):
    _type = _Set

class List(Field):
    _type = _List

class Integer(Field):
    _type = _Integer

class String(Field):
    _type = _String

class Hash(Field):
    _type = _Hash
