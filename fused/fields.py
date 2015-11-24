from . import exceptions

class BaseField:

    _namesep = ':'

    def qualified(self, arg=None):
        parts = [self.model_name, self.name]
        if arg is not None:
            parts.append(arg)
        return self._namesep.join(parts)


class Field(BaseField):

    _auto_cache = {}

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
            try:
                return self._auto_cache[this, self.name]
            except KeyError:
                retv = self._auto_cache[this, self.name] = self._auto(key, this)
                return retv
        else:
            return commandproxy(key, this)

    def __set__(self, this, value):
        if not isinstance(value, autotype):
            raise TypeError('Field.__set__ only works for Auto fields')
        self._auto.save(self.qualified(), this, value)



# Proxy classes

class callproxy:

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
        pass

    def remove(self, elem):
        self.srem(elem)
        return super().remove(elem)

    def pop(self):
        elem = super().pop()
        self.srem(elem)
        return elem

    def update(self, *a):
        self.sadd(*set.union(*a))
        return super().update(*a)

    def symmetric_difference_update(self, other):
        return super().symmetric_difference_update(other)

    def intersection_update(self, ):
        return super().intersection_update()

    def discard(self, ):
        return super().discard()

    def difference_update(self, ):
        return super().difference_update()

    def clear(self):
        self.delete()
        return super().clear()

    def add(self, elem):
        self.sadd(elem)
        return super().add(elem)

    def __ixor__(self, ):
        return super().__ixor__()

    def __ior__(self, ):
        return super().__ior__()

    def __iand__(self, ):
        return super().__iand__()


class Set(Field):
    _auto = _Set
