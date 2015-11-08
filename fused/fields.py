from functools import partial


class BaseField:

    _namesep = ':'

    def qualified(self, arg=None):
        parts = [self.model, self.name]
        if arg is not None:
            parts.append(arg)
        return self._namesep.join(parts)


class Field(BaseField):

    def __init__(self, unique=False, indexable=False, required=False):
        if unique:
            indexable = True
        self.unique = unique
        self.indexable = indexable
        self.required = required


    def __get__(self, this, _type):
        if this is None:
            raise TypeError('Expected instance, None found')
        class attrproxy:
            def __getattr__(_, name):
                return partial(getattr(this.redis, name), self.qualified())
        return attrproxy()

