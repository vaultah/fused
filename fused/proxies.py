from collections import defaultdict
from weakref import WeakKeyDictionary


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
