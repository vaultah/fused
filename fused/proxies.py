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
        self.key, self.model, self._cache = key, model, {}

    def __getattr__(self, attr):
        try:
            return self.get_instance(attr)
        except KeyError:
            rv = callproxy(self.key, self.model, attr)
            self.set_instance(attr, rv)
            return rv

    def __repr__(self):
        return '<command proxy for {!r} at {:#x}>'.format(
                    self.key, id(self))

    def set_instance(self, attr, new):
        self._cache[attr] = new

    def get_instance(self, attr):
        return self._cache[attr]
