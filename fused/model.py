from . import fields

class MetaModel(type):

    def __new__(mcs, model, base, namespace):
        cls = super().__new__(mcs, model, base, namespace)
        cls._fields = {}
        cls._unique_fields = {}
        cls._indexable_fields = {}
        cls._required_fields = {}

        for name, ob in namespace.items():
            if isinstance(ob, fields.BaseField):
                ob.name, ob.model = name, model
                cls._fields[name] = ob

                if ob.unique:
                    cls._unique_fields[name] = ob

                if ob.required:
                    cls._required_fields[name] = ob

                if ob.indexable:
                    cls._indexable_fields[name] = ob
                    

class BaseModel(metaclass=MetaModel):

    def __init__(self, **ka):
        self.__context_depth__ = 0
        self.__redis__ = self.redis
        if ka:
            # Will only search by one pair
            if len(ka) > 1:
                raise ValueError('Attempted to search by multiple fields;'
                                 'use get_by for that')
            field = next(iter(ka))
            if field not in self._unique_fields:  
                raise TypeError('Attempted to get by non-unique'
                                ' field {!r}'.format(field))
            self._data = self._get_unique(**ka)

    def __enter__(self):
        if not self.__context_depth__:
            self.redis = self.__redis__.pipeline()
        self.__context_depth__ += 1
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Must be set at the beginning of this method
        self.__context_depth__ -= 1
        if not self.__context_depth__:
            self.redis.execute()
            self.redis = self.__redis__