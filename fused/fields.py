from . import exceptions, utils, proxies, auto
from abc import ABCMeta
import ast


class MetaField(ABCMeta):

    def __new__(mcs, field_name, bases, attrs):
        if 'type' in attrs:
            attrs.setdefault('to_redis', attrs['type'].to_redis)
            attrs.setdefault('from_redis', attrs['type'].from_redis)
        cls = super().__new__(mcs, field_name, bases, attrs)
        return cls


class Field(metaclass=MetaField):

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
        if not self.standalone:
            # Return an instance of the corresponding Python type
            return model.data.get(self.name)

        try:
            return self.get_instance(model)
        except KeyError as e:
            key = model.qualified(self.name, pk=model.primary_key)
            # TODO: Optimize auto fields by looking at model.data? No.
            if self.auto:
                rv = self.type(key, model)
                rv.field = self 
            else:
                rv = proxies.commandproxy(key, model)
            self.set_instance(model, rv)
            return rv

    def __set__(self, model, value):
        if model is None:
            raise TypeError('Field.__set__ requires instance of '
                            '{!r}'.format(self.model_name))
        if self.unique:
            model._update_unique({self.name: value})
        elif self.standalone:
            if not isinstance(value, self.type):
                # Update the DB
                key = model.qualified(self.name, pk=model.primary_key)
                self.type.save(key, model.__redis__, value)
                new = self.type(key, model, value)
            else:
                # Simply update the cache
                new = value
            self.set_instance(model, new)
        else:
            model._update_plain({self.name: value})

    # TODO:
    # def __delete__(self, model):
    #     if model is None:
    #         raise TypeError('Field.__delete__ requires instance of '
    #                         '{!r}'.format(self.model_name))
    #     if not self.standalone:
    #         model._delete_plain(self.name)
    #     else:
    #         model._delete_standalone(self.name)

    def set_instance(self, model, new):
        model._field_cache[self.name] = new

    def get_instance(self, model):
        return model._field_cache[self.name]


class String(Field):
    type = auto.auto_str


class List(Field):
    type = auto.auto_list


class Set(Field):
    type = auto.auto_set


class Bytes(Field):

    # There's no auto.auto_bytes... yet?

    @classmethod
    def from_redis(cls, value, encoding=None):
        # Return value unchanged
        return value

    @classmethod
    def to_redis(cls, value, encoding=None):
        # Return the value unchanged
        return value


class Integer(Field):
    type = auto.auto_int


# TODO: Add Integer (AI) field
class PrimaryKey(String):
    pass


class Foreign(String):

    def __init__(self, foreign, **ka):
        self.foreign = foreign
        super().__init__(**ka)


__all__ = ['Field'] + [s.__name__ for s in Field.__subclasses__()]