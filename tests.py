import redis
from fused import fields, model, exceptions, proxies
import pytest


TEST_PORT = 6379
TEST_DB = 14
TEST_CONNECTION = redis.Redis(port=TEST_PORT, db=TEST_DB)


@pytest.fixture(autouse=True)
def flushdb():
    TEST_CONNECTION.flushdb()


class lightmodel(model.Model):
    redis = TEST_CONNECTION
    id = fields.PrimaryKey()
    proxy = fields.Set(standalone=True)


class automodel(model.Model):
    redis = TEST_CONNECTION
    id = fields.PrimaryKey()
    set = fields.Set(auto=True)
    list = fields.List(auto=True)
    int = fields.Integer(auto=True)
    str = fields.String(auto=True)
    bytes = fields.Bytes(auto=True)
    sortedset = fields.SortedSet(auto=True)
    hash = fields.Hash(auto=True)


class fulltestmodel(model.Model):
    redis = TEST_CONNECTION
    id = fields.PrimaryKey()
    unique = fields.String(unique=True)
    required = fields.String(required=True)
    proxy_set = fields.Set(standalone=True)
    auto_set = fields.Set(auto=True)
    plain_set = fields.Set()

# Test encoding

class decodetestmodel(model.Model):
    redis = redis.Redis(port=TEST_PORT, db=TEST_DB, decode_responses=True)
    id = fields.PrimaryKey()
    bytes = fields.Bytes()
    str = fields.String()
    plain_set = fields.Set()


class nodecodetestmodel(model.Model):
    redis = redis.Redis(port=TEST_PORT, db=TEST_DB, decode_responses=False)
    id = fields.PrimaryKey()
    bytes = fields.Bytes()
    str = fields.String()
    plain_set = fields.Set()


# Pair of models to test circular foreign relations

class foreign_a(model.Model):
    redis = TEST_CONNECTION
    id = fields.PrimaryKey()
    b_field = fields.Foreign('foreign_b')

class foreign_b(model.Model):
    redis = TEST_CONNECTION
    id = fields.PrimaryKey()
    a_field = fields.Foreign(foreign_a)


# A model with not-so-foreign relation
class self_foreign(model.Model):
    redis = TEST_CONNECTION
    id = fields.PrimaryKey()
    field = fields.Foreign('self_foreign')


class TestFields:

    def test_types(self):
        tm = lightmodel.new(id='A')
        # Primary key
        assert type(tm.id) is str
        # Proxy fields
        assert type(tm.proxy) is proxies.commandproxy
        assert type(tm.proxy.get) is proxies.callproxy

    @pytest.mark.parametrize('command,args,inverse,invargs', [
        ('HSET', (b'<string>', 1), 'HKEYS', ()),
        ('SADD', (b'<string>',), 'SMEMBERS', ()),
        ('ZADD', (b'<string>', 1), 'ZRANGE', (0, -1)),
        ('LPUSH', (b'<string>',), 'LRANGE', (0, -1))
    ])
    def test_proxy(self, command, args, inverse, invargs):
        tm = lightmodel.new(id='A')
        proxy = getattr(tm.proxy, command.lower())
        iproxy = getattr(tm.proxy, inverse.lower())
        assert all(x == y for x, y in zip(args, iproxy(*invargs)))

    @pytest.mark.parametrize('field,value', [
        ('set', {'1', '2', '3'}),
        ('list', ['1', '2', '3']),
        ('int', 123),
        ('str', '123'),
        ('bytes', b'123'),
        ('sortedset', {'a': 1, 'b': 2, 'c': 3}),
        ('hash', {'a': 'b', 'b': 'c', 'c': 'd'})
    ])
    def test_auto_setget(self, field, value):
        tm = automodel.new(id='A')
        assert getattr(tm, field) == type(value)()
        setattr(tm, field, value)
        assert getattr(tm, field) == value

    @pytest.mark.parametrize('field,value', [
        ('set', {'1', '2', '3'}),
        ('list', ['1', '2', '3']),
        ('int', 123),
        ('str', '123'),
        ('bytes', b'123'),
        ('sortedset', {'a': 1, 'b': 2, 'c': 3}),
        ('hash', {'a': 'b', 'b': 'c', 'c': 'd'})
    ])
    def test_auto_delete(self, field, value):
        tm = automodel.new(id='A')
        setattr(tm, field, value)
        delattr(tm, field)
        assert getattr(tm, field) == type(value)()


class TestModelCreation:

    def test_new_plain(self):
        # Plain fields
        ka = {'id': 'A', 'unique': '<string>', 'required': '',
              'plain_set': {1, 2, 3}}
        new = fulltestmodel.new(**ka)
        for k in ka:
            assert ka[k] == getattr(new, k)

        # Load and test again
        reloaded = fulltestmodel(id=ka['id'])

        for k in ka:
            assert ka[k] == getattr(reloaded, k)

    def test_new_uniqueness(self):
        ka = {'id': 'A', 'unique': '<string>', 'required': ''}
        fulltestmodel.new(**ka)
        ka['id'] = 'B'
        with pytest.raises(exceptions.DuplicateEntry):
            fulltestmodel.new(**ka)

    def test_new_pk_uniqueness(self):
        new = lightmodel.new(id='A')
        with pytest.raises(exceptions.DuplicateEntry):
            lightmodel.new(id='A')

    def test_new_auto(self):
        val = {'1', '2', '3'}
        new = automodel.new(id='A', set=val)
        assert new.set == val 
        val = val.copy()
        val.add('4')
        new.set = val
        assert new.set == val
        reloaded = automodel(primary_key='A')
        assert reloaded.set == val

    def test_new_proxy(self):
        val = {b'1', b'2', b'3'}
        ka = {'id': 'A', 'unique': '<string>', 'required': '', 'proxy': val}
        new = lightmodel.new(**ka)
        assert isinstance(new.proxy, proxies.commandproxy)
        assert new.proxy.smembers() == val


class TestModelUpdate:

    def test_instant_update_plain(self):
        ka = {'id': 'A', 'unique': '<string>', 'required': ''}
        new = fulltestmodel.new(**ka)
        new.required = 'new value'
        assert new.required == 'new value'
        reloaded = fulltestmodel(id=ka['id'])
        assert reloaded.required == 'new value'

    def test_instant_update_unique(self):
        ka1 = {'id': 'A', 'unique': '<string 1>', 'required': ''}
        ka2 = {'id': 'B', 'unique': '<string 2>', 'required': ''}
        new = fulltestmodel.new(**ka1)
        other = fulltestmodel.new(**ka2)
        # Can we update it?
        new.unique = '<string>'
        assert new.unique == '<string>'
        # Does reloading change anything?
        reloaded = fulltestmodel(id=ka1['id'])
        assert reloaded.unique == '<string>'

        with pytest.raises(exceptions.DuplicateEntry):
            new.unique = other.unique

    # TODO: should we test standalone fields?


class TestModelLoad:
    pass # TODO


class TestModel:

    def test_load(self):
        ka = {'id': 'A', 'unique': '<string>', 'required': ''}
        new = fulltestmodel.new(**ka)
        # By the primary key
        reloaded = fulltestmodel(id=ka['id'])
        for k in ka:
            assert ka[k] == getattr(reloaded, k)

        # By unique field
        reloaded = fulltestmodel(unique=ka['unique'])
        for k in ka:
            assert ka[k] == getattr(reloaded, k)

    def test_new_foreign(self):
        # Model.new(field=X) where X is an instance of some
        # foreign type must return an instance of Model with
        # 'field' set to X
        fa = foreign_a.new(id='A', b_field='B')
        fb = foreign_b.new(id='B', a_field='A') 
        assert not fa.b_field.good()
        assert fb.good()
        # TODO?: Not to sure if this is the desired behaviour
        assert fa.b_field.a_field is None

        new_a = foreign_a.new(id='C', b_field=fb)
        # Ensure it wasn't reloaded
        assert new_a.b_field is fb
        assert new_a.b_field.a_field == fa
        reloaded = foreign_a(id='C')
        assert reloaded.b_field == fb
        assert reloaded.b_field.a_field == fa

    def test_load_foreign(self):
        fa = foreign_a.new(id='A', b_field='B')
        fb = foreign_b.new(id='B', a_field='A')
        # Load foreign_a
        la = foreign_a(id='A')
        assert isinstance(la.b_field, foreign_b)
        assert isinstance(la.b_field.a_field, foreign_a)
        # Test that objects were not reloaded
        assert la.b_field.a_field is la
        assert la.b_field.a_field.b_field is la.b_field

    def test_self_foreign(self):
        sf1 = self_foreign.new(id='A', field='B')
        sf2 = self_foreign.new(id='B', field='A')
        # Load foreign_a
        ls1 = self_foreign(id='A')
        assert isinstance(ls1.field, self_foreign)
        assert isinstance(ls1.field.field, self_foreign)
        assert ls1.field.field is ls1
        # Lel
        assert ls1.field.field.field.field is ls1

    def test_eq_hash(self):
        lightmodel.new(id='A')
        r1 = lightmodel(id='A')
        r2 = lightmodel(id='A')
        assert r1 != 'something'
        assert r1 == r2
        assert hash(r1) == hash(r2)
        # Different model, same id
        f = automodel.new(id='A')
        assert f != r1
        assert hash(f) != hash(r1)

    def test_count(self):
        assert lightmodel.count() == 0
        new = lightmodel.new(id='A')
        assert lightmodel.count() == 1
        new = lightmodel.new(id='B')
        assert lightmodel.count() == 2

    def test_get_by_pks(self):
        instances = [lightmodel.new(id='<primary key {}>'.format(i))
                        for i in range(10)]
        lst = list(lightmodel.get(x.primary_key for x in instances))
        assert len(lst) == 10
        assert all(x == y for x, y in zip(instances, lst))
        
    def test_get_by_unique(self):
        instances = []
        for i in range(10):
            new = fulltestmodel.new(id='<primary key {}>'.format(i),
                                    required='', unique=str(i))
            instances.append(new)
        lst = list(fulltestmodel.get(unique=[str(x) for x in range(10)]))
        assert len(lst) == 10
        assert all(x == y for x, y in zip(instances, lst))

    def test_get_zrange(self):
        instances = []
        instances.append(lightmodel.new(id=(0, '1')))
        instances.append(lightmodel.new(id=(2, '2')))
        instances.append(lightmodel.new(id=(50, '3')))
        instances.append(lightmodel.new(id=('+inf', '4')))
        lst = list(lightmodel.get(offset=0, limit=2))
        assert len(lst) == 2
        assert lst == instances[:2]
        lst = list(lightmodel.get(offset=2, limit=2))
        assert len(lst) == 2
        assert lst == instances[2:4]
        lst = list(lightmodel.get(start=2, stop=40))
        assert len(lst) == 1
        assert lst == instances[1:2]
        lst = list(lightmodel.get(start=2, stop=50))
        assert len(lst) == 2
        assert lst == instances[1:3]
        lst = list(lightmodel.get(offset=1, start=2, stop=50, limit=10))
        assert len(lst) == 1
        assert lst == instances[2:3]

    def test_delete_plain(self):
        new = fulltestmodel.new(id='A', unique='<string>', required='')
        del new.required
        assert new.required is None
        del new.unique
        assert new.unique is None
        reloaded = fulltestmodel(id='A')
        assert reloaded.required is None
        assert reloaded.unique is None
        # Doesn't raise
        fulltestmodel.new(id='B', unique='<string>', required='')

    def test_delete_standalone(self):
        new = lightmodel.new(id='A')
        new.proxy.sadd(b'1')
        assert new.proxy.smembers() == {b'1'}
        del new.proxy
        assert new.proxy.smembers() == set()

    def test_delete(self):
        assert lightmodel.count() == 0
        new = lightmodel.new(id='A')
        assert lightmodel.count() == 1
        new.proxy.sadd(b'1')
        new.set = {1, 2, 3}
        assert isinstance(new.proxy, proxies.commandproxy)
        assert isinstance(new.set, set)
        new.delete()
        assert lightmodel.count() == 0
        assert not new.good()
        assert new.proxy is None

        reloaded = lightmodel(id='A')
        assert lightmodel.count() == 0
        assert not reloaded.good()
        assert reloaded.proxy is None


class TestEncoding:

    # TODO: standalone
 
    def test_decode_responses(self):
        ka = {'id': 'A', 'str': '¿Cómo está usted?', 
              'bytes': b'\x00 and some more chars',
              'plain_set': {1, 2, 3, 'a', b'b', ('c',)}}
        new = decodetestmodel.new(**ka)
        for k in ka:
            assert ka[k] == getattr(new, k)

        reloaded = decodetestmodel(id=ka['id'])
        # If decode_responses is True, it will raise
        # AssertionError for 'bytes', this is by design
        for k in ka.keys() - {'bytes'}:
            assert ka[k] == getattr(reloaded, k)

        assert ka['bytes'] != reloaded.bytes

    def test_no_decode_responses(self):
        ka = {'id': 'A', 'str': '¿Cómo está usted?',
              'bytes': b'\x00 and some more chars',
              'plain_set': {1, 2, 3, 'a', b'b', ('c',)}}
        new = nodecodetestmodel.new(**ka)
        for k in ka:
            assert ka[k] == getattr(new, k)

        reloaded = nodecodetestmodel(id=ka['id'])
        # decode_responses is False, 'bytes' won't be decoded
        for k in ka.keys():
            assert ka[k] == getattr(reloaded, k)
