import redis
from fused import fields, model, exceptions, proxies, auto
import pytest


TEST_PORT = 6379
TEST_DB = 14
TEST_CONNECTION = redis.Redis(port=TEST_PORT, db=TEST_DB)


@pytest.fixture(autouse=True)
def flushdb():
    TEST_CONNECTION.flushdb()


class litetestmodel(model.Model):
    redis = TEST_CONNECTION
    id = fields.PrimaryKey()
    standalone = fields.Set(standalone=True)
    set = fields.Set(auto=True)
    list = fields.List(auto=True)
    int = fields.Integer(auto=True)
    str = fields.String(auto=True)


class fulltestmodel(model.Model):
    redis = TEST_CONNECTION
    id = fields.PrimaryKey()
    unique = fields.String(unique=True)
    required = fields.String(required=True)
    proxy_set = fields.Set(standalone=True)
    auto_set = fields.Set(auto=True)
    plain_set = fields.Set()


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
        tm = litetestmodel.new(id='A')
        # Proxy fields
        assert type(tm.standalone) is proxies.commandproxy
        assert type(tm.standalone.get) is proxies.callproxy
        # Auto fields
        assert isinstance(tm.set, auto.autotype)
        assert isinstance(tm.set, auto.autotype)

    @pytest.mark.parametrize('command,args,inverse,invargs', [
        ('HSET', (b'<string>', 1), 'HKEYS', ()),
        ('SADD', (b'<string>',), 'SMEMBERS', ()),
        ('ZADD', (b'<string>', 1), 'ZRANGE', (0, -1)),
        ('LPUSH', (b'<string>',), 'LRANGE', (0, -1))
    ])
    def test_proxy(self, command, args, inverse, invargs):
        tm = litetestmodel.new(id='A')
        proxy = getattr(tm.standalone, command.lower())
        iproxy = getattr(tm.standalone, inverse.lower())
        assert all(x == y for x, y in zip(args, iproxy(*invargs)))


# TODO: Test augmented assignment operators as well?
# TODO: They're implemented using tested methods, I see no point in
# TODO: testing them specifically.


class TestSet:

    # def test_add(self):
    #     tm = litetestmodel.new(id='A')
    #     assert not tm.set
    #     assert not tm.set.smembers()
    #     tm.set.add(b'<string>')
    #     assert tm.set
    #     assert len(tm.set) == 1
    #     assert tm.set == {b'<string>'}
    #     assert tm.set.smembers()
    #     assert tm.set.smembers() == {b'<string>'}

    def test_sadd(self):
        tm = litetestmodel.new(id='A')
        assert not tm.set
        assert not tm.set.smembers()
        tm.set.sadd(b'<string>')
        assert tm.set
        assert len(tm.set) == 1
        assert tm.set == {b'<string>'}
        assert tm.set.smembers()
        assert tm.set.smembers() == {b'<string>'}

    def test_clear(self):
        tm = litetestmodel.new(id='A')
        tm.set.add(b'<string>')
        tm.set.clear()
        assert not tm.set
        assert not tm.set.smembers()

    def test_pop(self):
        tm = litetestmodel.new(id='A')
        tm.set.add(b'<string>')
        tm.set.pop()
        assert not tm.set
        assert not tm.set.smembers()
        with pytest.raises(KeyError):
            tm.set.pop()

    def test_remove(self):
        tm = litetestmodel.new(id='A')
        tm.set.add(b'<string>')
        tm.set.remove(b'<string>')
        assert not tm.set
        assert not tm.set.smembers()
        with pytest.raises(KeyError):
            tm.set.remove(b'<string>')

    def test_discard(self):
        tm = litetestmodel.new(id='A')
        tm.set.add(b'a')
        tm.set.discard(b'a')
        assert not tm.set
        assert not tm.set.smembers()
        # No exception if the element is not present
        tm.set.discard(b'a')

    def test_update(self):
        tm = litetestmodel.new(id='A')
        other = ({b'a', b'b', b'c', b'd'},
                 {b'e', b'f', b'g', b'h'})
        tm.set.update(*other)
        assert tm.set
        flat = {e for tup in other for e in tup}
        assert tm.set == flat
        assert tm.set.smembers() == flat

    def test_symmetric_difference_update(self):
        tm = litetestmodel.new(id='A')
        tm.set.add(b'a')
        tm.set.add(b'b')
        other = {b'b', b'c'}
        # The result of tm.set ^ other
        symdiff = {b'a', b'c'}
        assert tm.set ^ other == symdiff
        tm.set.symmetric_difference_update(other)
        assert tm.set == symdiff
        assert tm.set.smembers() == symdiff

    def test_intersection_update(self):
        tm = litetestmodel.new(id='A')
        tm.set.add(b'a')
        tm.set.add(b'b')
        tm.set.add(b'c')
        other = ({b'b', b'c', b'd'}, {b'c', b'd', b'e'})
        intersection = {b'c'}
        assert tm.set.intersection(*other) == intersection
        tm.set.intersection_update(*other)
        assert tm.set == intersection
        assert tm.set.smembers() == intersection

    def test_difference_update(self):
        tm = litetestmodel.new(id='A')
        tm.set.add(b'a')
        tm.set.add(b'b')
        tm.set.add(b'c')
        other = ({b'b'}, {b'c'})
        diff = {b'a'}
        assert tm.set.difference(*other) == diff
        tm.set.difference_update(*other)
        assert tm.set == {b'a'}
        assert tm.set.smembers() == {b'a'}

    def test_set(self):
        tm = litetestmodel.new(id='A')
        new = {b'1', b'2', b'3'}
        assert tm.set == set()
        tm.set = new
        assert tm.set == new
        assert tm.set.smembers() == new

class TestList:

    def test_append(self):
        tm = litetestmodel.new(id='A')
        assert not tm.list
        lst = [b'a', b'b']
        for x in lst:
            tm.list.append(x)
        assert tm.list
        assert len(tm.list) == 2
        assert tm.list == lst
        assert tm.list.lrange(0, -1) == lst

    def test_extend(self):
        tm = litetestmodel.new(id='A')
        assert not tm.list
        lst = [b'a', b'b']
        tm.list.extend(lst)
        assert tm.list
        assert len(tm.list) == 2
        assert tm.list == lst
        assert tm.list.lrange(0, -1) == [b'a', b'b']

    def test_clear(self):
        tm = litetestmodel.new(id='A')
        tm.list.append(b'<string>')
        tm.list.clear()
        assert not tm.list
        assert not tm.list.lrange(0, -1)

    def test_remove(self):
        tm = litetestmodel.new(id='A')
        lst = [b'a', b'b', b'a']
        tm.list.extend(lst)
        tm.list.remove(b'a')
        # Only the first occurrence was removed
        assert tm.list == lst[1:]

    def test_pop(self):
        tm = litetestmodel.new(id='A')
        lst = [b'a', b'b', b'c', b'd']
        tm.list.extend(lst)
        tm.list.pop()
        assert tm.list == lst[:-1]
        assert tm.list.lrange(0, -1) == lst[:-1]
        tm.list.pop(0)
        assert tm.list == lst[1:-1]
        assert tm.list.lrange(0, -1) == lst[1:-1]
        with pytest.raises(exceptions.UnsupportedOperation):
            tm.list.pop(1)

    def test_set(self):
        tm = litetestmodel.new(id='A')
        new = [b'1', b'2', b'3']
        assert tm.list == []
        tm.list = new
        assert tm.list == new
        assert tm.list.lrange(0, -1) == new

    def test_insert(self):
        tm = litetestmodel.new(id='A')
        lst = [b'a', b'b', b'c', b'd']
        tm.list.extend(lst)
        tm.list.insert(0, b'e')
        assert tm.list == [b'e'] + lst
        assert tm.list.lrange(0, -1) == [b'e'] + lst
        tm.list.pop(0)
        tm.list.insert(len(tm.list), b'e')
        assert tm.list == lst + [b'e']
        assert tm.list.lrange(0, -1) == lst + [b'e']

    def test_setitem(self):
        tm = litetestmodel.new(id='A')
        lst = [b'a', b'b', b'c', b'd']
        tm.list.extend(lst)
        tm.list[0] = b'e'
        assert tm.list == [b'e'] + lst[1:]
        assert tm.list.lrange(0, -1) == [b'e'] + lst[1:]
        tm.list[-1] = b'e'
        assert tm.list == [b'e'] + lst[1:-1] + [b'e']
        assert tm.list.lrange(0, -1) == [b'e'] + lst[1:-1] + [b'e']


class TestInteger:

    def test_set(self):
        tm = litetestmodel.new(id='A')
        assert tm.int == 0
        tm.int = 43
        assert tm.int == 43

    def test_incr(self):
        tm = litetestmodel.new(id='A')
        tm.int += 41
        assert tm.int == 41
        tm.int += 2
        assert tm.int == 43


class TestModel:

    def test_new_plain(self):
        # Plain fields
        ka = {'id': 'A', 'unique': '<string>',
              'required': '', 'plain_set': {1, 2, 3}}
        new = fulltestmodel.new(**ka)
        for k in ka:
            assert ka[k] == getattr(new, k)

        # Load and test again
        reloaded = fulltestmodel(id=ka['id'])

        for k in ka:
            assert ka[k] == getattr(reloaded, k)

    def test_new_uniqueness(self):
        ka = {'id': 'A', 'unique': '<string>',
              'required': ''}
        fulltestmodel.new(**ka)
        ka['id'] = 'B'
        with pytest.raises(exceptions.DuplicateEntry):
            fulltestmodel.new(**ka)

    def test_new_pk_uniqueness(self):
        new = litetestmodel.new(id='A')
        with pytest.raises(exceptions.DuplicateEntry):
            litetestmodel.new(id='A')

    def test_new_auto(self):
        val = {b'1', b'2', b'3'}
        ka = {'id': 'A', 'unique': '<string>',
              'required': '', 'auto_set': val}
        new = fulltestmodel.new(**ka)
        assert isinstance(new.auto_set, auto.autotype)
        assert isinstance(new.auto_set, proxies.commandproxy)
        assert new.auto_set == val
        assert new.auto_set.smembers() == val

    def test_new_proxy(self):
        val = {b'1', b'2', b'3'}
        ka = {'id': 'A', 'unique': '<string>',
              'required': '', 'proxy_set': val}
        new = fulltestmodel.new(**ka)
        assert isinstance(new.proxy_set, proxies.commandproxy)
        assert new.proxy_set.smembers() == val

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

    def test_foreign(self):
        fa = foreign_a.new(id='A', b_field='B')
        fb = foreign_b.new(id='B', a_field='A')
        # Load foreign_a
        la = foreign_a(id='A')
        assert isinstance(la.b_field, foreign_b)
        assert isinstance(la.b_field.a_field, foreign_a)
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

    def test_eq(self):
        litetestmodel.new(id='A')
        r1 = litetestmodel(id='A')
        r2 = litetestmodel(id='A')
        assert r1 != 'something'
        assert r1 == r2

    def test_count(self):
        assert litetestmodel.count() == 0
        new = litetestmodel.new(id='A')
        assert litetestmodel.count() == 1
        new = litetestmodel.new(id='B')
        assert litetestmodel.count() == 2

    def test_get_by_pks(self):
        instances = [litetestmodel.new(id='<primary key {}>'.format(i))
                        for i in range(10)]
        lst = list(litetestmodel.get(x.primary_key for x in instances))
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
        instances.append(litetestmodel.new(id=(0, '1')))
        instances.append(litetestmodel.new(id=(2, '2')))
        instances.append(litetestmodel.new(id=(50, '3')))
        instances.append(litetestmodel.new(id=('+inf', '4')))
        lst = list(litetestmodel.get(offset=0, limit=2))
        assert len(lst) == 2
        assert lst == instances[:2]
        lst = list(litetestmodel.get(offset=2, limit=2))
        assert len(lst) == 2
        assert lst == instances[2:4]
        lst = list(litetestmodel.get(start=2, stop=40))
        assert len(lst) == 1
        assert lst == instances[1:2]
        lst = list(litetestmodel.get(start=2, stop=50))
        assert len(lst) == 2
        assert lst == instances[1:3]
        lst = list(litetestmodel.get(offset=1, start=2, stop=50, limit=10))
        assert len(lst) == 1
        assert lst == instances[2:3]

    def test_delete_plain(self):
        new = fulltestmodel.new(id='A', unique='<string>',
                                required='')
        del new.required
        assert new.required is None
        del new.unique
        assert new.unique is None
        reloaded = fulltestmodel(id='A')
        assert reloaded.required is None
        assert reloaded.unique is None
        # Doesn't raise
        fulltestmodel.new(id='B', unique='<string>',
                          required='')

    def test_delete_standalone(self):
        new = litetestmodel.new(id='A')
        new.standalone.sadd(b'1')
        assert new.standalone.smembers() == {b'1'}
        del new.standalone
        assert new.standalone.smembers() == set()

    def test_delete(self):
        new = litetestmodel.new(id='A')
        new.standalone.sadd(b'1')
        new.delete()
        assert not new.good()
        assert new.standalone.smembers() == set()
        reloaded = litetestmodel(id='A')
        assert not new.good()

    def test_new_foreign(self):
        # Model.new(field=X) where X is an instance of some
        # foreign type must return an instance of Model with
        # 'field' set to X
        fa = foreign_a.new(id='A', b_field='B')
        fb = foreign_b.new(id='B', a_field='A')
        new_a = foreign_a.new(id='C', b_field=fb)
        assert new_a.b_field is fb
        reloaded = foreign_a(id='C')
        assert reloaded.b_field == fb


class TestEncoding:

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
