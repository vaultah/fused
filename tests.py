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


class TestFields:

    def test_types(self):
        tm = litetestmodel.new(id='<string>')
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
        tm = litetestmodel.new(id='<string>')
        proxy = getattr(tm.standalone, command.lower())
        iproxy = getattr(tm.standalone, inverse.lower())
        assert all(x == y for x, y in zip(args, iproxy(*invargs)))


# TODO: Test augmented assignment operators as well?
# TODO: They're implemented using tested methods, I see no point in
# TODO: testing them specifically.


class TestSet:

    def test_add(self):
        tm = litetestmodel.new(id='<string>')
        assert not tm.set
        assert not tm.set.smembers()
        tm.set.add(b'<string>')
        assert tm.set
        assert len(tm.set) == 1
        assert tm.set == {b'<string>'}
        assert tm.set.smembers()
        assert tm.set.smembers() == {b'<string>'}

    def test_clear(self):
        tm = litetestmodel.new(id='<string>')
        tm.set.add(b'<string>')
        tm.set.clear()
        assert not tm.set
        assert not tm.set.smembers()

    def test_pop(self):
        tm = litetestmodel.new(id='<string>')
        tm.set.add(b'<string>')
        tm.set.pop()
        assert not tm.set
        assert not tm.set.smembers()
        with pytest.raises(KeyError):
            tm.set.pop()

    def test_remove(self):
        tm = litetestmodel.new(id='<string>')
        tm.set.add(b'<string>')
        tm.set.remove(b'<string>')
        assert not tm.set
        assert not tm.set.smembers()
        with pytest.raises(KeyError):
            tm.set.remove(b'<string>')

    def test_discard(self):
        tm = litetestmodel.new(id='<string>')
        tm.set.add(b'a')
        tm.set.discard(b'a')
        assert not tm.set
        assert not tm.set.smembers()
        # No exception if the element is not present
        tm.set.discard(b'a')

    def test_update(self):
        tm = litetestmodel.new(id='<string>')
        other = ({b'a', b'b', b'c', b'd'},
                 {b'e', b'f', b'g', b'h'})
        tm.set.update(*other)
        assert tm.set
        flat = {e for tup in other for e in tup}
        assert tm.set == flat
        assert tm.set.smembers() == flat

    def test_symmetric_difference_update(self):
        tm = litetestmodel.new(id='<string>')
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
        tm = litetestmodel.new(id='<string>')
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
        tm = litetestmodel.new(id='<string>')
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
        tm = litetestmodel.new(id='<string>')
        new = {b'1', b'2', b'3'}
        tm.set = new
        assert tm.set == new
        assert tm.set.smembers() == new

class TestList:

    def test_append(self):
        tm = litetestmodel.new(id='<string>')
        assert not tm.list
        lst = [b'a', b'b']
        for x in lst:
            tm.list.append(x)
        assert tm.list
        assert len(tm.list) == 2
        assert tm.list == lst
        assert tm.list.lrange(0, -1) == lst

    def test_extend(self):
        tm = litetestmodel.new(id='<string>')
        assert not tm.list
        lst = [b'a', b'b']
        tm.list.extend(lst)
        assert tm.list
        assert len(tm.list) == 2
        assert tm.list == lst
        assert tm.list.lrange(0, -1) == [b'a', b'b']

    def test_clear(self):
        tm = litetestmodel.new(id='<string>')
        tm.list.append(b'<string>')
        tm.list.clear()
        assert not tm.list
        assert not tm.list.lrange(0, -1)

    def test_remove(self):
        tm = litetestmodel.new(id='<string>')
        lst = [b'a', b'b', b'a']
        tm.list.extend(lst)
        tm.list.remove(b'a')
        # Only the first occurrence was removed
        assert tm.list == lst[1:]

    def test_pop(self):
        tm = litetestmodel.new(id='<string>')
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
        tm = litetestmodel.new(id='<string>')
        new = [b'1', b'2', b'3']
        tm.list = new
        assert tm.list == new
        assert tm.list.lrange(0, -1) == new


class TestModel:

    def test_new_plain(self):
        # Plain fields
        ka = {'id': '<irrelevant 1>', 'unique': '<string>',
              'required': '', 'plain_set': {1, 2, 3}}
        new = fulltestmodel.new(**ka)
        for k in ka:
            assert ka[k] == getattr(new, k)

        # Load and test again
        reloaded = fulltestmodel(id=ka['id'])

        for k in ka:
            assert ka[k] == getattr(reloaded, k)

    def test_new_uniqueness(self):
        ka = {'id': '<irrelevant 1>', 'unique': '<string>',
              'required': ''}
        fulltestmodel.new(**ka)
        ka['id'] = '<irrelevant 2>'
        with pytest.raises(exceptions.DuplicateEntry):
            fulltestmodel.new(**ka)

    def test_new_auto(self):
        val = {b'1', b'2', b'3'}
        ka = {'id': '<irrelevant>', 'unique': '<string>',
              'required': '', 'auto_set': val}
        new = fulltestmodel.new(**ka)
        assert isinstance(new.auto_set, auto.autotype)
        assert isinstance(new.auto_set, proxies.commandproxy)
        assert new.auto_set == val
        assert new.auto_set.smembers() == val

    def test_new_proxy(self):
        val = {b'1', b'2', b'3'}
        ka = {'id': '<irrelevant>', 'unique': '<string>',
              'required': '', 'proxy_set': val}
        new = fulltestmodel.new(**ka)
        assert isinstance(new.proxy_set, proxies.commandproxy)
        assert new.proxy_set.smembers() == val

    def test_load(self):
        ka = {'id': '<irrelevant>', 'unique': '<string>', 'required': ''}
        new = fulltestmodel.new(**ka)
        # By the primary key
        reload = fulltestmodel(id=ka['id'])
        for k in ka:
            assert ka[k] == getattr(reload, k)

        # By unique field
        reload = fulltestmodel(unique=ka['unique'])
        for k in ka:
            assert ka[k] == getattr(reload, k)

    def test_instant_update_plain(self):
        ka = {'id': '<irrelevant>', 'unique': '<string>', 'required': ''}
        new = fulltestmodel.new(**ka)
        new.required = 'new value'
        assert new.required == 'new value'
        reload = fulltestmodel(id=ka['id'])
        assert reload.required == 'new value'

    def test_instant_update_unique(self):
        ka1 = {'id': '<irrelevant 1>', 'unique': '<string 1>', 'required': ''}
        ka2 = {'id': '<irrelevant 2>', 'unique': '<string 2>', 'required': ''}
        new = fulltestmodel.new(**ka1)
        other = fulltestmodel.new(**ka2)
        # Can we update it?
        new.unique = '<string>'
        assert new.unique == '<string>'
        # Does reloading change anything?
        reload = fulltestmodel(id=ka1['id'])
        assert reload.unique == '<string>'

        with pytest.raises(exceptions.DuplicateEntry):
            new.unique = other.unique



class TestEncoding:

    def test_decode_responses(self):
        ka = {'id': '<irrelevant>', 'str': '¿Cómo está usted?',
              'bytes': b'\x00 and some more chars',
              'plain_set': {1, 2, 3, 'a', b'b', ('c',)}}
        new = decodetestmodel.new(**ka)
        for k in ka:
            assert ka[k] == getattr(new, k)

        reload = decodetestmodel(id=ka['id'])
        # If decode_responses is True, it will raise
        # AssertionError for 'bytes', this is by design
        for k in ka.keys() - {'bytes'}:
            assert ka[k] == getattr(reload, k)

        assert ka['bytes'] != reload.bytes

    def test_no_decode_responses(self):
        ka = {'id': '<irrelevant>', 'str': '¿Cómo está usted?',
              'bytes': b'\x00 and some more chars',
              'plain_set': {1, 2, 3, 'a', b'b', ('c',)}}
        new = nodecodetestmodel.new(**ka)
        for k in ka:
            assert ka[k] == getattr(new, k)

        reload = nodecodetestmodel(id=ka['id'])
        # decode_responses is False, 'bytes' won't be decoded
        for k in ka.keys():
            assert ka[k] == getattr(reload, k)