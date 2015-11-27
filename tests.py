import redis
from fused import fields, model

import pytest


TEST_PORT = 6379
TEST_DB = 14
TEST_CONNECTION = redis.Redis(port=TEST_PORT, db=TEST_DB)


@pytest.fixture(autouse=True)
def flushdb():
    TEST_CONNECTION.flushdb()


class testmodel(model.BaseModel):
    redis = TEST_CONNECTION
    standalone = fields.Set(standalone=True)
    set = fields.Set(auto=True)
    list = fields.List(auto=True)




class TestFields:

    def test_types(self):
        tm = testmodel()
        # Proxy fields
        assert type(tm.standalone) is fields.commandproxy
        assert type(tm.standalone.get) is fields.callproxy
        # Auto fields
        assert isinstance(tm.set, fields.autotype)
        assert isinstance(tm.set, fields.autotype)

    @pytest.mark.parametrize('command,args,inverse,invargs', [
        ('HSET', (b'<string>', 1), 'HKEYS', ()),
        ('SADD', (b'<string>',), 'SMEMBERS', ()),
        ('ZADD', (b'<string>', 1), 'ZRANGE', (0, -1)),
        ('LPUSH', (b'<string>',), 'LRANGE', (0, -1))
    ])
    def test_proxy(self, command, args, inverse, invargs):
        tm = testmodel()
        proxy = getattr(tm.standalone, command.lower())
        iproxy = getattr(tm.standalone, inverse.lower())
        assert all(x == y for x, y in zip(args, iproxy(*invargs)))


# TODO: Test augmented assignment operators as well?
# TODO: They're implemented using tested methods, I see no point in
# TODO: testing them specifically.


class TestSet:

    def test_add(self):
        tm = testmodel()
        assert not tm.set
        assert not tm.set.smembers()
        tm.set.add(b'<string>')
        assert tm.set
        assert len(tm.set) == 1
        assert tm.set == {b'<string>'}
        assert tm.set.smembers()
        assert tm.set.smembers() == {b'<string>'}

    def test_clear(self):
        tm = testmodel()
        tm.set.add(b'<string>')
        tm.set.clear()
        assert not tm.set
        assert not tm.set.smembers()

    def test_pop(self):
        tm = testmodel()
        tm.set.add(b'<string>')
        tm.set.pop()
        assert not tm.set
        assert not tm.set.smembers()
        with pytest.raises(KeyError):
            tm.set.pop()

    def test_remove(self):
        tm = testmodel()
        tm.set.add(b'<string>')
        tm.set.remove(b'<string>')
        assert not tm.set
        assert not tm.set.smembers()
        with pytest.raises(KeyError):
            tm.set.remove(b'<string>')

    def test_discard(self):
        tm = testmodel()
        tm.set.add(b'a')
        tm.set.discard(b'a')
        assert not tm.set
        assert not tm.set.smembers()
        # No exception if the element is not present
        tm.set.discard(b'a')

    def test_update(self):
        tm = testmodel()
        other = ({b'a', b'b', b'c', b'd'},
                 {b'e', b'f', b'g', b'h'})
        tm.set.update(*other)
        assert tm.set
        flat = {e for tup in other for e in tup}
        assert tm.set == flat
        assert tm.set.smembers() == flat

    def test_symmetric_difference_update(self):
        tm = testmodel()
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
        tm = testmodel()
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
        tm = testmodel()
        tm.set.add(b'a')
        tm.set.add(b'b')
        tm.set.add(b'c')
        other = ({b'b'}, {b'c'})
        diff = {b'a'}
        assert tm.set.difference(*other) == diff
        tm.set.difference_update(*other)
        assert tm.set == {b'a'}
        assert tm.set.smembers() == {b'a'}


class TestList:

    def test_append(self):
        tm = testmodel()
        assert not tm.list
        lst = [b'a', b'b']
        for x in lst:
            tm.list.append(x)
        assert tm.list
        assert len(tm.list) == 2
        assert tm.list == lst
        assert tm.list.lrange(0, -1) == lst

    def test_extend(self):
        tm = testmodel()
        assert not tm.list
        lst = [b'a', b'b']
        tm.list.extend(lst)
        assert tm.list
        assert len(tm.list) == 2
        assert tm.list == lst
        assert tm.list.lrange(0, -1) == [b'a', b'b']

    def test_clear(self):
        tm = testmodel()
        tm.list.append(b'<string>')
        tm.list.clear()
        assert not tm.list
        assert not tm.list.lrange(0, -1)

    def test_remove(self):
        tm = testmodel()
        lst = [b'a', b'b', b'a']
        tm.list.extend(lst)
        tm.list.remove(b'a')
        # Only the first occurrence was removed
        assert tm.list == lst[1:]



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
        with pytest.raises(Exception):
            fulltestmodel.new(**ka)

    def test_new_auto(self):
        val = {b'1', b'2', b'3'}
        ka = {'id': '<irrelevant>', 'unique': '<string>',
              'required': '', 'auto_set': val}
        new = fulltestmodel.new(**ka)
        assert isinstance(new.auto_set, fields.autotype)
        assert isinstance(new.auto_set, fields.commandproxy)
        assert new.auto_set == val
        assert new.auto_set.smembers() == val

    def test_new_proxy(self):
        val = {b'1', b'2', b'3'}
        ka = {'id': '<irrelevant>', 'unique': '<string>',
              'required': '', 'proxy_set': val}
        new = fulltestmodel.new(**ka)
        assert isinstance(new.proxy_set, fields.commandproxy)
        assert new.proxy_set.smembers() == val


class fulltestmodel(model.BaseModel):
    redis = TEST_CONNECTION
    id = fields.PrimaryKey()
    unique = fields.String(unique=True)
    required = fields.String(required=True)
    proxy_set = fields.Set(standalone=True)
    auto_set = fields.Set(auto=True)
    plain_set = fields.Set()