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
    proxy = fields.Field()
    set = fields.Set(auto=True)

class TestFields:

    def test_types(self):
        tm = testmodel()
        # Proxy fields
        assert type(tm.proxy) is fields.commandproxy
        assert type(tm.proxy.get) is fields.callproxy
        # Auto fields
        assert isinstance(tm.set, fields.autotype)

    @pytest.mark.parametrize('command,args,inverse,invargs', [
        ('HSET', (b'<string>', 1), 'HKEYS', ()),
        ('SADD', (b'<string>',), 'SMEMBERS', ()),
        ('ZADD', (b'<string>', 1), 'ZRANGE', (0, -1)),
        ('LPUSH', (b'<string>',), 'LRANGE', (0, -1))
    ])
    def test_proxy(self, command, args, inverse, invargs):
        tm = testmodel()
        proxy = getattr(tm.proxy, command.lower())
        iproxy = getattr(tm.proxy, inverse.lower())
        assert all(x == y for x, y in zip(args, iproxy(*invargs)))


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

    def test_update(self):
        tm = testmodel()
        elems = [{b'a', b'b', b'c', b'd'},
                 {b'e', b'f', b'g', b'h'}]
        tm.set.update(*elems)
        assert tm.set
        flat = {e for tup in elems for e in tup}
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
        other = {b'b', b'c', b'd'}, {b'c', b'd', b'e'}
        intersection = {b'c'}
        assert tm.set.intersection(*other) == intersection
        tm.set.intersection_update(*other)
        assert tm.set == intersection
        assert tm.set.smembers() == intersection

    def test_discard(self):
        tm = testmodel()
        tm.set.add(b'a')
        tm.set.discard(b'a')
        assert not tm.set
        assert not tm.set.smembers()
        tm.set.discard(b'a')