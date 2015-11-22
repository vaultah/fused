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

class TestFields:

    def test_types(self):
        tm = testmodel()
        # Proxy fields
        assert type(tm.proxy) is fields.commandproxy
        assert type(tm.proxy.get) is fields.callproxy
        # Auto fields
        # assert isinstance(tm.auto, fields.autotype)

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