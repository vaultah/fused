import redis
from fused import fields, model

import pytest


TEST_PORT = 6379
TEST_DB = 14
TEST_CONNECTION = redis.Redis(port=TEST_PORT, db=TEST_DB)

@pytest.fixture(autouse=True)
def flushdb():
    TEST_CONNECTION.flushdb()


class pureproxymodel(model.BaseModel):
    redis = TEST_CONNECTION
    field = fields.Field()


class TestProxies:

    def test_types(self):
        ppm = pureproxymodel()
        assert type(ppm.field) is fields.commandproxy
        assert type(ppm.field.get) is fields.callproxy

    @pytest.mark.parametrize('command,args,inverse,invargs', [
        ('HSET', (b'<string>', 1), 'HKEYS', ()),
        ('SADD', (b'<string>',), 'SMEMBERS', ()),
        ('ZADD', (b'<string>', 1), 'ZRANGE', (0, -1)),
        ('LPUSH', (b'<string>',), 'LRANGE', (0, -1))
    ])
    def test_commands(self, command, args, inverse, invargs):
        ppm = pureproxymodel()
        proxy = getattr(ppm.field, command.lower())
        iproxy = getattr(ppm.field, inverse.lower())
        assert all(x == y for x, y in zip(args, iproxy(*invargs)))