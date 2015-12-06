-- redis-py < 3.0 doesn't support ZADD ... NX
return redis.call('ZADD', KEYS[1], 'NX', ARGV[2], ARGV[1]);