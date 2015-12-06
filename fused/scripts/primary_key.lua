-- Write the primary key
local ID, SCORE = ARGV[1], ARGV[2];
local SSET = KEYS[1];

return redis.call('ZADD', SSET, 'NX', SCORE, ID);