local ID, VALUES = ARGV[1], cjson.decode(ARGV[2]);

for i=1, #KEYS do
    local res = redis.call('HEXISTS', KEYS[i], VALUES[i]);
    if res ~= 0 then
        return i
    end
end

for i=1, #KEYS do
    redis.call('HSET', KEYS[i], VALUES[i], ID);
end

return 0