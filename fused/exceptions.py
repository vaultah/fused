from redis.exceptions import RedisError


class InvalidCommand(RedisError):
	pass