class FusedError(Exception):
    pass


class MissingFields(FusedError):
    pass

    
class NoPrimaryKey(FusedError):
	pass


class DuplicateEntry(FusedError):
    pass


class UnsupportedOperation(FusedError):
	pass
