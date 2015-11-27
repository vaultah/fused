class FusedError(Exception):
    pass


class MissingFields(FusedError):
    pass

    
class NoPrimaryKey(FusedError):
    def __init__(self):
        super().__init__('')


class DuplicateEntry(FusedError):
    pass