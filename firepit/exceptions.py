class InvalidAttr(Exception):
    def __init__(self, msg):
        self.message = msg

    def __str__(self):
        return f"{self.message}"


class InvalidObject(Exception):
    def __init__(self, msg):
        self.message = msg

    def __str__(self):
        return f"{self.message}"


class StixPatternError(Exception):
    def __init__(self, stix):
        self.stix = stix

    def __str__(self):
        return f"{self.stix}"


class InvalidViewname(Exception):
    pass


class InvalidStixPath(Exception):
    pass


class IncompatibleType(Exception):
    pass


class UnknownViewname(Exception):
    pass


class DuplicateTable(Exception):
    pass
