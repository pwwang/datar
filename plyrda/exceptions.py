class PlyrdaException(Exception):
    ...

class ColumnNotExistingError(PlyrdaException):
    """When selecting non-existing columns"""

class ColumnNameInvalidError(PlyrdaException):
    """When invalid column names provided to select"""

class DataUnalignableError(PlyrdaException):
    """When two data cannot be aligned to each other"""
