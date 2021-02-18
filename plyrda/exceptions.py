class PlyrdaException(Exception):
    ...

class ColumnNotExistingError(PlyrdaException):
    """When selecting non-existing columns"""

class ColumnNameInvalidError(PlyrdaException):
    """When invalid column names provided to select"""
