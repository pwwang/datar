class DatarException(Exception):
    ...

class ColumnNotExistingError(DatarException):
    """When selecting non-existing columns"""

class ColumnNameInvalidError(DatarException):
    """When invalid column names provided to select"""

class DataUnalignableError(DatarException):
    """When two data cannot be aligned to each other"""
