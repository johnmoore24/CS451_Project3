class TransactionException(Exception):
    """Base class for all transaction-related exceptions"""
    pass

class LockConflictError(TransactionException):
    """Raised when a transaction cannot acquire a lock due to conflict"""
    def __init__(self, message="Lock conflict occurred"):
        self.message = message
        super().__init__(self.message)

class TransactionAbortError(TransactionException):
    """Raised when a transaction must be aborted"""
    def __init__(self, message="Transaction aborted"):
        self.message = message
        super().__init__(self.message)

class DeadlockError(TransactionException):
    """Raised when a potential deadlock is detected"""
    def __init__(self, message="Deadlock condition detected"):
        self.message = message
        super().__init__(self.message)
class LockAcquisitionError(TransactionException):
    """Raised when a transaction fails to acquire a lock"""
    def __init__(self, message="Failed to acquire lock"):
        self.message = message
        super().__init__(self.message)

class QueryExecutionError(TransactionException):
    """Raised when a query execution fails during a transaction"""
    def __init__(self, message="Query execution failed"):
        self.message = message
        super().__init__(self.message)

class RecoveryError(TransactionException):
    """Raised when there's an error during transaction recovery"""
    def __init__(self, message="Error during transaction recovery"):
        self.message = message
        super().__init__(self.message)

class InvalidTransactionStateError(TransactionException):
    """Raised when an operation is attempted on a transaction in an invalid state"""
    def __init__(self, message="Invalid transaction state"):
        self.message = message
        super().__init__(self.message)

class TransactionTimeoutError(TransactionException):
    """Raised when a transaction exceeds its time limit"""
    def __init__(self, message="Transaction timed out"):
        self.message = message
        super().__init__(self.message)