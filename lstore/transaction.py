from lstore.table import Table, Record
from lstore.index import Index
from lstore.lock_manager import LockType, LockManager
from lstore.transaction_exceptions import *
from enum import Enum
from time import time
from datetime import datetime

class TransactionState(Enum):
    ACTIVE = 0
    COMMITTED = 1
    ABORTED = 2

class Transaction:
    _transaction_counter = 0  # Class variable to generate unique IDs

    def __init__(self, transaction_id=None, lock_manager=None):
        # Generate unique transaction ID if none provided
        if transaction_id is None:
            Transaction._transaction_counter += 1
            transaction_id = Transaction._transaction_counter
            
        self.transaction_id = transaction_id
        self.lock_manager = lock_manager
        self.queries = []
        self._started = False
        self._committed = False
        self._aborted = False

    def begin(self):
        if self._started:
            return False
        self._started = True
        return True

    def commit(self):
        if not self._started or self._committed or self._aborted:
            return False
        self._committed = True
        return True

    def abort(self):
        if not self._started or self._committed:
            return False
        self._aborted = True
        return True

    def add_query(self, query_func, table, *args):
        """Add a query to this transaction"""
        self.queries.append((query_func, table, *args))
        return True

    def get_queries(self):
        """Get all queries in this transaction"""
        return self.queries

    def has_query(self, query_type):
        """Check if transaction has a specific type of query"""
        return any(q[0].__name__ == query_type for q in self.queries)

    @property
    def is_running(self):
        return self._started and not (self._committed or self._aborted)