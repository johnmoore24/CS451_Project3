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
        self.lock_manager = lock_manager if lock_manager else LockManager()
        self.queries = []
        self._started = False
        self._committed = False
        self._aborted = False
        self.results = []

    def begin(self):
        if self._started:
            print(f"DEBUG: Transaction {self.transaction_id} failed to begin - already started")
            return False
        #print(f"DEBUG: Transaction {self.transaction_id} beginning with {len(self.queries)} queries")
        self._started = True
        return True

    def commit(self):
        if not self._started or self._committed or self._aborted:
            return False
        try:
            # Release all locks
            if self.lock_manager:
                self.lock_manager.release_all_locks(self.transaction_id)
            self._committed = True
            return True
        except Exception as e:
            self.abort()
            return False

    def abort(self):
        if not self._started or self._committed:
            return False
        try:
            # Release all locks
            if self.lock_manager:
                self.lock_manager.release_all_locks(self.transaction_id)
            self._aborted = True
            return True
        except Exception as e:
            return False

    def add_query(self, query_func, table, *args):
        """Add a query to this transaction"""
        self.queries.append((query_func, table, *args))
        return True

    def execute(self):
        """Execute all queries in the transaction"""
        if not self.begin():
            print(f"DEBUG: Transaction {self.transaction_id} failed at begin() stage")
            return False

        try:
            #print(f"DEBUG: Transaction {self.transaction_id} starting execution of {len(self.queries)} queries")
            for i, (query_func, table, *args) in enumerate(self.queries):
                #print(f"DEBUG: Transaction {self.transaction_id} - Query {i+1}/{len(self.queries)}")
                #print(f"DEBUG: Function: {query_func.__name__}, Args: {args}")
                
                # Get appropriate lock type
                lock_type = LockType.SHARED if query_func.__name__ == 'select' else LockType.EXCLUSIVE
                
                try:
                    # Acquire lock with timeout
                    if not self.lock_manager.acquire_lock(self.transaction_id, args[0], lock_type):
                        print(f"DEBUG: Transaction {self.transaction_id} failed to acquire {lock_type} lock for {args[0]}")
                        raise TransactionAbortError(f"Lock acquisition failed for {args[0]}")
                        
                    result = query_func(*args)
                    #print(f"DEBUG: Query {i+1} successful in Transaction {self.transaction_id}")
                    self.results.append(result)
                    
                    # Release lock if it's the last operation on this key
                    next_queries = self.queries[i+1:]
                    if not any(args[0] == next_args[0] for _, _, *next_args in next_queries):
                        #print(f"DEBUG: Releasing lock for {args[0]} - no more operations on this key")
                        self.lock_manager.release_lock(self.transaction_id, args[0])
                    
                except Exception as e:
                    print(f"DEBUG: Query {i+1} failed in Transaction {self.transaction_id}: {str(e)}")
                    self.lock_manager.release_lock(self.transaction_id, args[0])
                    raise

            print(f"DEBUG: Transaction {self.transaction_id} committing.")
            return self.commit()

        except Exception as e:
            print(f"DEBUG: Transaction {self.transaction_id} failed during execution: {str(e)}")
            self.abort()
            raise TransactionAbortError(f"Transaction failed: {str(e)}")

    def get_queries(self):
        """Get all queries in this transaction"""
        return self.queries

    def has_query(self, query_type):
        """Check if transaction has a specific type of query"""
        return any(q[0].__name__ == query_type for q in self.queries)

    @property
    def is_running(self):
        return self._started and not (self._committed or self._aborted)