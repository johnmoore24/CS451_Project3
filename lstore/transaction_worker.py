from lstore.lock_manager import LockManager
import uuid
import threading
import time
from collections import defaultdict
from lstore.transaction_exceptions import *

class TransactionWorker(threading.Thread):
    # Class-level lock for synchronizing table access
    _global_table_lock = threading.Lock()

    def __init__(self, lock_manager=None):
        super().__init__()  # Initialize the Thread superclass
        self.lock_manager = lock_manager if lock_manager else LockManager()
        self.id = str(uuid.uuid4())[:8]
        self.transactions = []
        self.MAX_RETRIES = 3
        self.RETRY_DELAY = 0.1
        self.transaction_states = {}
        self._lock = threading.Lock()
        self._started = threading.Event()

    def _execute_transaction(self, transaction, attempt):
        """Execute a single transaction"""
        try:
            # Acquire global lock before starting transaction
            with self._global_table_lock:
                if not transaction.begin():
                    self._log(f"Failed to begin transaction {transaction.transaction_id}")
                    return False, None
                
                self.transaction_states[transaction.transaction_id] = 'RUNNING'
                
                # Execute each query within the global lock
                for query_func, table, *args in transaction.queries:
                    try:
                        # Execute query (no need for individual locks since we have global lock)
                        if not query_func(*args):
                            raise QueryExecutionError("Query execution failed")
                            
                    except Exception as e:
                        self._log(f"Query execution failed in transaction {transaction.transaction_id}: {str(e)}")
                        transaction.abort()
                        return False, f"Query execution failed in transaction {transaction.transaction_id}: {str(e)}"
                        
                # Commit transaction
                if transaction.commit():
                    self.transaction_states[transaction.transaction_id] = 'COMPLETED'
                    return True, None
                    
                return False, None
                
        except Exception as e:
            self._log(f"Transaction {transaction.transaction_id} failed: {str(e)}")
            transaction.abort()
            return False, f"Transaction {transaction.transaction_id} failed: {str(e)}"

    def add_transaction(self, transaction):
        """Add a transaction to this worker's queue"""
        if transaction.lock_manager is None:
            transaction.lock_manager = self.lock_manager
        self.transactions.append(transaction)
        self.transaction_states[transaction.transaction_id] = 'PENDING'

    def run(self):
        """Thread's run method - executes all transactions"""
        # If the thread hasn't been started, start it properly
        if not self._started.is_set():
            self.start()
            self.join()
            return

        success_count = 0
        total_count = len(self.transactions)
        
        for transaction in self.transactions:
            attempt = 1
            while attempt <= self.MAX_RETRIES:
                success, error = self._execute_transaction(transaction, attempt)
                if success:
                    success_count += 1
                    break
                attempt += 1
                if attempt <= self.MAX_RETRIES:
                    time.sleep(self.RETRY_DELAY * attempt)
                    
        self._log(f"Worker finished. Success rate: {success_count}/{total_count}")
        return success_count

    def start_and_join(self):
        """Helper method to start the thread and wait for it to finish"""
        self.start()
        self.join()
        return True

    def _log(self, message):
        """Log a message with the worker's ID"""
        print(f"Worker {self.id} [INFO]: {message}")