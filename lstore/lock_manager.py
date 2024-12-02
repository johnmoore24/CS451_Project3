import threading
from enum import Enum
from lstore.transaction_exceptions import LockConflictError, TransactionAbortError

class LockType(Enum):
    SHARED = 0      # Read lock
    EXCLUSIVE = 1   # Write lock

class Lock:
    def __init__(self, transaction_id, lock_type):
        self.transaction_id = transaction_id
        self.lock_type = lock_type
        self.timestamp = threading.get_ident()

class LockManager:
    def __init__(self):
        self._lock = threading.Lock()
        self.record_locks = {}  # {rid: [Lock objects]}
        self.transaction_locks = {}  # {transaction_id: set(rid)}
        self.debug = False

    def _debug_log(self, message):
        if self.debug:
            print(f"LOCK_MANAGER: {message}")
    
    def _log_lock_state(self, message):
        """Helper to log current lock state"""
        with open('pt3_testoutput.txt', 'a') as f:
            f.write(f"\n=== LOCK STATE ===\n")
            f.write(f"Context: {message}\n")
            f.write(f"Record locks:\n")
            for rid, locks in self.record_locks.items():
                f.write(f"  RID {rid}: {[(l.transaction_id, l.lock_type) for l in locks]}\n")
            f.write(f"Transaction locks:\n")
            for txn_id, rids in self.transaction_locks.items():
                f.write(f"  Transaction {txn_id}: {rids}\n")
            f.write("==================\n")
            f.flush()

    def acquire_lock(self, transaction_id, rid, lock_type):
        """
        Attempts to acquire a lock for a transaction.
        No-wait implementation - if lock cannot be acquired, raises LockConflictError.
        """
        with self._lock:
            # Log attempt
            self._debug_log(f"Transaction {transaction_id} attempting to acquire {lock_type} lock on rid {rid}")
            with open('pt3_testoutput.txt', 'a') as f:
                f.write(f"[LOCK] Pre-acquire state for txn {transaction_id}:\n")
                f.write(f"  - Current locks on rid {rid}: {self.record_locks.get(rid, [])}\n")
                f.write(f"  - Transaction locks: {self.transaction_locks.get(transaction_id, set())}\n")

            # Initialize lock tracking if needed
            if rid not in self.record_locks:
                self.record_locks[rid] = []
            if transaction_id not in self.transaction_locks:
                self.transaction_locks[transaction_id] = set()

            # Check existing locks on this record
            for lock in self.record_locks[rid]:
                # Another transaction holds a lock
                if lock.transaction_id != transaction_id:
                    # Conflict if either lock is exclusive
                    if lock.lock_type == LockType.EXCLUSIVE or lock_type == LockType.EXCLUSIVE:
                        error_msg = f"Cannot acquire {lock_type} lock on rid {rid} - conflicting lock held by transaction {lock.transaction_id}"
                        self._debug_log(f"Lock conflict: {error_msg}")
                        with open('pt3_testoutput.txt', 'a') as f:
                            f.write(f"[LOCK] Conflict detected: {error_msg}\n")
                        raise LockConflictError(error_msg)

                # Same transaction already holds a lock
                else:
                    if lock.lock_type == lock_type:
                        # Already have this type of lock
                        return True
                    elif lock.lock_type == LockType.SHARED and lock_type == LockType.EXCLUSIVE:
                        # Upgrade from shared to exclusive
                        lock.lock_type = LockType.EXCLUSIVE
                        return True

            # No conflicts - acquire the lock
            new_lock = Lock(transaction_id, lock_type)
            self.record_locks[rid].append(new_lock)
            self.transaction_locks[transaction_id].add(rid)
            
            self._debug_log(f"Lock acquired: Transaction {transaction_id} got {lock_type} lock on rid {rid}")
            with open('pt3_testoutput.txt', 'a') as f:
                f.write(f"[LOCK] Lock acquired: Transaction {transaction_id} got {lock_type} lock on rid {rid}\n")
            
            return True

    def release_lock(self, transaction_id, rid):
        """Releases a specific lock held by a transaction"""
        with self._lock:
            self._debug_log(f"Releasing lock on rid {rid} for transaction {transaction_id}")
            self._log_lock_state("Before release_lock")
            
            if rid in self.record_locks:
                self.record_locks[rid] = [lock for lock in self.record_locks[rid] 
                                        if lock.transaction_id != transaction_id]
                if not self.record_locks[rid]:
                    del self.record_locks[rid]

            if transaction_id in self.transaction_locks:
                self.transaction_locks[transaction_id].discard(rid)
                if not self.transaction_locks[transaction_id]:
                    del self.transaction_locks[transaction_id]
                
            self._log_lock_state("After release_lock")

    def release_all_locks(self, transaction_id):
        """Releases all locks held by a transaction"""
        with self._lock:
            self._debug_log(f"Releasing all locks for transaction {transaction_id}")
            if transaction_id in self.transaction_locks:
                rids_to_release = list(self.transaction_locks[transaction_id])
                for rid in rids_to_release:
                    self.release_lock(transaction_id, rid)

    def has_lock(self, transaction_id, rid, lock_type=None):
        """Checks if a transaction has a specific type of lock on a record"""
        with self._lock:
            if rid in self.record_locks:
                for lock in self.record_locks[rid]:
                    if lock.transaction_id == transaction_id:
                        if lock_type is None or lock.lock_type == lock_type:
                            return True
            return False

    def get_transaction_locks(self, transaction_id):
        """Returns all rids locked by a transaction"""
        return self.transaction_locks.get(transaction_id, set())

    def clear_all(self):
        """Clears all locks (used for testing and recovery)"""
        with self._lock:
            self.record_locks.clear()
            self.transaction_locks.clear()