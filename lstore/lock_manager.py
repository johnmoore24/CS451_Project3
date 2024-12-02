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
            #print(f"LOCK_MANAGER: {message}")
            pass

    def acquire_lock(self, transaction_id, rid, lock_type):
        """
        Attempts to acquire a lock for a transaction.
        No-wait implementation - if lock cannot be acquired, raises LockConflictError.
        """
        with self._lock:
            self._debug_log(f"Transaction {transaction_id} attempting to acquire {lock_type} lock on rid {rid}")

            if rid not in self.record_locks:
                self.record_locks[rid] = []
            if transaction_id not in self.transaction_locks:
                self.transaction_locks[transaction_id] = set()

            for lock in self.record_locks[rid]:
                if lock.transaction_id != transaction_id:
                    if lock.lock_type == LockType.EXCLUSIVE or lock_type == LockType.EXCLUSIVE:
                        error_msg = f"Cannot acquire {lock_type} lock on rid {rid} - conflicting lock held by transaction {lock.transaction_id}"
                        self._debug_log(f"Lock conflict: {error_msg}")
                        raise LockConflictError(error_msg)

            new_lock = Lock(transaction_id, lock_type)
            self.record_locks[rid].append(new_lock)
            self.transaction_locks[transaction_id].add(rid)

            self._debug_log(f"Lock acquired: Transaction {transaction_id} got {lock_type} lock on rid {rid}")
            return True

    def release_lock(self, transaction_id, rid):
        """Releases a specific lock held by a transaction"""
        with self._lock:
            self._debug_log(f"Releasing lock on rid {rid} for transaction {transaction_id}")
            
            if rid in self.record_locks:
                self.record_locks[rid] = [lock for lock in self.record_locks[rid] 
                                        if lock.transaction_id != transaction_id]
                if not self.record_locks[rid]:
                    del self.record_locks[rid]

            if transaction_id in self.transaction_locks:
                self.transaction_locks[transaction_id].discard(rid)
                if not self.transaction_locks[transaction_id]:
                    del self.transaction_locks[transaction_id]

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