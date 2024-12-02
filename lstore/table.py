from lstore.index import Index
from lstore.lock_manager import LockType
from lstore.transaction_exceptions import *
from time import time
import threading
import json

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Record:
    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.indirection = None
        self.timestamp = None
        self.schema_encoding = None
        
    def __str__(self):
        return f"[{', '.join(map(str, self.columns))}]"

class Table:
    _lock = threading.Lock()

    def __init__(self, name, num_columns, key, bufferpool, lock_manager, initialize_index=True):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.bufferpool = bufferpool
        self.lock_manager = lock_manager
        self.total_columns = num_columns + 4
        self._next_rid_lock = threading.Lock()
        self._next_rid = 0
        
        # Only initialize index if specified (not when loading from disk)
        if initialize_index:
            self.index = Index(self)
        else:
            self.index = None  # Will be set during restoration
        
        self.num_records = 0
        self.num_updates = 0
        
        # Initialize page IDs lists
        self.base_page_ids = []
        self.tail_page_ids = []
        
        # Initialize page IDs for each column
        for col in range(self.total_columns):
            self.base_page_ids.append([f"{self.name}_base_{col}_0"])
            self.tail_page_ids.append([f"{self.name}_tail_{col}_0"])
            
        # Merge threshold
        self.MERGE_THRESHOLD = 10  # Number of updates before triggering merge
        self.last_merge_time = time()
        self.merge_interval = 60  # Seconds between merges
        
        self.debug = False
        
    def _debug_log(self, message, level="INFO"):
        if self.debug:
            #print(f"TABLE [{level}]: {message}")
            pass

    def _get_next_rid(self):
        with self._next_rid_lock:
            rid = self._next_rid
            self._next_rid += 1
            #print(f"DEBUG: Generated new RID {rid} for table {self.name}")
            return rid
        
    @classmethod
    def from_metadata(cls, name, num_columns, key, bufferpool, metadata, lock_manager):
        """Create a table instance from metadata"""
        table = cls(name, num_columns, key, bufferpool, lock_manager, initialize_index=False)
        table.page_directory = {int(k): tuple(v) for k, v in metadata['page_directory'].items()}
        table.num_records = metadata['num_records']
        table.num_updates = metadata.get('num_updates', 0)
        table.base_page_ids = metadata['base_page_ids']
        table.tail_page_ids = metadata['tail_page_ids']
        
        table.index = Index(table)
        if 'index_data' in metadata:
            for col_str, index_dict in metadata['index_data'].items():
                col = int(col_str)
                if col < len(table.index.indices):
                    table.index.indices[col] = {
                        int(k): int(v) for k, v in index_dict.items()
                    }
        
        return table
    
    def create_record(self, *columns):
        try:
            # Generate RID
            rid = self._get_next_rid()
            timestamp = int(time() * 1000000)
            
            # Define page range size if not already defined
            if not hasattr(self, 'page_range_size'):
                self.page_range_size = 512  # Match Page class capacity
            
            # Find first page with capacity
            current_base_page_idx = 0
            while current_base_page_idx < len(self.base_page_ids[0]):
                if all(self.bufferpool.get_page(self.name, self.base_page_ids[col][current_base_page_idx]).has_capacity() 
                    for col in range(self.total_columns)):
                    break
                current_base_page_idx += 1
            
            # Create new page if needed
            if current_base_page_idx >= len(self.base_page_ids[0]):
                for col in range(self.total_columns):
                    new_page_id = f"{self.name}_base_{col}_{len(self.base_page_ids[col])}"
                    self.base_page_ids[col].append(new_page_id)
            
            # Get actual slot within the page
            slot = self.bufferpool.get_num_records(self.name, self.base_page_ids[0][current_base_page_idx])
            
            # Update bufferpool's page directory with the page location
            page_id = self.base_page_ids[0][current_base_page_idx]
            self.bufferpool.page_directory[rid] = page_id
            
            # Write metadata columns
            self.bufferpool.write_to_page(self.name, self.base_page_ids[INDIRECTION_COLUMN][current_base_page_idx], rid)
            self.bufferpool.write_to_page(self.name, self.base_page_ids[RID_COLUMN][current_base_page_idx], rid)
            self.bufferpool.write_to_page(self.name, self.base_page_ids[TIMESTAMP_COLUMN][current_base_page_idx], timestamp)
            self.bufferpool.write_to_page(self.name, self.base_page_ids[SCHEMA_ENCODING_COLUMN][current_base_page_idx], 0)
            
            # Write data columns
            for i, value in enumerate(columns):
                success = self.bufferpool.write_to_page(
                    self.name, 
                    self.base_page_ids[i + 4][current_base_page_idx], 
                    value
                )
                if not success:
                    return None
            
            # Update page directory with correct slot
            self.page_directory[rid] = ('base', current_base_page_idx, slot)
            
            # Create and return record object
            record = Record(rid, columns[self.key], list(columns))
            record.indirection = rid
            record.timestamp = timestamp
            record.schema_encoding = 0
            
            # Update index AFTER successful write
            if self.index:
                for i, value in enumerate(columns):
                    if self.index.indices[i] is not None:
                        self.index.update_index(i, value, rid)
                        
            self.num_records += 1
            return record

        except Exception as e:
            self._debug_log(f"Error in create_record: {str(e)}", "ERROR")
            return None

    def _get_page(self, is_base, column, page_index):
        """Helper method to get a page from the buffer pool"""
        page_ids = self.base_page_ids if is_base else self.tail_page_ids
        if column < 0 or column >= len(page_ids) or page_index < 0 or page_index >= len(page_ids[column]):
            return None
        return self.bufferpool.get_page(self.name, page_ids[column][page_index])
    
    def get_record(self, rid, transaction_id=None):
        """Get record with proper locking"""
        if rid not in self.page_directory:
            return None
            
        # Acquire shared lock if transaction_id provided
        if transaction_id is not None:
            try:
                if not self.lock_manager.acquire_lock(transaction_id, rid, LockType.SHARED):
                    raise LockConflictError(f"Could not acquire shared lock on rid {rid}")
            except LockConflictError as e:
                self._debug_log(f"Lock acquisition failed: {str(e)}", "ERROR")
                return None
            
        page_type, page_index, slot = self.page_directory[rid]
        if page_type == 'deleted':
            return None
            
        try:
            is_base = page_type == 'base'
            indirection = self._get_page(is_base, INDIRECTION_COLUMN, page_index).read(slot)
            timestamp = self._get_page(is_base, TIMESTAMP_COLUMN, page_index).read(slot)
            schema_encoding = self._get_page(is_base, SCHEMA_ENCODING_COLUMN, page_index).read(slot)
            
            values = []
            for i in range(4, self.total_columns):
                page = self._get_page(is_base, i, page_index)
                value = page.read(slot)
                if value is None:
                    return None
                values.append(value)
                
            record = Record(rid, values[self.key], values)
            record.indirection = indirection
            record.timestamp = timestamp
            record.schema_encoding = schema_encoding
            
            return record
            
        except Exception as e:
            self._debug_log(f"Error in get_record: {str(e)}", "ERROR")
            return None

    def update_record(self, rid, schema_encoding, *columns, transaction_id=None):
        """Update record with proper locking"""
        try:
            # Acquire exclusive lock if transaction_id provided
            if transaction_id is not None:
                if not self.lock_manager.acquire_lock(transaction_id, rid, LockType.EXCLUSIVE):
                    raise LockConflictError(f"Could not acquire exclusive lock on rid {rid}")
                    
            if rid not in self.page_directory:
                return False
                
            base_record = self.get_record(rid)
            if not base_record:
                return False

            # Create tail record
            tail_rid = self.num_records + self.num_updates
            timestamp = int(time() * 1000000)
            
            # Handle tail pages
            current_tail_page_idx = len(self.tail_page_ids[0]) - 1
            need_new_page = any(not self._get_page(False, col, current_tail_page_idx).has_capacity() 
                              for col in range(self.total_columns))
            
            if need_new_page:
                for col in range(self.total_columns):
                    new_page_id = f"{self.name}_tail_{col}_{len(self.tail_page_ids[col])}"
                    self.tail_page_ids[col].append(new_page_id)
                current_tail_page_idx = len(self.tail_page_ids[0]) - 1

            # Write tail record and update base record
            self._write_tail_record(base_record, tail_rid, timestamp, schema_encoding, 
                                  current_tail_page_idx, columns)
            self._update_base_record(rid, tail_rid, timestamp, schema_encoding, columns)

            self.num_updates += 1
            
            # Check if merge is needed
            if self.num_updates % self.MERGE_THRESHOLD == 0 and \
               time() - self.last_merge_time > self.merge_interval:
                self.__merge()
                
            return True

        except Exception as e:
            self._debug_log(f"Error in update_record: {str(e)}", "ERROR")
            return False

    def _write_tail_record(self, base_record, tail_rid, timestamp, schema_encoding, page_idx, new_columns):
        """Helper to write tail record"""
        # Write base values first
        for i, value in enumerate(base_record.columns):
            self.bufferpool.write_to_page(self.name, self.tail_page_ids[i + 4][page_idx], value)
        
        # Update with new values
        for i, value in enumerate(new_columns):
            if value is not None:
                self.bufferpool.write_to_page(self.name, self.tail_page_ids[i + 4][page_idx], value)

        # Write metadata
        tail_indirection = base_record.indirection if base_record.indirection != base_record.rid else None
        self.bufferpool.write_to_page(self.name, self.tail_page_ids[INDIRECTION_COLUMN][page_idx], 
                                    tail_indirection if tail_indirection is not None else base_record.rid)
        self.bufferpool.write_to_page(self.name, self.tail_page_ids[RID_COLUMN][page_idx], tail_rid)
        self.bufferpool.write_to_page(self.name, self.tail_page_ids[TIMESTAMP_COLUMN][page_idx], timestamp)
        self.bufferpool.write_to_page(self.name, self.tail_page_ids[SCHEMA_ENCODING_COLUMN][page_idx], 
                                    schema_encoding)
                                    
        # Register in directory
        tail_slot = self.bufferpool.get_num_records(self.name, self.tail_page_ids[0][page_idx]) - 1
        self.page_directory[tail_rid] = ('tail', page_idx, tail_slot)

    def _update_base_record(self, base_rid, tail_rid, timestamp, schema_encoding, new_columns):
        """Helper to update base record"""
        base_page_type, base_page_idx, base_slot = self.page_directory[base_rid]
        
        # Update base record values
        for i, value in enumerate(new_columns):
            if value is not None:
                self.bufferpool.write_to_page(self.name, self.base_page_ids[i + 4][base_page_idx], 
                                            value, base_slot)

        # Update metadata
        self.bufferpool.write_to_page(self.name, self.base_page_ids[INDIRECTION_COLUMN][base_page_idx], 
                                    tail_rid, base_slot)
        self.bufferpool.write_to_page(self.name, self.base_page_ids[TIMESTAMP_COLUMN][base_page_idx], 
                                    timestamp, base_slot)
        self.bufferpool.write_to_page(self.name, self.base_page_ids[SCHEMA_ENCODING_COLUMN][base_page_idx], 
                                    schema_encoding, base_slot)

    def __merge(self):
        """Merge tail records into base records"""
        try:
            self._debug_log("Starting merge operation")
            
            # Track records to merge
            merge_candidates = {}  # {base_rid: [tail_rids]}
            
            # Build merge candidates list
            for rid, (page_type, _, _) in self.page_directory.items():
                if page_type == 'base':
                    record = self.get_record(rid)
                    if record and record.indirection != rid:
                        merge_candidates[rid] = []
                        current_rid = record.indirection
                        while current_rid is not None:
                            tail_record = self.get_record(current_rid)
                            if not tail_record:
                                break
                            merge_candidates[rid].append(current_rid)
                            current_rid = tail_record.indirection

            # Process each base record
            for base_rid, tail_rids in merge_candidates.items():
                if not tail_rids:
                    continue
                    
                # Get the most recent values
                base_record = self.get_record(base_rid)
                final_values = list(base_record.columns)
                final_schema = 0
                
                # Apply updates in reverse order
                for tail_rid in reversed(tail_rids):
                    tail_record = self.get_record(tail_rid)
                    if not tail_record:
                        continue
                        
                    # Update values based on schema
                    schema = tail_record.schema_encoding
                    for i, value in enumerate(tail_record.columns):
                        if schema & (1 << i):
                            final_values[i] = value
                            final_schema |= (1 << i)

                # Update base record with merged values
                self._update_base_record(base_rid, base_rid, int(time() * 1000000), 
                                      final_schema, final_values)
                
                # Mark tail records as merged
                for tail_rid in tail_rids:
                    if tail_rid in self.page_directory:
                        self.page_directory[tail_rid] = ('merged', 0, 0)

            self.last_merge_time = time()
            self._debug_log("Merge operation completed")
            
        except Exception as e:
            self._debug_log(f"Error in merge operation: {str(e)}", "ERROR")

    def rollback_record(self, rid, transaction_id):
        """Rollback changes made to a record"""
        try:
            if rid not in self.page_directory:
                return False
                
            record = self.get_record(rid)
            if not record or record.indirection == rid:
                return False  # No changes to rollback
                
            # Get the previous version
            prev_record = self.get_record(record.indirection)
            if not prev_record:
                return False
                
            # Restore previous values
            self._update_base_record(rid, rid, int(time() * 1000000), 
                                   prev_record.schema_encoding, prev_record.columns)
            return True
            
        except Exception as e:
            self._debug_log(f"Error in rollback_record: {str(e)}", "ERROR")
            return False

    def get_record_version(self, rid, version):
        """Get a specific version of a record"""
        try:
            if rid not in self.page_directory:
                return None
                
            record = self.get_record(rid)
            if not record:
                return None
                
            if version == 0:  # Current version
                return record
                
            # Navigate version chain
            current = record
            version_count = 0
            while current and current.indirection and current.indirection != current.rid:
                if version_count == abs(version):
                    return current
                current = self.get_record(current.indirection)
                version_count += 1
                
            return current
            
        except Exception as e:
            self._debug_log(f"Error in get_record_version: {str(e)}", "ERROR")
            return None