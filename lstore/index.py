import threading

class Index:
    """
    A data structure holding indices for various columns of a table.
    Key column should be indexed by default, other columns can be indexed through this object.
    """
    def __init__(self, table):
        self.table = table
        # Dictionary of dictionaries: {column_index: {value: rid}}
        self.indices = [None] * table.num_columns
        # Initialize index for key column
        self.create_index(table.key)
        self._debug_log(f"Created index for table {table.name}, key column: {table.key}")
        
    @classmethod
    def from_metadata(cls, table, index_data):
        """
        Create an index instance from saved metadata
        """
        index = cls(table)
        # Reconstruct indices from saved data
        for col_str, index_dict in index_data.items():
            col = int(col_str)
            if col < len(index.indices):
                # Convert string keys back to integers for the index
                index.indices[col] = {
                    int(k): int(v) for k, v in index_dict.items()
                }
        return index
        
    def to_metadata(self):
        """
        Convert index state to serializable format
        """
        return {
            str(i): {str(k): v for k, v in (index_dict or {}).items()}
            for i, index_dict in enumerate(self.indices)
            if index_dict is not None
        }

    def _debug_log(self, message):
        """Helper method to log debug messages"""
        # with open('pt3_testoutput.txt', 'a') as f:
        #     f.write(f"[INDEX {self.table.name}] {message}\n")

    def locate(self, column, value):
        """Locate the RID for a given value in a column"""
        #print(f"DEBUG INDEX:")
        #print(f"  Looking up value {value} in column {column}")
        # print(f"  Current index state: {self.indices[column]}")
        
        if self.indices[column] is None:
            print("  No index exists for this column")
            return None
            
        rid = self.indices[column].get(value)
        #print(f"  Found RID: {rid}")
        return rid
        
    def locate_range(self, begin, end, column):
        """Returns a sorted list of RIDs of records within the given range"""
        self._debug_log(f"START locate_range({begin}, {end}, {column})", 1)
        self._debug_log(f"Index state for column {column}: {self.indices[column]}", 2)
        
        if self.indices[column] is None:
            self._debug_log("No index exists for this column", 1)
            return []
            
        # Track all operations for debugging
        matching_pairs = []
        excluded_pairs = []
        
        for key, rid in self.indices[column].items():
            if begin <= key <= end:
                matching_pairs.append((key, rid))
                self._debug_log(f"Including key {key} (RID {rid})", 2)
            else:
                excluded_pairs.append((key, rid))
                self._debug_log(f"Excluding key {key} (RID {rid})", 3)
                
        # Sort pairs and log the sorting process
        self._debug_log(f"Pre-sort pairs: {matching_pairs}", 2)
        sorted_pairs = sorted(matching_pairs, key=lambda x: x[0])
        self._debug_log(f"Post-sort pairs: {sorted_pairs}", 2)
        
        # Extract RIDs
        matching_rids = [pair[1] for pair in sorted_pairs]
        
        self._debug_log(f"Final RIDs: {matching_rids}", 1)
        self._debug_log(f"Total matches: {len(matching_rids)}", 1)
        
        return matching_rids

    def create_index(self, column):
        """Create index for the specified column"""
        if column >= len(self.indices):
            self._debug_log(f"Invalid column index: {column}")
            return False
            
        self.indices[column] = {}
        # Build index from existing records
        for rid in self.table.page_directory:
            record = self.table.get_record(rid)
            if record and column < len(record.columns):
                self.indices[column][record.columns[column]] = rid
                
        self._debug_log(f"Built index for column {column} with {len(self.indices[column])} entries")
        return True
        
    def drop_index(self, column_number):
        """Drops the index for the specified column"""
        if column_number >= self.table.num_columns:
            return False
            
        if column_number == self.table.key:
            return False  # Cannot drop index on key column
            
        if self.indices[column_number] is None:
            return False  # Index doesn't exist
            
        self.indices[column_number] = None
        return True

    def update_index(self, column, value, rid):
        """Update or insert an index entry"""
        if column >= len(self.indices) or self.indices[column] is None:
            return False
            
        old_value = None
        # Find and remove old value if it exists
        for val, old_rid in self.indices[column].items():
            if old_rid == rid:
                old_value = val
                break
                
        if old_value is not None:
            del self.indices[column][old_value]
                
        self.indices[column][value] = rid
        return True
        
    def rebuild_index(self):
        """
        Rebuilds all indices from the table data.
        Useful when loading from disk or after massive changes.
        """
        # Reset all indices except primary key
        self.indices = [None] * self.table.num_columns
        self.indices[self.table.key] = {}
        
        # Rebuild from page directory
        for rid in self.table.page_directory:
            record = self.table.get_record(rid)
            if record and record.columns:
                # Always index the primary key
                self.indices[self.table.key][record.columns[self.table.key]] = rid
                
                # Update other active indices
                for i, index in enumerate(self.indices):
                    if index is not None and i != self.table.key:
                        index[record.columns[i]] = rid