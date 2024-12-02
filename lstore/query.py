from lstore.table import Table, Record
from lstore.index import Index
from time import time

# Define metadata column indices
INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

class Query:
    """
    Handles all query operations on the table including insertions, deletions,
    updates, selections, and summations.
    """
    def __init__(self, table):
        if table is None:
            raise ValueError("Table cannot be None")
        self._table = table  # Use private attribute
        self.verify_table_state()
        self.debug = True
        
    @property
    def table(self):
        """Property to access table with verification"""
        if self._table is None:
            raise ValueError("Query has no associated table")
        return self._table
        
    def verify_table_state(self):
        """Verify table has all required attributes"""
        if not self._table:
            raise ValueError("No table associated with query")
            
        required_attrs = ['num_records', 'num_columns', 'name', 'bufferpool', 'lock_manager']
        missing_attrs = [attr for attr in required_attrs if not hasattr(self._table, attr)]
        
        if missing_attrs:
            raise ValueError(f"Table missing required attributes: {missing_attrs}")
            
        print(f"Table verification passed - {self._table.name}")
        
    def _debug_log(self, message, level=1):
        """Helper method to log debug messages with levels"""
        if self.debug:
            with open('pt3_testoutput.txt', 'a') as f:
                f.write(f"[DEBUG] {message}\n")
                f.flush()
        
    def delete(self, primary_key):
        """Delete record with given primary key."""
        # Locate record using index
        rid = self.table.index.locate(self.table.key, primary_key)
        if rid is None:
            return False
            
        # Get record location from page directory
        if rid not in self.table.page_directory:
            return False
            
        page_type, page_index, record_index = self.table.page_directory[rid]
        if page_type != 'base':
            return False  # Can only delete base records
            
        # Mark record as deleted in page directory
        self.table.page_directory[rid] = ('deleted', page_index, record_index)
        
        # Remove from index
        if self.table.key < len(self.table.index.indices) and self.table.index.indices[self.table.key]:
            self.table.index.indices[self.table.key].pop(primary_key, None)
            
        return True
        
    def insert(self, *columns):
        """Insert a record with specified columns"""
        self._debug_log(f"\n=== INSERT OPERATION ===")
        
        if not self.table:
            self._debug_log("ERROR: No table associated with query")
            return False
            
        if len(columns) != self.table.num_columns:
            self._debug_log(f"ERROR: Column count mismatch. Expected {self.table.num_columns}, got {len(columns)}")
            return False
            
        try:
            record = self.table.create_record(*columns)
            if record:
                self._debug_log(f"Successfully inserted record with columns: {columns}")
                return True
            self._debug_log("Failed to create record")
            return False
            
        except Exception as e:
            self._debug_log(f"ERROR: Exception in insert: {str(e)}")
            import traceback
            self._debug_log(f"Traceback: {traceback.format_exc()}")
            return False

    def select(self, key, column, query_columns):
        """
        # Returns Record(rid, key, columns) if found
        # Returns False if record not found
        """
        try:
            # First check index for the key
            if self.table.index.indices[self.table.key] is not None:
                rid = self.table.index.locate(self.table.key, key)
                if rid is None:
                    return False
                    
                # Get the record using the RID
                record = self.table.get_record(rid)
                if not record:
                    return False
                    
                # Verify this is the correct record
                if record.key != key:  # Add this check!
                    return False
                    
                # Project only requested columns
                if query_columns:
                    projected_columns = []
                    for i, include in enumerate(query_columns):
                        if include:
                            projected_columns.append(record.columns[i])
                        else:
                            projected_columns.append(None)
                    record.columns = projected_columns
                    
                return [record]
                
            # If no index, do full table scan
            records = self.table.get_all_records()
            result = []
            for record in records:
                if record.columns[column] == key:
                    if query_columns:
                        projected_columns = []
                        for i, include in enumerate(query_columns):
                            if include:
                                projected_columns.append(record.columns[i])
                            else:
                                projected_columns.append(None)
                        record.columns = projected_columns
                    result.append(record)
            return result if result else False

        except Exception as e:
            print(f"Error in select: {str(e)}")
            return False
    
    def select_version(self, key, key_index, projected_columns_index, relative_version):
        """
        Select a specific version of a record with logging for version -2
        """
        # Add class variable if it doesn't exist
        if not hasattr(self.__class__, '_debug_count'):
            self.__class__._debug_count = 0

        def log(message):
            # Only log if it's version -2 and we haven't exceeded 100 logs
            if relative_version == -2 and self.__class__._debug_count < 100:
                with open('M1_output.txt', 'a') as f:
                    f.write(str(message) + '\n')
                    f.flush()
        
        try:
            if relative_version == -2 and self.__class__._debug_count < 100:
                self.__class__._debug_count += 1
                log(f"\n====== SELECT VERSION -2 START (Query {self.__class__._debug_count}/100) ======")
            
            # Get base record RID
            rid = self.table.index.locate(key_index, key)
            if rid is None:
                return [Record(None, key, [None] * sum(projected_columns_index))]

            # Get base record
            base_record = self.table.get_record(rid)
            if not base_record:
                return [Record(None, key, [None] * sum(projected_columns_index))]

            # For version 0, return current record
            if relative_version == 0:
                projected_columns = [base_record.columns[i] for i, include in enumerate(projected_columns_index) if include]
                return [Record(base_record.rid, key, projected_columns)]

            # Build version chain
            versions = []
            current_record = base_record
            visited = {rid}

            while current_record and current_record.indirection is not None:
                if (current_record.indirection in visited or 
                    current_record.indirection == current_record.rid):
                    break

                next_record = self.table.get_record(current_record.indirection)
                if not next_record:
                    break

                versions.append(next_record)
                visited.add(next_record.rid)
                current_record = next_record

            if relative_version == -2:
                log(f"Found {len(versions)} versions in chain")
                for i, v in enumerate(versions):
                    log(f"Version {i}: RID={v.rid}, Columns={v.columns}")

            # Handle no history or version too old
            if not versions:
                return [Record(base_record.rid, key,
                            [base_record.columns[i] for i, include in enumerate(projected_columns_index) if include])]
                            
            if abs(relative_version) > len(versions):
                # Return original record state
                projected_columns = [base_record.columns[i] for i, include in enumerate(projected_columns_index) if include]
                result = Record(base_record.rid, key, projected_columns)
                return [result]

            # KEY CHANGE: For version -2, we want original state before first update
            if relative_version == -2:
                # Start with base record state
                result_columns = list(versions[-1].columns)  # Use oldest tail record's values
                log(f"Using oldest version columns: {result_columns}")
                
                projected_columns = [result_columns[i] for i, include in enumerate(projected_columns_index) if include]
                result = Record(base_record.rid, key, projected_columns)
                log(f"Final version -2 columns: {projected_columns}")
                return [result]
                
            # For version -1, use the previous version
            elif relative_version == -1:
                # Use the first tail record's state directly
                projected_columns = [versions[0].columns[i] for i, include in enumerate(projected_columns_index) if include]
                result = Record(base_record.rid, key, projected_columns)
                return [result]

            return [Record(base_record.rid, key,
                        [base_record.columns[i] for i, include in enumerate(projected_columns_index) if include])]

        except Exception as e:
            if relative_version == -2:
                log(f"Error in select_version: {str(e)}")
                import traceback
                log(traceback.format_exc())
            return [Record(None, key, [None] * sum(projected_columns_index))]

    def _decode_schema(self, schema_encoding):
        """Convert schema encoding to list of booleans indicating which columns were updated"""
        if isinstance(schema_encoding, str):
            return [bit == '1' for bit in schema_encoding]
        return [(schema_encoding & (1 << i)) != 0 for i in range(self.table.num_columns)]

    def locate(self, column, value):
        """Returns the RID of the record with the given value in the given column"""
        # Validate column index
        if column >= len(self.indices) or column < 0:
            self._debug_log(f"ERROR: Invalid column index {column}. Valid range is 0 to {len(self.indices) - 1}.")
            return None

        # Ensure index for the column exists
        if self.indices[column] is None:
            self._debug_log(f"ERROR: No index exists for column {column}")
            return None

        # Get RID from the index
        rid = self.indices[column].get(value)
        if rid is None:
            self._debug_log(f"INFO: Value '{value}' not found in index for column {column}")
        else:
            self._debug_log(f"INFO: Located RID '{rid}' for value '{value}' in column {column}")

        return rid


    def _project_record_to_list(self, record, projected_columns_index):
        """Helper method to project record columns"""
        projected_values = []
        for i, include in enumerate(projected_columns_index):
            if include:
                projected_values.append(record.columns[i])
        return [Record(record.rid, record.key, projected_values)]
            
    def update(self, primary_key, *columns):
        """Update with verbose logging"""
        self._debug_log(f"\n=== UPDATE OPERATION ===")
        self._debug_log(f"Primary Key: {primary_key}")
        self._debug_log(f"Update columns: {columns}")
        
        # Get current record
        current_record = self.select(primary_key, self.table.key, [1] * self.table.num_columns)
        if not current_record:
            self._debug_log("ERROR: Record not found")
            return False
            
        current_record = current_record[0]
        self._debug_log(f"Current record: {current_record.columns}")
        
        # Create new column values
        new_columns = list(current_record.columns)
        schema_encoding = 0
        
        # Log each column update
        for i, value in enumerate(columns):
            if value is not None:
                self._debug_log(f"Updating column {i}: {new_columns[i]} -> {value}")
                new_columns[i] = value
                schema_encoding |= (1 << i)
                
        self._debug_log(f"New columns: {new_columns}")
        self._debug_log(f"Schema encoding: {bin(schema_encoding)}")
        
        # Update record
        rid = self.table.index.locate(self.table.key, primary_key)
        if rid is None:
            self._debug_log("ERROR: RID not found in index")
            return False
            
        success = self.table.update_record(rid, schema_encoding, *new_columns)
        self._debug_log(f"Update {'successful' if success else 'failed'}")
        
        # Verify update
        updated_record = self.select(primary_key, self.table.key, [1] * self.table.num_columns)
        if updated_record:
            self._debug_log(f"Verification record: {updated_record[0].columns}")
        else:
            self._debug_log("ERROR: Could not verify update")
            
        return success
            
    def sum(self, start_range, end_range, aggregate_column_index):
        """Calculate sum with enhanced debugging and proper update handling"""
        self._debug_log(f"\n=== SUM OPERATION START ===", 1)
        self._debug_log(f"Parameters: range=[{start_range}, {end_range}], column={aggregate_column_index}", 1)
        
        try:
            # Input validation
            if aggregate_column_index >= self.table.num_columns:
                self._debug_log("ERROR: Invalid column index", 1)
                return False
                
            # Get all records in range with detailed tracking
            records_info = {}  # Use dictionary to ensure uniqueness by key
            running_total = 0
            skipped_records = []
            
            # Get matching RIDs from index
            rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            self._debug_log(f"Found {len(rids)} RIDs in range", 1)
            
            # Process each RID
            for rid in rids:
                # Get record
                record = self.table.get_record(rid)
                if not record:
                    skipped_records.append(rid)
                    self._debug_log(f"WARNING: Could not retrieve record for RID {rid}", 1)
                    continue
                    
                # Skip if we've already seen this key (meaning this is an older version)
                if record.key in records_info:
                    self._debug_log(f"Skipping duplicate key {record.key} (RID {rid})", 2)
                    continue
                    
                value = record.columns[aggregate_column_index]
                records_info[record.key] = {
                    'rid': rid,
                    'value': value,
                    'all_columns': record.columns
                }
                running_total += value
                
                self._debug_log(f"Record {rid}:", 2)
                self._debug_log(f"  Key: {record.key}", 2)
                self._debug_log(f"  Value: {value}", 2)
                self._debug_log(f"  Running total: {running_total}", 2)
                    
            # Summary information
            self._debug_log("\n=== SUM OPERATION SUMMARY ===", 1)
            self._debug_log(f"Total records processed: {len(records_info)}", 1)
            self._debug_log(f"Records skipped: {len(skipped_records)}", 1)
            self._debug_log(f"Final sum: {running_total}", 1)
            
            # Detailed record list
            self._debug_log("\n=== DETAILED RECORD LIST ===", 3)
            for key, info in sorted(records_info.items()):
                self._debug_log(f"RID {info['rid']}: key={key}, value={info['value']}", 3)
                
            return running_total
            
        except Exception as e:
            self._debug_log(f"ERROR: Exception in sum operation: {str(e)}", 1)
            import traceback
            self._debug_log(f"Traceback: {traceback.format_exc()}", 1)
            return False
                
            
                
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        """Calculate sum for a specific version over a key range"""
        try:
            if aggregate_column_index >= self.table.num_columns:
                return 0
                
            running_sum = 0
            
            # Get all base records in range using index
            matching_rids = self.table.index.locate_range(start_range, end_range, self.table.key)
            
            # For each matching key, get the correct version and add to sum
            for rid in matching_rids:
                base_record = self.table.get_record(rid)
                if not base_record:
                    continue
                    
                # Get correct version of the record
                versioned_record = self.select_version(
                    base_record.key,
                    self.table.key,
                    [1] * self.table.num_columns,
                    relative_version
                )[0]
                
                if versioned_record and versioned_record.rid is not None:
                    running_sum += versioned_record.columns[aggregate_column_index]
                    
            return running_sum
            
        except Exception as e:
            print(f"Error in sum_version: {str(e)}")
            return 0
        
    def increment(self, key, column):
        """Increment value in specified column for record with given key."""
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
        
    def _project_record(self, record, projected_columns_index):
        """Helper method to project specific columns from a record."""
        projected_columns = []
        for i, include in enumerate(projected_columns_index):
            if include:
                projected_columns.append(record.columns[i])
        return Record(record.rid, record.key, projected_columns)
    
    def _verify_record(self, record, expected_values):
        """Helper method to verify record values"""
        if not record:
            self._debug_log("ERROR: Record is None")
            return False
            
        self._debug_log("=== RECORD VERIFICATION ===")
        self._debug_log(f"Expected: {expected_values}")
        self._debug_log(f"Actual: {record.columns}")
        
        for i, (expected, actual) in enumerate(zip(expected_values, record.columns)):
            if expected != actual:
                self._debug_log(f"Mismatch in column {i}: expected {expected}, got {actual}")
                return False
        return True
    
    def _verify_sum(self, start_range, end_range, aggregate_column_index, expected_sum):
        """Helper method to verify sum calculation"""
        self._debug_log(f"\n=== VERIFYING SUM ===")
        self._debug_log(f"Expected sum: {expected_sum}")

        # Get keys in range
        keys = []
        values = []
        key_index = self.table.key
        index_dict = self.table.index.indices[key_index]

        for key in sorted(index_dict.keys()):
            if start_range <= key <= end_range:
                rid = index_dict[key]
                record = self.table.get_record(rid)
                if record:
                    keys.append(key)
                    values.append(record.columns[aggregate_column_index])
                    
        self._debug_log(f"Keys used in calculation: {keys}")
        self._debug_log(f"Values used in calculation: {values}")

        # Calculate sum
        actual_sum = sum(values)
        self._debug_log(f"Calculated sum: {actual_sum}")
        self._debug_log(f"Match: {actual_sum == expected_sum}")

        return actual_sum == expected_sum