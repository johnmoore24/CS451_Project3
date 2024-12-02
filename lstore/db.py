"""
general interface to the database and handles high-level operations such
as starting and shutting down the database instance and loading the database from stored disk files.

This class also handles the creation and deletion of tables via the create and drop function. The
create function will create a new table in the database. 

The Table constructor takes as input the name of the table, the number of columns, and the index of the key column. The drop function
drops the specified table. 

In this milestone, we have also added open and close functions for
reading and writing all data (not the indexes) to files at the restart.
"""
from lstore.table import Table
from lstore.bufferpool import Bufferpool
from lstore.index import Index
from lstore.lock_manager import LockManager
from lstore.transaction_exceptions import *
import os
import json

class Database:
    def __init__(self):
        self.tables = {}
        self.bufferpool = Bufferpool(pool_size=1000)
        self.lock_manager = LockManager()
        self.data_path = None
        self.active_transactions = set()  # Track active transactions
        
    def open(self, path):
        """Open or create database at specified path"""
        self.data_path = path
        print(f"DEBUG: Opening database at {path}")
        
        if not os.path.exists(path):
            os.makedirs(path)
            print(f"DEBUG: No existing database found. Creating new at {path}")
            return

        try:
            metadata_path = os.path.join(path, 'metadata.json')
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                    print(f"DEBUG: Loaded metadata for tables: {list(metadata.keys())}")
                    
                    for table_name, table_info in metadata.items():
                        print(f"DEBUG: Loading table {table_name}")
                        table_metadata_path = os.path.join(path, f"{table_name}_metadata.json")
                        
                        if os.path.exists(table_metadata_path):
                            with open(table_metadata_path, 'r') as tf:
                                table_metadata = json.load(tf)
                                
                                table = Table.from_metadata(
                                    name=table_name,
                                    num_columns=table_info['num_columns'],
                                    key=table_info['key'],
                                    bufferpool=self.bufferpool,
                                    metadata=table_metadata,
                                    lock_manager=self.lock_manager
                                )
                                
                                print(f"DEBUG: Table {table_name} loaded - attributes check:")
                                print(f"  - bufferpool: {table.bufferpool is not None}")
                                print(f"  - lock_manager: {table.lock_manager is not None}")
                                print(f"  - num_columns: {table.num_columns}")
                                print(f"  - num_records: {table.num_records}")
                                
                                self.tables[table_name] = table
                                
                print(f"Database loaded from {path}.")
                
        except Exception as e:
            print(f"Error loading database: {str(e)}")
            import traceback
            print(traceback.format_exc())
            print("Starting with a new database.")
            self.tables = {}

    def close(self, path):
        """Save database state to disk and cleanup"""
        try:
            # Wait for active transactions to complete
            if self.active_transactions:
                print(f"Waiting for {len(self.active_transactions)} active transactions to complete...")
                # In practice, might want to add timeout logic here
            
            # Clear all locks
            self.lock_manager.clear_all()
            
            # Ensure the directory exists
            os.makedirs(path, exist_ok=True)
            
            # Save general metadata (table names and structure)
            metadata = {}
            for table_name, table in self.tables.items():
                metadata[table_name] = {
                    'num_columns': table.num_columns,
                    'key': table.key
                }
                
                # Save individual table metadata
                table_metadata = {
                    'page_directory': {str(k): list(v) for k, v in table.page_directory.items()},
                    'num_records': table.num_records,
                    'num_updates': table.num_updates,
                    'base_page_ids': table.base_page_ids,
                    'tail_page_ids': table.tail_page_ids,
                    'index_data': {
                        str(i): {str(k): v for k, v in (index_dict or {}).items()}
                        for i, index_dict in enumerate(table.index.indices)
                        if index_dict is not None
                    }
                }
                
                table_metadata_path = os.path.join(path, f"{table_name}_metadata.json")
                with open(table_metadata_path, 'w') as f:
                    json.dump(table_metadata, f)

            # Save main metadata file
            metadata_path = os.path.join(path, 'metadata.json')
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f)
                
            # Flush bufferpool
            self.bufferpool.close()
            print(f"Database saved to {path}.")
            
        except Exception as e:
            print(f"Error saving database: {str(e)}")
            import traceback
            print(traceback.format_exc())

    def create_table(self, name, num_columns, key_index):
        """Create a new table or properly initialize existing table"""
        try:
            print(f"DEBUG: Creating/getting table '{name}'")
            print(f"DEBUG: Current tables: {list(self.tables.keys())}")
            
            if name in self.tables:
                print(f"DEBUG: Table '{name}' exists, verifying state")
                existing_table = self.tables[name]
                
                # Verify table state
                print(f"DEBUG: Existing table state:")
                print(f"  - bufferpool: {existing_table.bufferpool is not None}")
                print(f"  - lock_manager: {existing_table.lock_manager is not None}")
                print(f"  - num_columns: {existing_table.num_columns}")
                print(f"  - num_records: {getattr(existing_table, 'num_records', None)}")
                
                # Re-initialize if needed
                if not existing_table.bufferpool:
                    existing_table.bufferpool = self.bufferpool
                if not existing_table.lock_manager:
                    existing_table.lock_manager = self.lock_manager
                if not existing_table.index:
                    existing_table.index = Index(existing_table)
                if not hasattr(existing_table, 'num_records'):
                    existing_table.num_records = 0
                    
                return existing_table
                
            # Create new table
            print(f"DEBUG: Creating new table")
            table = Table(name, num_columns, key_index, self.bufferpool, self.lock_manager)
            self.tables[name] = table
            
            # Verify new table state
            print(f"DEBUG: New table state:")
            print(f"  - bufferpool: {table.bufferpool is not None}")
            print(f"  - lock_manager: {table.lock_manager is not None}")
            print(f"  - num_columns: {table.num_columns}")
            print(f"  - num_records: {table.num_records}")
            
            return table
            
        except Exception as e:
            print(f"ERROR in create_table: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return None

    def drop_table(self, name):
        """Drop a table"""
        if name not in self.tables:
            print(f"Table '{name}' does not exist.")
            return False
            
        # Remove table metadata file if it exists
        if self.data_path:
            table_metadata_path = os.path.join(self.data_path, f"{name}_metadata.json")
            if os.path.exists(table_metadata_path):
                os.remove(table_metadata_path)
                
        # Release all locks for this table
        # In practice, might want to wait for transactions to complete
        table = self.tables[name]
        # TODO: Implement table-specific lock cleanup
        
        del self.tables[name]
        print(f"Table '{name}' dropped successfully.")
        return True

    def get_table(self, name):
        """Get a table by name"""
        return self.tables.get(name)
        
    def begin_transaction(self):
        """Begin a new transaction"""
        transaction_id = len(self.active_transactions)
        self.active_transactions.add(transaction_id)
        return transaction_id
        
    def commit_transaction(self, transaction_id):
        """Commit a transaction"""
        if transaction_id in self.active_transactions:
            self.active_transactions.remove(transaction_id)
            
    def abort_transaction(self, transaction_id):
        """Abort a transaction"""
        try:
            self._debug_log(f"Aborting transaction {transaction_id}")
            if transaction_id in self.active_transactions:
                # Log current state
                self._debug_log(f"Transaction state before abort: {self.transaction_states.get(transaction_id)}")
                self._debug_log(f"Active locks: {self.lock_manager.get_transaction_locks(transaction_id)}")
                
                # Release all locks held by this transaction
                self.lock_manager.release_all_locks(transaction_id)
                self.active_transactions.remove(transaction_id)
                self.transaction_states[transaction_id] = 'ABORTED'
                
                self._debug_log(f"Transaction {transaction_id} aborted successfully")
            else:
                self._debug_log(f"Warning: Attempting to abort non-active transaction {transaction_id}")
        except Exception as e:
            self._debug_log(f"Error aborting transaction {transaction_id}: {str(e)}")
            raise