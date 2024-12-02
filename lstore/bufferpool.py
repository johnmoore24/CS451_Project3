"""
Bufferpool Management System

The bufferpool is a memory management layer that sits between the database and disk storage.
It maintains a fixed-size pool of pages in memory to optimize database performance by:
1. Keeping frequently accessed pages in memory
2. Managing page eviction using LRU (Least Recently Used) policy
3. Handling dirty page tracking and disk writes
4. Providing pin/unpin mechanisms to protect active pages
"""

from lstore.page import Page
import os
import json

class Bufferpool:
    def __init__(self, pool_size):
        self.pool_size = pool_size
        self.pool = {}
        self.lru = []
        self.page_directory = {}
        self.debug = False
        
        # Load page directory from metadata
        try:
            with open('CS451/Grades_metadata.json', 'r') as f:
                metadata = json.loads(f.read())
                if 'page_directory' in metadata:
                    self._debug_log(f"Loading page directory from metadata")
                    self.page_directory = metadata['page_directory']
                    self._debug_log(f"Loaded {len(self.page_directory)} page directory entries")
        except Exception as e:
            self._debug_log(f"Failed to load page directory: {str(e)}")
        
    def _debug_log(self, message):
        if self.debug:
            print(f"BUFFERPOOL: {message}")
        
    def get_page(self, table_name, page_id):
        """
        Retrieves a page from bufferpool or disk if necessary
        """
        self._debug_log(f"\n=== GET PAGE START for {table_name}/{page_id} ===")
        pool_key = f"{table_name}_{page_id}"
        
        # Check if in memory
        if pool_key in self.pool:
            self._debug_log(f"Found page in pool")
            self.lru.remove(pool_key)
            self.lru.append(pool_key)
            page, pin_count, is_dirty = self.pool[pool_key]
            self.pool[pool_key] = (page, pin_count + 1, is_dirty)
            return page
            
        # Load from disk
        self._debug_log(f"Page not in pool, loading from disk")
        page = self._load_page_from_disk(table_name, page_id)
        
        # Add to pool
        if len(self.pool) >= self.pool_size:
            self._debug_log(f"Pool full, evicting page")
            self._evict_page()
            
        self.pool[pool_key] = (page, 1, False)
        self.lru.append(pool_key)
        
        return page
        
    def _extract_page_info(self, pool_key):
        """Safely extracts table name and page id from pool key"""
        parts = pool_key.split('_', 1)  # Split on first underscore only
        if len(parts) != 2:
            raise ValueError(f"Invalid pool key format: {pool_key}")
        return parts[0], parts[1]
        
    def write_to_page(self, table_name, page_id, value, index=None):
        """
        Writes a value to a page and marks it as dirty
        """
        page = self.get_page(table_name, page_id)
        success = page.write(value, index)
        if success:
            self.mark_dirty(table_name, page_id)
        return success
        
    def read_from_page(self, table_name, page_id, index):
        """
        Reads a value from a page
        """
        page = self.get_page(table_name, page_id)
        return page.read(index)
        
    def unpin_page(self, table_name, page_id):
        pool_key = f"{table_name}_{page_id}"
        if pool_key in self.pool:
            page, pin_count, is_dirty = self.pool[pool_key]
            if pin_count > 0:
                self.pool[pool_key] = (page, pin_count - 1, is_dirty)
                
    def mark_dirty(self, table_name, page_id):
        pool_key = f"{table_name}_{page_id}"
        if pool_key in self.pool:
            page, pin_count, _ = self.pool[pool_key]
            self.pool[pool_key] = (page, pin_count, True)
            
    def _evict_page(self):
        """
        Evicts the least recently used unpinned page
        """
        for pool_key in self.lru:
            page, pin_count, is_dirty = self.pool[pool_key]
            if pin_count == 0:  # Only evict unpinned pages
                if is_dirty:
                    try:
                        table_name, page_id = self._extract_page_info(pool_key)
                        self._write_page_to_disk(table_name, page_id, page)
                    except ValueError:
                        continue
                del self.pool[pool_key]
                self.lru.remove(pool_key)
                return
                
    def _load_page_from_disk(self, table_name, page_id):
        """Loads a page from disk. Returns a new page if file doesn't exist."""
        self._debug_log(f"\n=== LOAD PAGE FROM DISK ===")
        directory = f"data/{table_name}"
        filepath = f"{directory}/{page_id}.db"
        
        self._debug_log(f"Looking for file: {filepath}")
        self._debug_log(f"Directory exists: {os.path.exists(directory)}")
        
        if not os.path.exists(filepath):
            self._debug_log(f"File not found: {filepath}")
            return Page()
            
        try:
            with open(filepath, 'rb') as f:
                page = Page()
                data = f.read()
                self._debug_log(f"Read {len(data)} bytes")
                page.data = bytearray(data)
                
                # Set number of records based on page size
                page.num_records = len(data) // 8  # Assuming 8-byte records
                
                self._debug_log(f"Raw data preview: {page.data[:32].hex()}")  # Show first few bytes in hex
                self._debug_log(f"Loaded page with {page.num_records} records")
                return page
                
        except Exception as e:
            self._debug_log(f"Error loading page: {str(e)}")
            import traceback
            self._debug_log(f"Traceback: {traceback.format_exc()}")
            return Page()
            
    def _write_page_to_disk(self, table_name, page_id, page):
        """
        Writes a page to disk, creating directories if needed
        """
        directory = f"data/{table_name}"
        if not os.path.exists(directory):
            os.makedirs(directory)
            
        filepath = f"{directory}/{page_id}.db"
        with open(filepath, 'wb') as f:
            f.write(page.data)
            
    def close(self):
        """
        Flushes all dirty pages to disk
        """
        for pool_key in list(self.pool.keys()):
            page, pin_count, is_dirty = self.pool[pool_key]
            if is_dirty:
                try:
                    table_name, page_id = self._extract_page_info(pool_key)
                    self._write_page_to_disk(table_name, page_id, page)
                except ValueError:
                    continue
                    
    def get_num_records(self, table_name, page_id):
        """
        Returns the number of records in a page
        """
        page = self.get_page(table_name, page_id)
        return page.num_records
        
    def get_record(self, rid):
        self._debug_log(f"\n=== GET RECORD START for RID {rid} ===")
        
        try:
            # Get page info from directory
            if str(rid) not in self.page_directory:
                self._debug_log(f"RID {rid} not found in directory")
                return None
            
            page_info = self.page_directory[str(rid)]
            self._debug_log(f"Page directory entry for RID {rid}: {page_info}")
            
            # Get specific page
            page_type, page_index, slot = page_info
            page_id = f"base_{page_type}_{page_index}"  # Changed format to match logs
            
            self._debug_log(f"Looking for specific page: {page_id}")
            page = self.get_page("Grades", page_id)
            
            if not page:
                self._debug_log(f"Failed to get page {page_id}")
                return None
            
            # Try to read record
            self._debug_log(f"Reading from slot {slot} in page {page_id}")
            self._debug_log(f"Page data size: {len(page.data)}, num_records: {page.num_records}")
            
            value = page.read(slot)
            self._debug_log(f"Read result: {value}")
            
            return value
            
        except Exception as e:
            self._debug_log(f"Error in get_record: {str(e)}")
            import traceback
            self._debug_log(f"Traceback: {traceback.format_exc()}")
            return None

    def load_page_from_disk(self, page_id):
        """Load a page from disk into the bufferpool"""
        table_name = page_id.split('_')[0]  # Extract table name from page_id
        page = self._load_page_from_disk(table_name, page_id)
        self.pool[page_id] = (page, 1, False)  # pin_count=1, is_dirty=False
        self.lru.append(page_id)
        self._debug_log(f"Page {page_id} loaded into bufferpool")