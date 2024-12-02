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

class Bufferpool:
    def __init__(self, pool_size):
        self.pool_size = pool_size
        self.pool = {}
        self.lru = []
        
    def get_page(self, table_name, page_id):
        """
        Retrieves a page from bufferpool or disk if necessary
        """
        pool_key = f"{table_name}_{page_id}"
        
        if pool_key in self.pool:
            self.lru.remove(pool_key)
            self.lru.append(pool_key)
            page, pin_count, is_dirty = self.pool[pool_key]
            self.pool[pool_key] = (page, pin_count + 1, is_dirty)
            return page
            
        page = self._load_page_from_disk(table_name, page_id)
        
        if len(self.pool) >= self.pool_size:
            self._evict_page()
            
        self.pool[pool_key] = (page, 1, False)  # pin_count=1, is_dirty=False
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
        """
        Loads a page from disk. Returns a new page if file doesn't exist.
        """
        directory = f"data/{table_name}"
        filepath = f"{directory}/{page_id}.db"
        
        if not os.path.exists(filepath):
            return Page()
            
        try:
            with open(filepath, 'rb') as f:
                page = Page()
                page.data = bytearray(f.read())
                # Calculate number of records
                for i in range(512):  # 4096/8 = 512 possible records
                    if i * 8 >= len(page.data) or all(b == 0 for b in page.data[i*8:(i+1)*8]):
                        page.num_records = i
                        break
                return page
        except Exception:
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