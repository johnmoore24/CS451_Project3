class Page:
    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)  # 4KB page size
        
    def has_capacity(self):
        return self.num_records < 512  # 4096/8 = 512 (64-bit integers)
        
    def write(self, value, index=None):
        """
        Write value to page. If index is provided, update existing value.
        Otherwise, append new value.
        """
        if index is not None:
            if index >= self.num_records:  # Can only update existing records
                return False
            offset = index * 8
        else:
            if not self.has_capacity():
                return False
            offset = self.num_records * 8
            self.num_records += 1
            
        try:
            # Ensure value is within valid range for 8-byte signed integer
            if not (-9223372036854775808 <= int(value) <= 9223372036854775807):
                with open('pt3_testoutput.txt', 'a') as f:
                    f.write(f"[PAGE] Warning: Value {value} outside valid range\n")
                return False
                
            self.data[offset:offset + 8] = int(value).to_bytes(8, 'big', signed=True)
            with open('pt3_testoutput.txt', 'a') as f:
                f.write(f"[PAGE] Writing value {value} at offset {offset}\n")
            return True
            
        except (OverflowError, ValueError) as e:
            with open('pt3_testoutput.txt', 'a') as f:
                f.write(f"[PAGE] Error writing value {value}: {str(e)}\n")
            return False
        
    def read(self, index):
        if index >= self.num_records:
            with open('pt3_testoutput.txt', 'a') as f:
                f.write(f"[PAGE] Attempted to read invalid index {index} >= {self.num_records}\n")
            return None
            
        try:
            offset = index * 8
            value = int.from_bytes(self.data[offset:offset + 8], 'big', signed=True)
            with open('pt3_testoutput.txt', 'a') as f:
                f.write(f"[PAGE] offset={offset}: {value}\n")
            return value
            
        except Exception as e:
            with open('pt3_testoutput.txt', 'a') as f:
                f.write(f"[PAGE] Error reading from offset {offset}: {str(e)}\n")
            return None