from lstore.db import Database
from lstore.query import Query
from time import process_time
from random import choice, randrange

print("=== Starting the Database Performance Test ===")

# Step 1: Create Database and Table
print("Creating a database and a 'Grades' table with 5 columns (Student ID + 4 grades)...")
db = Database()
grades_table = db.create_table('Grades', 5, 0)  # Primary key on column 0
query = Query(grades_table)
keys = []

# Step 2: Insert 10k Records
print("\n[INSERT] Inserting 10,000 records...")
insert_time_0 = process_time()
for i in range(0, 10000):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)
    if (i + 1) % 500 == 0:
        print(f"Inserted {i + 1} records so far...")
insert_time_1 = process_time()
print(f"Inserting 10k records took:\t\t\t{insert_time_1 - insert_time_0:.6f} seconds")

# Step 3: Update 10k Records
print("\n[UPDATE] Updating 10,000 records...")
update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]

update_time_0 = process_time()
for i in range(0, 10000):
    key = choice(keys)
    cols = choice(update_cols)
    query.update(key, *cols)
    if (i + 1) % 500 == 0:
        print(f"Updated {i + 1} records so far...")
update_time_1 = process_time()
print(f"Updating 10k records took:\t\t\t{update_time_1 - update_time_0:.6f} seconds")

# Step 4: Select 10k Records
print("\n[SELECT] Selecting 10,000 records...")
select_time_0 = process_time()
for i in range(0, 10000):
    key = choice(keys)
    query.select(key, 0, [1, 1, 1, 1, 1])
    if (i + 1) % 500 == 0:
        print(f"Selected {i + 1} records so far...")
select_time_1 = process_time()
print(f"Selecting 10k records took:\t\t\t{select_time_1 - select_time_0:.6f} seconds")

# Step 5: Aggregate Operation on 10k Records (in 100-record batches)
print("\n[AGGREGATE] Performing aggregate queries in 100-record batches...")
agg_time_0 = process_time()
for i in range(0, 10000, 100):
    start_value = 906659671 + i
    end_value = start_value + 100
    column = randrange(0, 5)
    result = query.sum(start_value, end_value - 1, column)
    print(f"Aggregated sum from key {start_value} to {end_value - 1} on column {column}: {result}")
agg_time_1 = process_time()
print(f"Aggregate 10k of 100 record batch took:\t{agg_time_1 - agg_time_0:.6f} seconds")

# Step 6: Delete 10k Records
print("\n[DELETE] Deleting 10,000 records...")
delete_time_0 = process_time()
for i in range(0, 10000):
    key = 906659671 + i
    query.delete(key)
    if (i + 1) % 100 == 0:
        print(f"Deleted {i + 1} records so far...")
delete_time_1 = process_time()
print(f"Deleting 10k records took:\t\t\t{delete_time_1 - delete_time_0:.6f} seconds")

# Final Summary
print("\n=== Database Performance Test Completed ===")
print("END OF MAIN.PY")
