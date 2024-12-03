from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
from random import choice, randint, sample, seed
import sys

# Set up file output
original_stdout = sys.stdout
f = open('test3_2_output.txt', 'w')
sys.stdout = f

db = Database()
db.open('./CS451')

# Getting the existing Grades table
grades_table = db.get_table('Grades')

# create a query class for the grades table
query = Query(grades_table)

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_transactions = 100
number_of_operations_per_record = 1
num_threads = 8

keys = []
records = {}
seed(3562901)

# re-generate records for testing
for i in range(0, number_of_records):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    print(records[key])

transaction_workers = []
transactions = []

for i in range(number_of_transactions):
    transactions.append(Transaction())

for i in range(num_threads):
    transaction_workers.append(TransactionWorker())




updated_records = {}
# x update on every column
for j in range(number_of_operations_per_record):
    for key in keys:
        updated_columns = [None, None, None, None, None]
        updated_records[key] = records[key].copy()
        for i in range(2, grades_table.num_columns):
            # updated value
            value = randint(0, 20)
            updated_columns[i] = value
            # update our test directory
            updated_records[key][i] = value
        transactions[key % number_of_transactions].add_query(query.select, grades_table, key, 0, [1, 1, 1, 1, 1])
        transactions[key % number_of_transactions].add_query(query.update, grades_table, key, *updated_columns)
print("Update finished")


# add trasactions to transaction workers  
for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(transactions[i])



# run transaction workers
for i in range(num_threads):
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()

def debug_version_chain(table, rid, indent="  "):
    """Helper function to print version chain details"""
    chain = []
    visited = set()
    current = table.get_record(rid)
    
    while current and current.indirection and current.indirection != current.rid:
        chain_info = f"{indent}RID: {current.rid}, "
        chain_info += f"Columns: {current.columns}, "
        chain_info += f"Schema: {current.schema_encoding}, "
        chain_info += f"Indirection: {current.indirection}"
        chain.append(chain_info)
        
        if current.indirection in visited:
            chain.append(f"{indent}CYCLE DETECTED at {current.indirection}!")
            break
        visited.add(current.indirection)
        current = table.get_record(current.indirection)
        if not current:
            chain.append(f"{indent}Chain broken - couldn't fetch record {current.indirection}")
            break
            
    return "\n".join(chain)

# Add this after your imports
DEBUG_KEY = 92106430  # Pick a key that's failing

# Add this before the version checking loops
def test_single_key(key):
    """Test a single key with detailed output"""
    print(f"\n=== Testing key {key} ===")
    print(f"Original record: {records[key]}")
    print(f"Updated record: {updated_records[key]}")
    
    # Get initial chain state
    print("\nInitial version chain:")
    rid = grades_table.index.locate(grades_table.key, key)
    print(debug_version_chain(grades_table, rid))
    
    query = Query(grades_table)
    
    print("\nTesting version -1:")
    print("Expected behavior: Should return the record state before the most recent update")
    result = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0]
    print(f"Got RID:   {result.rid}")
    print(f"Got:       {result.columns}")
    print(f"Expected:  {records[key]}")
    print(f"Matches:   {result.columns == records[key]}")
    if result.columns != records[key]:
        print("Column differences:")
        for i, (got, expected) in enumerate(zip(result.columns, records[key])):
            if got != expected:
                print(f"  Column {i}: Got {got}, Expected {expected}")
    
    print("\nTesting version -2:")
    print("Expected behavior: Should return the record state two updates ago")
    result = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0]
    print(f"Got RID:   {result.rid}")
    print(f"Got:       {result.columns}")
    print(f"Expected:  {records[key]}")
    print(f"Matches:   {result.columns == records[key]}")
    if result.columns != records[key]:
        print("Column differences:")
        for i, (got, expected) in enumerate(zip(result.columns, records[key])):
            if got != expected:
                print(f"  Column {i}: Got {got}, Expected {expected}")
    
    print("\nTesting version 0:")
    print("Expected behavior: Should return current record state")
    result = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0]
    print(f"Got RID:   {result.rid}")
    print(f"Got:       {result.columns}")
    print(f"Expected:  {updated_records[key]}")
    print(f"Matches:   {result.columns == updated_records[key]}")
    if result.columns != updated_records[key]:
        print("Column differences:")
        for i, (got, expected) in enumerate(zip(result.columns, updated_records[key])):
            if got != expected:
                print(f"  Column {i}: Got {got}, Expected {expected}")

# Add this before running the full tests
test_single_key(DEBUG_KEY)

# Version -1 checking
score = len(keys)
for key in keys:
    correct = records[key]
    query = Query(grades_table)
    
    try:
        result = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0].columns
        if correct != result:
            print(f'\nSelect error on primary key {key}:')
            print(f'Got:      {result}')
            print(f'Expected: {correct}')
            print(f'Version chain:')
            rid = grades_table.index.locate(grades_table.key, key)
            print(f'  {debug_version_chain(grades_table, rid)}')
            score -= 1
    except Exception as e:
        print(f'\nException on primary key {key}:')
        print(f'Error: {str(e)}')
        import traceback
        print(traceback.format_exc())
        score -= 1
print('Version -1 Score:', score, '/', len(keys))

# Version -2 checking
v2_score = len(keys)
for key in keys:
    correct = records[key]
    query = Query(grades_table)
    
    try:
        result = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0].columns
        if correct != result:
            print(f'\nSelect error on primary key {key}:')
            print(f'Got:      {result}')
            print(f'Expected: {correct}')
            print(f'Version chain:')
            rid = grades_table.index.locate(grades_table.key, key)
            print(f'  {debug_version_chain(grades_table, rid)}')
            v2_score -= 1
    except Exception as e:
        print(f'\nException on primary key {key}:')
        print(f'Error: {str(e)}')
        import traceback
        print(traceback.format_exc())
        v2_score -= 1
print('Version -2 Score:', v2_score, '/', len(keys))
if score != v2_score:
    print('Failure: Version -1 and Version -2 scores must be same')

score = len(keys)
for key in keys:
    correct = updated_records[key]
    query = Query(grades_table)
    
    result = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0].columns
    if correct != result:
        print('select error on primary key', key, ':', result, ', correct:', correct)
        score -= 1
print('Version 0 Score:', score, '/', len(keys))

number_of_aggregates = 100
valid_sums = 0
for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum_version(keys[r[0]], keys[r[1]], 0, -1)
    if column_sum == result:
        valid_sums += 1
print("Aggregate version -1 finished. Valid Aggregations: ", valid_sums, '/', number_of_aggregates)

v2_valid_sums = 0
for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: records[x][0] if x in records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum_version(keys[r[0]], keys[r[1]], 0, -2)
    if column_sum == result:
        v2_valid_sums += 1
print("Aggregate version -2 finished. Valid Aggregations: ", v2_valid_sums, '/', number_of_aggregates)
if valid_sums != v2_valid_sums:
    print('Failure: Version -1 and Version -2 aggregation scores must be same.')

valid_sums = 0
for i in range(0, number_of_aggregates):
    r = sorted(sample(range(0, len(keys)), 2))
    column_sum = sum(map(lambda x: updated_records[x][0] if x in updated_records else 0, keys[r[0]: r[1] + 1]))
    result = query.sum_version(keys[r[0]], keys[r[1]], 0, 0)
    if column_sum == result:
        valid_sums += 1
print("Aggregate version 0 finished. Valid Aggregations: ", valid_sums, '/', number_of_aggregates)

db.close('./CS451')

# Restore stdout and close file
sys.stdout = original_stdout
f.close()

# Print final scores to console
with open('test3_2_output.txt', 'r') as f:
    for line in f:
        if 'Score:' in line or 'Aggregations:' in line:
            print(line.strip())