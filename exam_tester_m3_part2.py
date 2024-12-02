from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed

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
    transaction_workers.append(TransactionWorker(worker_id=i))

# Print transaction distribution
key_to_transaction = {}
for i in range(number_of_transactions):
    worker_id = i % num_threads
    transaction_workers[worker_id].add_transaction(transactions[i])
    
    # Track which keys are in this transaction
    for query_func, table, args in transactions[i].queries:
        if query_func.__name__ == 'update':
            key = args[0]
            if key in key_to_transaction:
                print(f"WARNING: Key {key} appears in both transaction {i} (worker {worker_id}) and transaction {key_to_transaction[key]}")
            key_to_transaction[key] = i

print("\nTransaction Distribution:")
for i, worker in enumerate(transaction_workers):
    print(f"Worker {i}: {len(worker.transactions)} transactions")

# Run workers
print("\nStarting Workers...")
for i in range(num_threads):
    transaction_workers[i].run()

print("\nWaiting for completion...")
for i in range(num_threads):
    transaction_workers[i].join()
print("All workers finished")


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

# Open or append to the file
with open('M3_output.txt', 'a') as f:
    f.write("\n=== DETAILED VERSION TESTING ===\n")
    
    # Version -1 Testing
    score = len(keys)
    f.write("\nChecking Version -1:\n")
    for key in keys:
        correct = records[key]
        query = Query(grades_table)
        
        # Get base record info
        base_rid = query.table.index.locate(0, key)
        base_record = query.table.get_record(base_rid)
        
        result = query.select_version(key, 0, [1, 1, 1, 1, 1], -1)[0].columns
        if correct != result:
            f.write(f'\nERROR on key {key}:\n')
            f.write(f'  Base Record:\n')
            f.write(f'    RID: {base_record.rid}\n')
            f.write(f'    Current Values: {base_record.columns}\n')
            f.write(f'    Points to: {base_record.indirection}\n')
            f.write(f'      Schema: {next_record.schema_encoding}\n')
            f.write(f'  Got     : {result}\n')
            f.write(f'  Expected: {correct}\n')
            score -= 1
        else:
            f.write(f'\nSUCCESS on key {key}:\n')
            f.write(f'  Base Record:\n')
            f.write(f'    RID: {base_record.rid}\n')
            f.write(f'    Current Values: {base_record.columns}\n')
            f.write(f'    Points to: {base_record.indirection}\n')
            f.write(f'    Schema: {base_record.schema_encoding}\n')
            f.write(f'  Retrieved Values: {result}\n')
            f.write(f'  Matches Expected: {correct}\n')
            
            # Show version chain for successful case
            current = base_record
            chain_depth = 0
            f.write('  Version Chain:\n')
            while current and current.indirection and current.indirection != current.rid and chain_depth < 5:
                next_record = query.table.get_record(current.indirection)
                if next_record:
                    f.write(f'    Tail {chain_depth}:\n')
                    f.write(f'      RID: {next_record.rid}\n')
                    f.write(f'      Values: {next_record.columns}\n')
                    f.write(f'      Points to: {next_record.indirection}\n')
                    f.write(f'      Schema: {next_record.schema_encoding}\n')
                current = next_record
                chain_depth += 1
            f.write('\n')
    f.write(f'Version -1 Score: {score} / {len(keys)}\n')

    # Version -2 Testing
    v2_score = len(keys)
    f.write("\nChecking Version -2:\n")
    for key in keys:
        correct = records[key]
        query = Query(grades_table)
        
        # Get base record info
        base_rid = query.table.index.locate(0, key)
        base_record = query.table.get_record(base_rid)
        
        result = query.select_version(key, 0, [1, 1, 1, 1, 1], -2)[0].columns
        if correct != result:
            f.write(f'\nERROR on key {key}:\n')
            f.write(f'  Base Record:\n')
            f.write(f'    RID: {base_record.rid}\n')
            f.write(f'    Current Values: {base_record.columns}\n')
            f.write(f'    Points to: {base_record.indirection}\n')
            f.write(f'    Schema: {next_record.schema_encoding}\n')
            f.write(f'  Got     : {result}\n')
            f.write(f'  Expected: {correct}\n')
            v2_score -= 1
        else:
            f.write(f'\nSUCCESS on key {key}:\n')
            f.write(f'  Base Record:\n')
            f.write(f'    RID: {base_record.rid}\n')
            f.write(f'    Current Values: {base_record.columns}\n')
            f.write(f'    Points to: {base_record.indirection}\n')
            f.write(f'    Schema: {base_record.schema_encoding}\n')
            f.write(f'  Retrieved Values: {result}\n')
            f.write(f'  Matches Expected: {correct}\n')
            
            # Show version chain for successful case
            current = base_record
            chain_depth = 0
            f.write('  Version Chain:\n')
            while current and current.indirection and current.indirection != current.rid and chain_depth < 5:
                next_record = query.table.get_record(current.indirection)
                if next_record:
                    f.write(f'    Tail {chain_depth}:\n')
                    f.write(f'      RID: {next_record.rid}\n')
                    f.write(f'      Values: {next_record.columns}\n')
                    f.write(f'      Points to: {next_record.indirection}\n')
                    f.write(f'      Schema: {next_record.schema_encoding}\n')
                current = next_record
                chain_depth += 1
            f.write('\n')
    f.write(f'Version -2 Score: {v2_score} / {len(keys)}\n')
    if score != v2_score:
        f.write('Failure: Version -1 and Version -2 scores must be same\n')

    # Version 0 Testing
    score = len(keys)
    f.write("\nChecking Version 0:\n")
    for key in keys:
        correct = updated_records[key]
        query = Query(grades_table)
        
        # Get base record info
        base_rid = query.table.index.locate(0, key)
        base_record = query.table.get_record(base_rid)
        
        result = query.select_version(key, 0, [1, 1, 1, 1, 1], 0)[0].columns
        if correct != result:
            f.write(f'\nERROR on key {key}:\n')
            f.write(f'  Base Record:\n')
            f.write(f'    RID: {base_record.rid}\n')
            f.write(f'    Current Values: {base_record.columns}\n')
            f.write(f'    Points to: {base_record.indirection}\n')
            f.write(f'      Schema: {next_record.schema_encoding}\n')
            f.write(f'  Got     : {result}\n')
            f.write(f'  Expected: {correct}\n')
            score -= 1
        else:
            f.write(f'\nSUCCESS on key {key}:\n')
            f.write(f'  Base Record:\n')
            f.write(f'    RID: {base_record.rid}\n')
            f.write(f'    Current Values: {base_record.columns}\n')
            f.write(f'    Points to: {base_record.indirection}\n')
            f.write(f'    Schema: {base_record.schema_encoding}\n')
            f.write(f'  Retrieved Values: {result}\n')
            f.write(f'  Matches Expected: {correct}\n')
            
            # Show version chain for successful case
            current = base_record
            chain_depth = 0
            f.write('  Version Chain:\n')
            while current and current.indirection and current.indirection != current.rid and chain_depth < 5:
                next_record = query.table.get_record(current.indirection)
                if next_record:
                    f.write(f'    Tail {chain_depth}:\n')
                    f.write(f'      RID: {next_record.rid}\n')
                    f.write(f'      Values: {next_record.columns}\n')
                    f.write(f'      Points to: {next_record.indirection}\n')
                    f.write(f'      Schema: {next_record.schema_encoding}\n')
                current = next_record
                chain_depth += 1
            f.write('\n')
    f.write(f'Version 0 Score: {score} / {len(keys)}\n')

    # Write summary
    f.write("\n=== FINAL SUMMARY ===\n")
    f.write(f"Version -1: {score}/{len(keys)} correct\n")
    f.write(f"Version -2: {v2_score}/{len(keys)} correct\n")
    f.write(f"Version 0: {score}/{len(keys)} correct\n")

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