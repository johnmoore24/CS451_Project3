from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker

from random import choice, randint, sample, seed

def log_to_file(message, level="INFO"):
    """Helper function to log messages to file with timestamp"""
    from datetime import datetime
    timestamp = datetime.now().strftime('%H:%M:%S.%f')[:-3]
    with open('pt3_testoutput.txt', 'a') as f:
        f.write(f"[{timestamp}] [{level}] {message}\n")
        f.flush()

# Clear the file at start
with open('pt3_testoutput.txt', 'w') as f:
    f.write("=== TEST START ===\n")

log_to_file("Starting test setup...", "INFO")
db = Database()
db.open('./CS451')

log_to_file(f"Creating grades table...", "INFO")
grades_table = db.create_table('Grades', 5, 0)
log_to_file(f"Table created: {grades_table}", "INFO")
log_to_file(f"Table attributes: {dir(grades_table)}", "DEBUG")

# create a query class for the grades table
query = Query(grades_table)
log_to_file(f"Query object created: {query}", "INFO")

log_to_file("\nTesting single record insertion...", "INFO")
test_record = [92106429, 1, 2, 3, 4]
try:
    # Test direct insertion first
    log_to_file("Testing direct insert...", "INFO")
    log_to_file(f"Test record: {test_record}", "DEBUG")
    result = query.insert(*test_record)
    log_to_file(f"Direct insert result: {result}", "INFO")
    
    # Test transaction-based insertion
    log_to_file("\nTesting transaction-based insert...", "INFO")
    test_transaction = Transaction()
    log_to_file(f"Transaction created: {test_transaction.transaction_id}", "DEBUG")
    test_transaction.add_query(query.insert, grades_table, *test_record)
    log_to_file(f"Query added to transaction", "DEBUG")
    test_transaction.begin()
    log_to_file(f"Transaction begun", "DEBUG")
    success = test_transaction.run()
    log_to_file(f"Transaction success: {success}", "INFO")
    
except Exception as e:
    log_to_file(f"Test insert failed: {str(e)}", "ERROR")
    import traceback
    log_to_file(traceback.format_exc(), "ERROR")

# Continue with main test...
log_to_file("\nStarting main test...", "INFO")

# dictionary for records to test the database: test directory
records = {}

number_of_records = 1000
number_of_transactions = 100
num_threads = 8

# create index on the non primary columns
try:
    grades_table.index.create_index(2)
    grades_table.index.create_index(3)
    grades_table.index.create_index(4)
except Exception as e:
    print('Index API not implemented properly, tests may fail.')

keys = []
records = {}
seed(3562901)

# array of insert transactions
insert_transactions = []

for i in range(number_of_transactions):
    insert_transactions.append(Transaction())

for i in range(0, number_of_records):
    key = 92106429 + i
    keys.append(key)
    records[key] = [key, randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20), randint(i * 20, (i + 1) * 20)]
    t = insert_transactions[i % number_of_transactions]
    t.add_query(query.insert, grades_table, *records[key])

transaction_workers = []
for i in range(num_threads):
    worker = TransactionWorker()
    worker.id = i
    transaction_workers.append(worker)
    log_to_file(f"Created worker {i}", "DEBUG")
    
for i in range(number_of_transactions):
    transaction_workers[i % num_threads].add_transaction(insert_transactions[i])
    log_to_file(f"Added transaction {i} to worker {i % num_threads}", "DEBUG")

log_to_file(f"Starting {num_threads} workers with {number_of_transactions} transactions", "INFO")

# run transaction workers
for i in range(num_threads):
    log_to_file(f"Starting worker {i}", "INFO")
    transaction_workers[i].run()

# wait for workers to finish
for i in range(num_threads):
    transaction_workers[i].join()
    log_to_file(f"Worker {i} finished", "INFO")

# Add verification logging
log_to_file(f"\nVerifying table contents:", "INFO")
log_to_file(f"Total records expected: {number_of_records}", "INFO")
log_to_file(f"Actual table size: {grades_table.num_records}", "INFO")

# Modify the select verification to handle empty results
for key in keys:
    result = query.select(key, 0, [1, 1, 1, 1, 1])
    if not result:
        log_to_file(f"No record found for key {key}")
        continue
    record = result[0]
    error = False
    for i, column in enumerate(record.columns):
        if column != records[key][i]:
            error = True
    if error:
        log_to_file('select error on', key, ':', record, ', correct:', records[key])
    else:
        pass
        # print('select on', key, ':', record)
log_to_file("Select finished")


db.close('./CS451')
