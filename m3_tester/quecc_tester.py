import sys
sys.path.append(sys.path[0] + "/..")
from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
import threading
import os
os.system("clear")
if (os.path.isdir('ECS165')):
    os.system("rm -rf ECS165")

db = Database()
db.open('ECS165')
grades_table = db.create_table('Grades', 5, 0)

keys = []
records = {}
num_threads = 2

grades_table.init_priority_queues(num_threads)

for i in range(8):
    key = 100 + i
    keys.append(key)
    record = [key, 0, 0, 0, 0]
    records[key] = record
    q = Query(grades_table)
    q.insert(*records[key])

# 2 transaction workers, 2 transactions / worker, 2 queries / transaction
transaction_workers = []
for i in range(num_threads):
    transaction_workers.append(TransactionWorker([], grades_table))

k = 0
for i in range(2):
    for l in range(2):
        transaction = Transaction(i % num_threads)
        for j in range(2):
            key = keys[k]
            q = Query(grades_table)
            transaction.add_query(q.select, key, 0, [1, 1, 1, 1, 1])
            k = k + 1
        transaction_workers[i % num_threads].add_transaction(transaction)

threads = []
for transaction_worker in transaction_workers:
    threads.append(threading.Thread(target = transaction_worker.run, args = ()))

for i, thread in enumerate(threads):
    print('Thread', i, 'started')
    thread.start()

for i, thread in enumerate(threads):
    thread.join()
    print('Thread', i, 'finished')

'''
Desired Output (order may be different but every one should appear exactly once):
100, 0, 0, 0, 0
101, 0, 0, 0, 0
102, 0, 0, 0, 0
103, 0, 0, 0, 0
104, 0, 0, 0, 0
105, 0, 0, 0, 0
106, 0, 0, 0, 0
107, 0, 0, 0, 0
'''

