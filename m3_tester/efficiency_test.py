import sys
sys.path.append(sys.path[0] + "/..")
from lstore.db import Database
from lstore.query import Query
from lstore.transaction import Transaction
from lstore.transaction_worker import TransactionWorker
import threading
from random import choice, randint, sample, seed
from time import process_time
import os

seed(3562901)

class Efficiency_Tester:
    def __init__(self):
        if (os.path.isdir('ECS165')):
            os.system("rm -rf ECS165")
        self.db = Database()
        self.db.open('ECS165')
        self.table = self.db.create_table('Grades', 5, 0)
        self.records = {}
        self.keys = []
        self.num_threads = 10
        self.num_records = 5000

    def setup(self):
        for i in range(0, self.num_records):
            key = 92106429 + i
            self.keys.append(key)
            self.records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
            q = Query(self.table)
            q.insert(*self.records[key])

    def thread_run(self, trans_workers):
        threads = []
        for trans_worker in trans_workers:
            threads.append(threading.Thread(target = trans_worker.run, args = ()))
        for i, thread in enumerate(threads):
            # print('Thread', i, 'started')
            thread.start()
        for i, thread in enumerate(threads):
            thread.join()
            # print('Thread', i, 'finished')
        num_committed_transactions = sum(t.result for t in trans_workers)
        print(num_committed_transactions, 'transaction committed.')

    def init_trans_workers(self):
        transaction_workers = []
        for i in range(self.num_threads):
            transaction_workers.append(TransactionWorker([]))
        return transaction_workers

    def low_select(self):
        transaction_workers = self.init_trans_workers()
        k = 0
        for i in range(self.num_threads):
            for j in range(10):
                transaction = Transaction()
                for j in range(self.num_records // self.num_threads // 10):
                    key = self.keys[k]
                    q = Query(self.table)
                    transaction.add_query(q.select, key, 0, [1, 1, 1, 1, 1])
                    k = k + 1
                transaction_workers[i % self.num_threads].add_transaction(transaction)
        self.thread_run(transaction_workers)
    
    def low_update(self):
        pass

    def low_sum(self):
        pass    

    def low_delete(self):
        pass

    def low_run_all(self):
        self.setup()

        select_time_0 = process_time()
        self.low_select()
        select_time_1 = process_time()
        print("Selecting 5k records took:  \t\t\t", select_time_1 - select_time_0)

        # self.low_update()
        # self.low_sum()
        # self.low_delete()
        os.system('rm -rf ECS165')

def main():
    print("\n*** Low Contention Tester ***\n")
    efficiency_tester = Efficiency_Tester()
    efficiency_tester.low_run_all()
    # print("\n*** High Contention Tester ***\n")
    # efficiency_tester = Efficiency_Tester()
    # efficiency_tester.high_run_all()

if __name__ == "__main__":
    os.system("clear")
    main()