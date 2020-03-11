from lstore.table import Table, Record
from lstore.index import Index

class TransactionWorker:
    """
    # Adopted algorithm for transaction worker and transactions
    # From the tester, each transaction worker holds muliple transactions
    # At the planning stages,
    # Each transaction holds the plan for its corresponding priority queue
    # Priority queue location will be passd out by indices of transaction workers
    # which corresponds to the specific priority queue
    # At the executon stages,
    # Each transaction worker will load transactions inside priority queue in order
    """

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = transactions
        self.result = 0
        pass

    def add_transaction(self, t):
        self.transactions.append(t)

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    # transaction_worker = TransactionWorker([t])
    """
    # current thread is getting ready to execute operations inside one transaction
    # def execution_stage(self):

    def query_select(self, query, page_pointers):
        for page_pointer in page_pointers:
            self.read_base_column(query, page_pointer, )

    def run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
