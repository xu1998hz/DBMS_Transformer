from lstore.table import Table, Record
from lstore.buffer_pool import BufferPool
from lstore.index import Index
from lstore.config import *

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self, num_queue):
        self.queries = []
        self.num_queue = num_queue
        pass

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        self.queries.append((query, args))

    # If you choose to implement this differently this method must still return True if transaction commits or False on abort
    def run(self):
        for query, args in self.queries:
            result = query(*args)
            # If the query has failed the transaction should abort\
            if result == False:
                return self.abort()
        return self.commit()

    # current thread is getting ready to plan operations inside one transaction
    def planning_stage(self):
        for query, args in self.queries:
            r_w_ops_list = query(*args)
            for r_w_ops in r_w_ops_list:
                # locate the priority queue
                query.table.priority_queues[self.num_queue][r_w_ops[0]].put(r_w_ops[1])
