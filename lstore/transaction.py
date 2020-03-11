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
                if r_w_ops[1]['command_type'] == "select":
                    # locate the priority queue
                    query.table.priority_queues[self.num_queue][r_w_ops[0]].put(r_w_ops[1])

    # read data column from page pointer for specific query column, return specific value of record
    def read_data_column(self, query, page_pointer, query_col, base_tail, meta_data):
        if meta_data == "Meta":
            args = [self.table.name, base_tail, query_col, *page_pointer]
            return int.from_bytes(BufferPool.get_records(*args), byteorder = "big")
        else:
            args = [query.table.name, base_tail, SCHEMA_ENCODING_COLUMN, *page_pointer]
            base_schema = int.from_bytes(BufferPool.get_record(*args), byteorder='big')
            args = [query.table.name, base_tail, INDIRECTION_COLUMN, *page_pointer]
            base_indirection = BufferPool.get_record(*args)
            if (base_schema & (1<<query_col)) >> query_col == 1:
                return(self.query.table.get_tail(int.from_bytes(base_indirection,byteorder = 'big'),query_col, page_pointer[0]))
            else:
                args = [self.table.name, base_tail, query_col + NUM_METAS, *page_pointer]
                return int.from_bytes(BufferPool.get_records(*args), byteorder = "big")

    # write to the specfic record to the tail page
    def write(self):
