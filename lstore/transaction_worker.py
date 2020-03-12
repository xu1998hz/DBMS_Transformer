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

    # read data column from page pointer for specific query column, return specific value of record
    def read_data_column(self, query, page_pointer, query_col, base_tail, meta_data):
        if meta_data == "Meta":
            args = [query.table.name, base_tail, query_col, *page_pointer]
            return int.from_bytes(BufferPool.get_records(*args), byteorder = "big"))
        else:
            args = [query.table.name, base_tail, SCHEMA_ENCODING_COLUMN, *page_pointer]
            base_schema = int.from_bytes(BufferPool.get_record(*args), byteorder='big')
            args = [query.table.name, base_tail, INDIRECTION_COLUMN, *page_pointer]
            base_indirection = BufferPool.get_record(*args)
            if (base_schema & (1<<query_col)) >> query_col == 1:
                return(self.query.table.get_tail(int.from_bytes(base_indirection,byteorder = 'big'),query_col, page_pointer[0]))
            else:
                args = [query.table.name, base_tail, query_col + NUM_METAS, *page_pointer]
                return int.from_bytes(BufferPool.get_records(*args), byteorder = "big"))

    # write to one tail record to the tail page
    def write_rec(self, query, page_pointer, base_tail, meta_data, data_cols):
        query.table.mg_rec_update(NUM_METAS+query_col, *page_pointer[0])

        new_rec = meta_data
        new_rec.extend(data_cols)
        query.table.tail_page_write(new_rec, page_pointer[0])

        # update base page indirection and schema encoding
        args = [self.table.name, "Base", INDIRECTION_COLUMN, page_pointer[0][0], page_pointer[0][1]]
        page = BufferPool.get_page(*args)
        page.update(update_record_index, next_tid)

        args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, page_pointer[0][0], page_pointer[0][1]]
        page = BufferPool.get_page(*args)
        page.update(update_record_index, schema_encoding)

        self.table.num_updates += 1
