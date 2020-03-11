from lstore.table import Table, Record
from lstore.index import Index

class Transaction:

    """
    # Creates a transaction object.
    """
    def __init__(self):
        self.queries = []
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
            # If the query has failed the transaction should abort\            if result == False:
                return self.abort()
        return self.commit()

    def abort(self):
        #TODO: do roll-back and any other necessary operations
        return False

    def commit(self):
        # TODO: commit to database
        return True

    def read_column(self, query, pagepointer, query_col):
        args = [query.table.name, "Base", SCHEMA_ENCODING_COLUMN, *page_pointer]
        base_schema = int.from_bytes(BufferPool.get_record(*args), byteorder='big')
        args = [query.table.name, "Base", INDIRECTION_COLUMN, *page_pointer]
        base_indirection = BufferPool.get_record(*args)
        if(base_schema & (1<<query_col)) >> query_col == 1:
            return(self.query.table.get_tail(int.from_bytes(base_indirection,byteorder = 'big'),query_col, page_pointer[i][0]))
        else:
            args = [self.table.name, "Base", query_col + NUM_METAS, *page_pointer]
            return int.frome_bytes(BufferPool.get_records(*args), byteorder = "big"))
         #Total record specified by key and columns
    def write(self):
