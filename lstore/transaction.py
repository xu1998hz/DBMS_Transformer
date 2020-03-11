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

    def read_column(self, pagepointer):
        for i in range(len(page_pointer)):
            args = [self.table.name, "Base", SCHEMA_ENCODING_COLUMN, *page_pointer[i]]
            base_schema = int.from_bytes(BufferPool.get_record(*args), byteorder='big')
            args = [self.table.name, "Base", INDIRECTION_COLUMN, *page_pointer[i]]
            base_indirection = BufferPool.get_record(*args)

             #Total record specified by key and columns
            res = []
            for query_col, val in enumerate(query_columns):
                # column is not selected
                if val != 1:
                    res.append(None)
                    continue
                if (base_schema & (1<<query_col))>>query_col == 1:
                    res.append(self.table.get_tail(int.from_bytes(base_indirection,byteorder = 'big'),query_col, page_pointer[i][0]))
                else:
                    args = [self.table.name, "Base", query_col + NUM_METAS, *page_pointer[i]]
                    res.append(int.from_bytes(BufferPool.get_record(*args), byteorder="big"))

            # construct the record with rid, primary key, columns
            args = [self.table.name, "Base", RID_COLUMN, *page_pointer[i]]
            rid = BufferPool.get_record(*args)
            args = [self.table.name, "Base", NUM_METAS + column, *page_pointer[i]]
            # or non_prim _key
            prim_key = BufferPool.get_record(*args)
            record = Record(rid, prim_key, res)
            records.append(record)

    def write(self):
