from lstore.table import Table
from lstore.buffer_pool import BufferPool
from lstore.config import *
import os
import time
import pickle
import signal
import sys

sys.setrecursionlimit(RECURSION_LIMIT)

# TODO: Write & Reaed latest_tail of each table to disk

def read_table(path):
    f = open(path, "rb")
    table = pickle.load(f)
    f.close()

    return table


def write_table(path, table):
    f = open(path, 'wb')
    pickle.dump(table, f)
    f.close()


class Database():

    def __init__(self):
        self.tables = []

    def open(self, path):
        print("BufferPool Path @ {}".format(path))
        if not os.path.exists(path):
            os.makedirs(path)

        BufferPool.initial_path(path)

        name2idx= {}
        # Restore Existed Table on Disk
        tables = [name for name in os.listdir(path) if os.path.isdir(os.path.join(path, name))]
        for t_name in tables:
            t_path = os.path.join(path, t_name, 'table.pkl')
            old_table = read_table(t_path)
            name2idx[t_name] = len(self.tables)
            self.tables.append(old_table)

        # Restore Page Directory to BufferPool
        fname = os.path.join(path, "page_directory.txt")
        # Create page_directory.txt if not exist
        if not os.path.exists(fname):
            f = open(fname, "w+")
            f.close()
        f = open(fname, "r")
        lines = f.readlines()
        for line in lines:
            t_name, base_tail, column_id, page_range_id, page_id = line.rstrip('\n').split(',')
            uid = (t_name, base_tail, int(column_id), int(page_range_id), int(page_id))
            BufferPool.add_page(uid)
        f.close()

        # Restore tps to BufferPool
        fname = os.path.join(path, "tps.pkl")
        # Create page_directory.txt if not exist
        if not os.path.exists(fname):
            f = open(fname, "w+")
            f.close()
        else:
            f = open(fname, "rb")
            old_tps = pickle.load(f)
            f.close()
            BufferPool.copy_tps(old_tps)

        # Restore latest_tail to BufferPool
        fname = os.path.join(path, "latest_tail.pkl")
        # Create page_directory.txt if not exist
        if not os.path.exists(fname):
            f = open(fname, "w+")
            f.close()
        else:
            f = open(fname, "rb")
            latest_tail = pickle.load(f)
            f.close()
            BufferPool.copy_latest_tail(latest_tail)


    def close(self):
        s_time = time.time()
        BufferPool.close()
        print("Closing BufferPool took: {}".format(time.time() - s_time))


        s_time = time.time()
        # Write Table Config file
        for table in self.tables:
            t_name = table.name
            # os.kill(table.merge_pid, signal.SIGSTOP)
            table.merge_pid = None
            t_path = os.path.join(BufferPool.path, t_name, "table.pkl")
            write_table(t_path, table)
        print("Updating table.txt: {}".format(time.time() - s_time))


        s_time = time.time()
        # Write Page Directory Config file
        all_uids = BufferPool.page_directories.keys()
        f = open(os.path.join(BufferPool.path, "page_directory.txt"), "w")
        for uid in all_uids:
            t_name, base_tail, column_id, page_range_id, page_id = uid
            my_list = [t_name, base_tail, str(column_id), str(page_range_id), str(page_id)]
            line = ",".join(my_list) + "\n"
            f.write(line)
        f.close()
        print("Updating page_directory.txt: {}".format(time.time() - s_time))

        s_time = time.time()
        # Write Tps Config file
        f = open(os.path.join(BufferPool.path, "tps.pkl"), "wb")
        pickle.dump(BufferPool.tps, f)
        f.close()
        print("Updating tps.pkl: {}".format(time.time() - s_time))

        s_time = time.time()
        # Write latest_tail Config file
        f = open(os.path.join(BufferPool.path, "latest_tail.pkl"), "wb")
        pickle.dump(BufferPool.latest_tail, f)
        f.close()
        print("Updating latest_tail.pkl: {}".format(time.time() - s_time))


    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        table = Table(name, num_columns, key)
        BufferPool.init_latest_tail(name)
        BufferPool.init_tps(name)

        # create a new table in database
        self.tables.append(table)
        # table.mergeProcessController()
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        if name in self.tables:
            self.tables.remove(name)
        else:
            pass

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        for table in self.tables:
            if (table.name == name):
                # table.mergeProcessController()
                return table
