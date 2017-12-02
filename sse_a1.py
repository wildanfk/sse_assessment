from TopProduct import TopProduct
from TopProductDB import DatabaseTopProduct, DatabaseMetadata
from datetime import datetime, timezone
import copy
import os
import time
import threading
from queue import Queue
import sys


data_conn_path = []

def build_path_data_connection(dir_db, total_shard):
    format_shard = "{:0%sd}" %(len(str(total_shard)))
    for i in range(0, total_shard):
        path_db = "%s/top5_shard%s.db" %(dir_db, format_shard.format(i))
        data_conn_path.append(path_db)

def remove_path_data_connection():
    for path in data_conn_path:
        if(os.path.exists(path)):
            os.remove(path)

def get_path_data_connection(uid, total_shard):
    return data_conn_path[(uid % total_shard)]

def init_data_connection():
    for path in data_conn_path:
        conn = DatabaseTopProduct(path)
        conn.create_table()
        conn.close()


def store_metadata(path_db, path_user_file, path_product_file, shard):
    metadata_db = DatabaseMetadata(path_db)
    metadata_db.create_table()
    metadata_db.insert(path_user_file, path_product_file, shard)
    metadata_db.close()

# Formula score
func_user_data = lambda x : tuple([int(x[0]),int(x[1]),float(x[2]),int(x[3])]) # uid, pid, score, timestamps
func_sc_user_pref = lambda sc_user_product, timestamps : sc_user_product * pow(0.95, (datetime.now(timezone.utc) - datetime.fromtimestamp(timestamps, timezone.utc)).days)
func_sc_rec_product = lambda sc_user_pref, sc_product : (sc_user_pref * sc_product) + sc_product


class InitTopProduct():
    def __init__(self, total_shard, total_worker, max_user_process):
        self.q = Queue()
        self.total_shard = total_shard
        self.max_user_process = max_user_process
        self.total_worker = total_worker
        self.qworker = Queue()

        # Create worker
        for i in range(self.total_worker):
            t = threading.Thread(target=self.worker_process_data)
            t.daemon = True  # thread dies when main thread (only non-daemon thread) exits.
            t.start()

    def initialize(self, path_user, path_product, path_metadata_db):
        start_time = time.time()

        # Open file
        file_user = open(path_user, 'r')
        file_product = open(path_product, 'r')

        # Store the product score into memory
        data_product = None
        try:
            data_product = dict(map(
                lambda x : tuple( (lambda y : [int(y[0]),int(y[1])] ) (x.split('\t')) ),
                file_product.read().strip().split("\n")
            )) # pid, score
        except:
            print("===== Error =====")
            print("Sorry your format file of 'product_score' was wrong.")
            print("Please use tsv format with all rows contain pid(int) and score(int).")
            print("===== Error =====")

        file_product.close()
        if(data_product is None):
            exit()

        # Default top product score
        default_top_product = TopProduct()
        for p in data_product:
            default_top_product.add(pid = p, score = func_sc_rec_product(0, data_product[p]), default=True)


        # Process the user data for find score
        error_process = 0
        total_process = 0
        data_user_process = {}
        for fu in file_user:
            try:
                u = func_user_data(fu.strip().split('\t')) 
                total_process += 1
            except Exception as e:
                u = None
                error_process += 1
                total_process += 1
                continue
            
            # Collect data process
            uid = u[0]
            if(uid not in data_user_process):
                data_user_process[uid] = []
            data_user_process[uid].append(u)

            if(len(data_user_process) >= self.max_user_process):
                print("Process data on : %s rows" %(total_process))
                for dup in data_user_process:
                    self.qworker.put((dup, data_user_process[dup], data_product, default_top_product))
                self.qworker.join()
                data_user_process = {}

        # Close file
        file_user.close()

        print("Process data on : %s rows" %(total_process))
        for dup in data_user_process:
            self.qworker.put((dup, data_user_process[dup], data_product, default_top_product))
        self.qworker.join()
        data_user_process = {}

        # Store at metadata
        store_metadata(path_metadata_db, path_user, path_product, total_shard)

        # Information
        print()
        print("Total Data Processed : %s rows" %(total_process))
        print("Total Data Error     : %s rows" %(error_process))
        print("Total Time           : %s seconds" % (time.time() - start_time) )



    def process_data(self, uid, user_data_arr, product_data_arr, default_top_product):
        # Process data per-user
        # Init user top product
        conn = DatabaseTopProduct(get_path_data_connection(uid, self.total_shard))
        data_json = conn.get_json_product(uid)
        if(data_json):
            user_top_product = TopProduct()
            user_top_product.loadJson(data_json)
        else:
            user_top_product = copy.deepcopy(default_top_product)

        # Scoring for search top product
        for user_data in user_data_arr:
            pid = user_data[1]
            if(pid in product_data_arr):
                user_top_product.add(pid = pid, score = func_sc_rec_product(func_sc_user_pref(user_data[2], user_data[3]), product_data_arr[pid])) 

        # Store to database
        conn.insert(uid, user_top_product.getJson(),user_top_product.getTop(5))
        conn.close()

    def worker_process_data_function(self, param):
        self.process_data(param[0], param[1], param[2], param[3])

    def worker_process_data(self):
        while True:
            param = self.qworker.get()
            self.worker_process_data_function(param)
            self.qworker.task_done()




if __name__ == '__main__':
    arg = sys.argv
    if(len(arg) != 3):
        print("Please run the scripts with argument e.g : ")
        print("python3 ss_a1.py /home/foo/Documents/user_preference_file.tsv /home/foo/Documents/product_score_file.tsv")
        exit()

    path_user = arg[1]
    path_product = arg[2]

    # Check File User
    try:
        file_user = open(path_user, 'r')
        file_user.close()
    except:
        print("No such file user : '%s'" %(path_user))
        exit()
    # Check File Product
    try:
        file_product = open(path_product, 'r')
        file_product.close()
    except:
        print("No such file product : '%s'" %(path_product))
        exit()

    # Info file
    print("path_user    : %s" %(path_user))
    print("path_product : %s" %(path_product))
    
    # Configuration Database and process
    dir_db = './db/'
    path_metadata_db = "%s/metadata.db" % (dir_db)
    total_shard = 10
    max_user_process = 1000
    total_worker = 50
    build_path_data_connection(dir_db, total_shard)

    # Check metadata
    is_already_init = False
    if(not os.path.isdir(dir_db)):
        os.mkdir(dir_db)
    elif(os.path.exists(path_metadata_db)):
        metadata_db = DatabaseMetadata(path_metadata_db)
        is_already_init = metadata_db.isMetadataExists(path_user, path_product, total_shard)
        metadata_db.close()

    # Clean before init
    if(is_already_init == False):
        print("Clean before initialization")
        if(os.path.exists(path_metadata_db)):
            os.remove(path_metadata_db)
        remove_path_data_connection()

    if(is_already_init == False):
        init_data_connection()
        print("Please wait on loading top 5 recomendation products")
        init_data = InitTopProduct(total_shard, total_worker, max_user_process)
        init_data.initialize(path_user, path_product, path_metadata_db)
        print("top 5 recomendation successfully created")
    else:
        print("top 5 recomendation already created")

    uid = None
    while(uid != 'exit'):
        print("\n====================================================")
        print("type 'exit' for exit from application")
        uid = input("Search top 5 recomendation product for user : ")
        if(uid != 'exit'):
            try:
                uid = int(uid)
            except:
                print("'error' please input numeric value")
                continue
            conn = DatabaseTopProduct(get_path_data_connection(uid, total_shard))
            top5_product = conn.get_top5_product(uid)
            if(top5_product == None):
                print("sorry we couldn't found user %s" %(uid))
            else:
                print("recomended product for user %s are : " %(uid))
                for p in top5_product.split(","):
                    print(p)
            conn.close()

