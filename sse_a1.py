from TopProduct import TopProduct
from TopProductDB import DatabaseTopProduct, DatabaseMetadata
from datetime import datetime, timezone
import copy
import os
import time


data_conn_path = []
data_conn = []

def build_path_data_connection(dir_db, total_shard):
    format_shard = "{:0%sd}" %(len(str(total_shard)))
    for i in range(0, total_shard):
        path_db = "%s/top5_shard%s.db" %(dir_db, format_shard.format(i))
        data_conn_path.append(path_db)

def open_data_connection():
    for path in data_conn_path:
        data_conn.append(DatabaseTopProduct(path))

def init_data_connection():
    for db in data_conn:
        db.create_table()

def close_data_connection():
    for db in data_conn:
        db.close()

def remove_path_data_connection():
    for path in data_conn_path:
        if(os.path.exists(path)):
            os.remove(path)

def get_data_connection(uid, total_shard):
    return data_conn[(uid % total_shard)]

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
    def __init__(self):
        self.data_process = {}

    def initialize(self, path_user, path_product, path_metadata_db, total_shard, max_user_data_process):
        start_time = time.time()
        # File Product
        try:
            file_product = open(path_product, 'r')
        except:
            print("No such file : '%s'" %(path_product))
            exit()
        # File User
        try:
            file_user = open(path_user, 'r')
        except:
            print("No such file : '%s'" %(path_user))
            exit()

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
        for fu in file_user:
            try:
                u = func_user_data(fu.strip().split('\t')) 
            except Exception as e:
                u = None
                error_process += 1
            total_process += 1

            # Collect data process
            if(u):
                self.collect_data_process(u, data_product, default_top_product)
                if(len(self.data_process) >= max_user_data_process):
                    # Store at database
                    for uid in self.data_process:
                        self.store_data_process(uid, self.data_process[uid], total_shard)
                    self.data_process = {}

        # Close file
        file_user.close()

        # Store at database
        for uid in self.data_process:
            self.store_data_process(uid, self.data_process[uid], total_shard)
        self.data_process = {}

        # Store at metadata
        store_metadata(path_metadata_db, path_user, path_product, total_shard)

        # Information
        print("Total Data Processed : %s" %(total_process))
        print("Total Data Error     : %s" %(error_process))
        print("Total Time           : %s seconds" % (time.time() - start_time) )


    def collect_data_process(self, user_data, product_data, default_top_product):
        uid = user_data[0]
        pid = user_data[1]
        if(uid not in self.data_process):
            data_json = get_data_connection(uid, total_shard).get_json_product(uid)
            if(data_json):
                print("From Json")
                self.data_process[uid] = TopProduct()
                self.data_process[uid].loadJson(data_json)
            else:
                print("From Default")
                self.data_process[uid] = copy.deepcopy(default_top_product)
        self.data_process[uid].add(pid = pid, score = func_sc_rec_product(func_sc_user_pref(user_data[2], user_data[3]), product_data[pid])) 

    def store_data_process(self, uid, data, total_shard):
        conn = get_data_connection(uid, total_shard)
        conn.insert(uid, data.getJson(),data.getTop(5))



if __name__ == '__main__':
    path_user = '/Users/wildanfk/Documents/other_workspace/sse/sse_data_sample/samples/user.tsv'
    path_product = "/Users/wildanfk/Documents/other_workspace/sse/sse_data_sample/samples/product.tsv"
    
    # Configuration Database and process
    dir_db = './db/'
    path_metadata_db = "%s/metadata.db" % (dir_db)
    total_shard = 10
    max_user_data_process = 1000
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

    # Open the connection
    open_data_connection()
    print("path_user    : %s" %(path_user))
    print("path_product : %s" %(path_product))

    if(is_already_init == False):
        init_data_connection()
        print("Please wait on loading top 5 recomendation products")
        init_data = InitTopProduct()
        init_data.initialize(path_user, path_product, path_metadata_db, total_shard, max_user_data_process)
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
            top5_product = get_data_connection(uid, total_shard).get_top5_product(uid)
            data_json = get_data_connection(uid, total_shard).get_json_product(uid)
            if(top5_product == None):
                print("sorry we couldn't found user %s" %(uid))
            else:
                print("recomended product for user %s are : " %(uid))
                for p in top5_product.split(","):
                    print(p)

    close_data_connection()

