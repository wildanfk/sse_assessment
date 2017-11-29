from TopProduct import TopProduct
from TopProductDB import DatabaseTopProduct, DatabaseMetadata
import copy
import os

# Formula score
from datetime import datetime, timezone
func_user_data = lambda x : tuple([int(x[0]),int(x[1]),float(x[2]),int(x[3])]) # uid, pid, score, timestamps
func_sc_user_pref = lambda sc_user_product, timestamps : sc_user_product * pow(0.95, (datetime.now(timezone.utc) - datetime.fromtimestamp(timestamps, timezone.utc)).days)
func_sc_rec_product = lambda sc_user_pref, sc_product : (sc_user_pref * sc_product) + sc_product

def initialize(path_user, path_product, path_top5_db, path_metadata_db):
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
        print("No such file : '%s'" %(path_product))
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
    data_process = {}
    for fu in file_user:
        try:
            u = func_user_data(fu.strip().split('\t'))
            uid = u[0]
            pid = u[1]
            if(uid not in data_process):
                data_process[uid] = copy.deepcopy(default_top_product)
            data_process[uid].add(pid = pid, score = func_sc_rec_product(func_sc_user_pref(u[2], u[3]), data_product[pid])) 
        except Exception as e:
            print(e)
            error_process += 1
        total_process += 1

    file_user.close()

    # Store at database
    top5_db = DatabaseTopProduct(path_top5_db)
    top5_db.create_table()
    data = []
    for uid in data_process:
        data.append((uid, data_process[uid].getJson(),data_process[uid].getTop(5)))
    top5_db.insert(data)
    top5_db.close()

    # Store at metadata
    metadata_db = DatabaseMetadata(path_metadata_db)
    metadata_db.create_table()
    metadata_db.insert(path_user, path_product)
    metadata_db.close()


if __name__ == '__main__':
    path_user = '/Users/wildanfk/Documents/other_workspace/sse/sse_data_sample/samples_example/user.tsv'
    path_product = "/Users/wildanfk/Documents/other_workspace/sse/sse_data_sample/samples_example/product.tsv"
    
    dir_db = './db/'
    path_top5_db = "%s/top5.db" % (dir_db)
    path_metadata_db = "%s/metadata.db" % (dir_db)

    is_already_init = False
    if(not os.path.isdir(dir_db)):
        os.mkdir(dir_db)
    elif(os.path.exists(path_metadata_db)):
        metadata_db = DatabaseMetadata(path_metadata_db)
        is_already_init = metadata_db.isMetadataExists(path_user, path_product)
        metadata_db.close()

    print("path_user    : %s" %(path_user))
    print("path_product : %s" %(path_product))

    if(is_already_init == False):
        if(os.path.exists(path_metadata_db)):
            os.remove(path_metadata_db)
        if(os.path.exists(path_top5_db)):
            os.remove(path_top5_db)

        print("Please wait on loading top 5 recomendation products")
        initialize(path_user, path_product, path_top5_db, path_metadata_db)
        print("top 5 recomendation successfully created")
    else:
        print("top 5 recomendation already created")


    top5_db = DatabaseTopProduct(path_top5_db)
    uid = None
    while(uid != 'exit'):
        print("====================================================")
        print("type 'exit' for exit from application")
        uid = input("Search top 5 recomendation product for user : ")
        top5_product = top5_db.get_top5_product(uid)
        if(top5_product == None):
            print("sorry we couldn't found user %s" %(uid))
        else:
            print("recomended product for user %s are : " %(uid))
            for p in top5_product.split(","):
                print(p)

    top5_db.close()

