path_user = '/Users/wildanfk/Documents/other_workspace/sse/sse_data_sample/samples_example/user.tsv'
path_product = "/Users/wildanfk/Documents/other_workspace/sse/sse_data_sample/samples_example/product.tsv"

from TopProduct import TopProduct
import copy

# Formula score
from datetime import datetime, timezone
func_user_data = lambda x : tuple([int(x[0]),int(x[1]),float(x[2]),int(x[3])]) # uid, pid, score, timestamps
func_sc_user_pref = lambda sc_user_product, timestamps : sc_user_product * pow(0.95, (datetime.now(timezone.utc) - datetime.fromtimestamp(timestamps, timezone.utc)).days)
func_sc_rec_product = lambda sc_user_pref, sc_product : (sc_user_pref * sc_product) + sc_product

def initialize(path_user, path_product):
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
    for dp in data_process:
        print("User : %s" %(dp))
        print(data_process[dp].getTop(5))


initialize(path_user, path_product)
