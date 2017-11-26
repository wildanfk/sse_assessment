
path_user = '/Users/wildanfk/Documents/other_workspace/sse/sse_data_sample/samples/user.tsv'
path_product = "/Users/wildanfk/Documents/other_workspace/sse/sse_data_sample/samples/product.tsv"

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
    data_product = list(map(
        lambda x : tuple( (lambda y : [int(y[0]),int(y[1])] ) (x.split('\t')) ), 
        file_product.read().strip().split("\n")
    ))
except:
    print("===== Error =====")
    print("Sorry your format file of 'product_score' was wrong.")
    print("Please use tsv format with all rows contain pid(int) and score(int).")
    print("===== Error =====")

file_product.close()
if(data_product is None):
    exit()


# Formula Score
from datetime import datetime, timezone
func_sc_user_pref = lambda sc_user_product, timestamps : sc_user_product * pow(0.95, (datetime.now(timezone.utc) - datetime.fromtimestamp(timestamps, timezone.utc)).days)
func_sc_rec_product = lambda sc_user_pref, sc_product : sc_user_pref + sc_product


# Process the user data
error_process = 0
for fu in file_user:
    u = fu.strip().split('\t')
    print(u)



file_user.close()
