
path_user = '/Users/wildanfk/Documents/other_workspace/sse/sse_data_sample/samples/user.tsv'
path_product = "/Users/wildanfk/Documents/other_workspace/sse/sse_data_sample/samples/product.tsv"

# Store the product score into memory
file_product = open(path_product, 'r')
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

for d in data_product:
    print(d)


