import os
import time

start_time = time.time()

path_file_age = '/Users/wildanfk/Documents/other_workspace/sse/sse_data_sample/samples/age.txt'

try:
    file_age = open(path_file_age, 'r')
except:
    print("No such file : '%s'" %(path_product))
    exit()

path_file_sorted_age = "%s/sorted_%s" %(os.path.dirname(path_file_age), os.path.basename(path_file_age))
file_sorted_age = open(path_file_sorted_age, 'w')

data_age = {}
error_process = 0
total_process = 0
for a in file_age:
    try:
        a = int(a)
        if(a not in data_age):
            data_age[a] = 0
        data_age[a] += 1
    except ValueError as ve:
        error_process += 1
    total_process += 1


for age in sorted(data_age.keys()):
    for n in range(0, data_age[age]):
        file_sorted_age.write("%s\n" %(age))

file_age.close()
file_sorted_age.close()

print("Total Data       : %s" %(total_process))
print("Total Data Error : %s" %(error_process))
print("Total Time       : %s seconds" % (time.time() - start_time) )

