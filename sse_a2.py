import os
import time
import subprocess
import sys

start_time = time.time()

arg = sys.argv
if(len(arg) != 2):
    print("Please run the scripts with argument e.g : ")
    print("python3 ss_a2.py /home/foo/Documents/age.txt")
    exit()

path_file_age = arg[1]
try:
    file_age = open(path_file_age, 'r')
except:
    print("No such file : '%s'" %(path_file_age))
    exit()

print("\nPlease wait on process sort the data...")
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
file_age.close()
print("Process sort the data successed.")


print("\nPlease wait on process create data sort file...")
path_file_sorted_age = "%s/sorted_%s" %(os.path.dirname(path_file_age), os.path.basename(path_file_age))
total_data = total_process - error_process
max_data_per_file = 1000000

total_part = int(total_data / max_data_per_file) + (1 if(total_data % max_data_per_file > 0) else 0)
format_part = "{:0%sd}" %(len(str(total_part)))
index_part = 0
index_process = 0
index_process_part = 0
path_part_arr = []
for age in sorted(data_age.keys()):
    for n in range(0, data_age[age]):
        if(index_process_part == 0):
            index_part += 1
            path_part = "%s.part%s" %(path_file_sorted_age, format_part.format(index_part))
            file_part = open(path_part, 'w')
            path_part_arr.append(path_part)
            print("Process file %s of %s" %(index_part, total_part))
        
        file_part.write("%s\n" %(age))
        index_process += 1
        index_process_part += 1

        if(index_process_part == max_data_per_file or index_process == total_data):
            index_process_part = 0
            file_part.close()


for i in range(0, len(path_part_arr)):
    print("Build from path %s of %s" %((i+1), total_part))
    if(i+1 == len(path_part_arr)):
        path_temp = path_file_sorted_age
    else:
        path_temp = "%s.temp%s" %(path_file_sorted_age, format_part.format(i+1))

    if(i == 0):
        old_temp = None
        command = "mv %s %s" %(path_part_arr[i], path_temp)
    else:
        old_temp = "%s.temp%s" %(path_file_sorted_age, format_part.format(i))
        command = "cat %s %s > %s" %(old_temp, path_part_arr[i], path_temp)
    subprocess.call(command, shell=True)

    if(old_temp):
        subprocess.call("rm %s" %(old_temp), shell=True)
        subprocess.call("rm %s" %(path_part_arr[i]), shell=True)

print("Create data sort file successed.\n")


print("Total Data Processed : %s" %(total_process))
print("Total Data Error     : %s" %(error_process))
print("Total Time           : %s seconds" % (time.time() - start_time) )

