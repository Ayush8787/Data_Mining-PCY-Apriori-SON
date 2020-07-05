import sys
import time
from collections import defaultdict
from pyspark import SparkContext

input_file = sys.argv[3]


sc = SparkContext('local[*]', 'task1')
sc.setLogLevel("OFF")
start = time.time()
readed_file = sc.textFile(input_file)

title = readed_file.first()
baskets = readed_file.filter(lambda x: x != title)

tt = int(sys.argv[1])
nn = int(sys.argv[2])
output_file = sys.argv[4]


def custom_partition(b_id):
    return hash(b_id)


def makePair(x):
    print("Function call")
    dicc = {}
    k = 1
    baskets_1 = [i[1] for i in list(x)]
    support = float((nn * len(baskets_1)) / bask)
    #support = nn/numofpart                     ------------- optimization- 1
    temp = []
    final_temp = []
    for subbasket in baskets_1:
        for j in subbasket:
            if j not in dicc:
                dicc[j] = 1
            else:
                dicc[j] += 1

    for i, j in dicc.items():
        if j >= support:
            temp.append({i})
            final_temp.append((i, 1))
    print("lenth 1 is completed")
    while (len(temp) > 0):
        k += 1
        if k == 2:
            tttt = time.time()
            temp_1 = []
            for i, j in enumerate(temp):
                for x, y in enumerate(temp):
                    if i < x:
                        temp_1.append(tuple(j.union(y)))
            temp = []
            print(time.time() - tttt, k)
            ff = time.time()
            pool = []
            for ele in baskets_1:
                pool.append(set(ele))
            count1 = {}
            for q in temp_1:
                count1[q] = 0
            for can in temp_1:
                for p in pool:
                    if set(can).issubset(p):
                        count1[can] += 1
                        if count1[can] >= support:
                            temp.append(tuple(can))
                            break           #------------------------------------optimization 2
            print("finding done", time.time() - ff)
            if len(temp) > 0:
                for ii in temp:
                    final_temp.append((tuple(sorted(ii)), 1))
        else:
            # print("in the else part")
            tempu = []
            for i, j in enumerate(temp):
                aa = set(j)
                for kk in range(i + 1, len(temp)):               #-------------------optimization 4
                    bb = set(temp[kk])
                    if len(aa.intersection(bb)) == k - 2:        #---------------------optimization 3
                        col = aa.union(bb)
                        if len(col) == k:
                            tempu.append(tuple(sorted(list(col))))
                        # temp_3 = sorted(list(j))
                        # temp_4 = sorted(list(y))
                        # if (temp_3[:k - 2] == temp_4[:k - 2]):
                        #     to_add = set(j) | set(y)
                        #     temp_2[dd] = to_add
                        #     dd+= 1

            tempu = sorted(list(set(tempu)))
            pool2 = []       #------------------optimization 4
            for ele in baskets_1:
                pool2.append(set(ele))
            count2 = {}
            for qq in tempu:
                count2[qq] = 0
            final_temp1 = []
            for can in tempu:
                for p in pool2:
                    if set(can).issubset(p):
                        count2[can] += 1
                        if count2[can] >= support:
                            final_temp1.append(tuple(can))
                            break
            temp_2 = sorted(list(set(final_temp1)))
            if len(temp_2) > 0:
                for i in temp_2:
                    final_temp.append((tuple(sorted(i)), 1))
            temp = temp_2
    return final_temp


def final_touch(container, candidates):
    counting_dic = defaultdict(int)
    baskets = [i[1] for i in container]
    for i in candidates:
        if type(i) == str:
            for b in baskets:
                if i in b:
                    counting_dic[i] += 1
        else:
            for b in baskets:
                if set(i).issubset(set(b)):
                    counting_dic[i] += 1
    return counting_dic.items()


start_1 = time.time()
baskets = baskets.map(lambda x: tuple(x.split(','))).distinct().map(lambda x: (x[0], [x[1]])).reduceByKey(
    lambda a, b: a + b)
baskets = baskets.filter(lambda x: len(x[1]) > tt).persist()
bask = baskets.count()
print(bask)
numofpart = baskets.getNumPartitions()

stepone = baskets.mapPartitions(lambda x: makePair(x)).groupByKey().map(lambda x: x[0]).collect()

print("--------------------------------step one done--------------------------------------------------",
      time.time() - start_1)

output_candidate = defaultdict(list)
for item in stepone:
    if type(item) == str:
        output_candidate[1].append(item)
    else:
        output_candidate[len(item)].append(item)

file = "Candidates:\n"
for key in output_candidate.keys():
    output_candidate[key].sort()
    if key == 1:
        temp = []
        for i in output_candidate[key]:
            temp.append("('" + i + "')")
        file += ','.join(temp) + "\n\n"
    else:
        final = []
        for j in output_candidate[key]:
            temp1 = []
            for ii in j:
                temp1.append("'" + ii + "'")
            final.append("(" + ', '.join(temp1) + ")")
        file += ','.join(final) + "\n\n"

start_2 = time.time()
print("--------------------------------step two start--------------------------------------------------")
steptwo = baskets.mapPartitions(lambda x: final_touch(x, stepone)).reduceByKey(lambda x, y: x + y).filter(
    lambda x: x[1] >= nn).map(lambda x: x[0]).collect()
output_frequent = defaultdict(list)

for item in steptwo:
    if type(item) == str:
        output_frequent[1].append(item)
    else:
        output_frequent[len(item)].append(item)
print("--------------------------------step two done--------------------------------------------------",
      time.time() - start_2)

file += "Frequent Itemsets:\n"
for key in output_frequent.keys():
    output_frequent[key].sort()
    if key == 1:
        temp = []
        for i in output_frequent[key]:
            temp.append("('" + i + "')")
        file += ','.join(temp) + "\n\n"
    else:
        final = []
        for j in output_frequent[key]:
            temp1 = []
            for ii in j:
                temp1.append("'" + ii + "'")
            final.append("(" + ', '.join(temp1) + ")")
        file += ','.join(final) + "\n\n"

with open(output_file, 'w', encoding='utf8') as file1:
    file1.write(file)

print("Duration:", time.time() - start)
