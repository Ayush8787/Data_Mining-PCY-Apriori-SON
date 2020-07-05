from pyspark import SparkContext
import sys
import time
from collections import defaultdict

input_file = sys.argv[3]

sc = SparkContext('local[*]', 'task1')
sc.setLogLevel("OFF")
start = time.time()
readed_file = sc.textFile(input_file)
title = readed_file.first()
baskets = readed_file.filter(lambda x: x != title)

case = int(sys.argv[1])
nn = int(sys.argv[2])
output_file = sys.argv[4]


def makePair(x):
    dicc = {}
    k = 1
    support = nn / numberOfPartitions
    baskets_1 = [i[1] for i in list(x)]
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

    while (len(temp) > 0):
        k += 1

        if k == 2:
            temp_1 = []
            for i, j in enumerate(temp):
                for x, y in enumerate(temp):
                    if i < x:
                        temp_1.append(j.union(y))
            temp = []
            for pair in temp_1:
                support_lite = 0
                for basket in baskets_1:
                    if pair.issubset(basket):
                        support_lite += 1
                if support_lite >= support:
                    temp.append(pair)

            if len(temp) > 0:
                for ii in temp:
                    final_temp.append((tuple(sorted(ii)), 1))

        else:
            temp_2 = []
            for q, j in enumerate(temp):
                for x, y in enumerate(temp):
                    if q < x:
                        temp_3 = sorted(list(j))
                        temp_4 = sorted(list(y))
                        if (temp_3[:k - 2] == temp_4[:k - 2]):
                            to_add = set(j) | set(y)
                            temp_2.append(to_add)
            temp = []

            for pair_1 in temp_2:
                support_lite_1 = 0
                for basket in baskets_1:
                    if pair_1.issubset(basket):
                        support_lite_1 += 1
                if support_lite_1 >= support:
                    temp.append(pair_1)

            if len(temp) > 0:
                for jj in temp:
                    final_temp.append((tuple(sorted(jj)), 1))

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


if case == 1:
    baskets = baskets.map(lambda x: tuple(x.split(','))).distinct().map(lambda x: (x[0], [x[1]])).reduceByKey(
        lambda a, b: a + b).persist()
elif case == 2:
    baskets = baskets.map(lambda x: tuple(x.split(','))).distinct().map(lambda x: (x[1], [x[0]])).reduceByKey(
        lambda a, b: a + b).persist()

numberOfPartitions = baskets.getNumPartitions()

stepone = baskets.mapPartitions(lambda x: makePair(x)).groupByKey().map(lambda x: x[0]).collect()
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

steptwo = baskets.mapPartitions(lambda x: final_touch(x, stepone)).reduceByKey(lambda x, y: x + y).filter(
    lambda x: x[1] >= nn).map(lambda x: x[0]).collect()
output_frequent = defaultdict(list)
for item in steptwo:
    if type(item) == str:
        output_frequent[1].append(item)
    else:
        output_frequent[len(item)].append(item)

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

with open(output_file, 'w') as file1:
    file1.write(file)

print("Duration:", time.time() - start)
