from pyspark import SparkContext
import sys
import time
import json
import csv

input_review = sys.argv[1]
input_business = sys.argv[2]

sc = SparkContext('local[*]', '')
sc.setLogLevel("OFF")
start = time.time()
review = sc.textFile(input_review).map(lambda x: json.loads(x)).map(lambda x: (x["business_id"], x["user_id"]))
business = sc.textFile(input_business).map(lambda x: json.loads(x)).filter(lambda x: x["state"] == "NV")
business_new = business.map(lambda x: (x["business_id"], x["state"]))
joined = business_new.join(review).map(lambda x:(x[1][1],x[0])).collect()


#ooutput_file = "C:\\Users\\acp87\Desktop\DM\\this\\user_business.csv"
ooutput_file = sys.argv[3]
with open(ooutput_file, 'w', newline='') as output_file:
    writer = csv.writer(output_file, quoting=csv.QUOTE_NONE)
    writer.writerow(["user_id", "business_id"])
    for row in joined:
        writer.writerow(row)
