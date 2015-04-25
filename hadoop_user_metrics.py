from collections import OrderedDict
import csv
import json
import sys
import urllib2
import base64
from datetime import datetime, timedelta, date, time


cur_time = (datetime.now() - timedelta(days=1))
print cur_time
to_time = cur_time.replace(hour=23, minute=59, second=59, microsecond=0).isoformat()
print to_time

# Get previous day's data
dt = str(cur_time.date())
yes_time = (datetime.now() - timedelta(days=1))
from_time = yes_time.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
print from_time

# Get 100 records
limiter = 1000

# pass argument to get specific metrics
metric = sys.argv[1]

# Insert your Cloudera Manager Endpoints
if metric == "applications":
    url = '<Cloudera Manager Endpoints>/services/YARN-1/yarnApplications?from={0}&to={1}&limit={2}&filter=''executing=false'''.format(
        from_time, to_time, limiter)
    print "YARN " + url
elif metric == 'queries':
    url = '<Cloudera Manager Endpoints>/services/IMPALA-1/impalaQueries?from={0}&to={1}&limit={2}&filter=''executing=false'''.format(
        from_time, to_time, limiter)
    print "IMPALA " + url

elif metric == 'storage':
    url = '<Cloudera Manager Endpoints>/services/reports/hdfsUsageReport?from={0}&to={1}&limit={2}'.format(
        from_time, to_time, limiter)
    print "HDFS STORAGE " + url

elif metric == 'items':
    url = '<Cloudera Manager Endpoints>/api/v7/audits?query=startTime={0}&endTime={1}'.format(from_time, to_time,
                                                                                              limiter)
    print "AUDIT " + url


def encodeUserData(user, password):
    return "Basic " + (user + ":" + password).encode("base64").rstrip()

# Authenticate
req = urllib2.Request(url)
req.add_header('Accept', 'application/json')
req.add_header("Content-type", "application/x-www-form-urlencoded")
req.add_header('Authorization', 'Basic fsffsfds=')
res = urllib2.urlopen(req)
infile = res.read()
outfile = open("output_" + metric + "_" + dt + ".csv", "w")
writer = csv.writer(outfile, delimiter=",")
data = json.loads(infile, object_pairs_hook=OrderedDict)
# Recursively flatten JSON
def flatten(structure, key="", path="", flattened=None):
    if flattened is None:
        flattened = OrderedDict()
    if type(structure) not in (OrderedDict, list):
        flattened[((path + "_") if path else "") + key] = structure
    elif isinstance(structure, list):
        for i, item in enumerate(structure):
            flatten(item, "", path + "_" + key, flattened)
    else:
        for new_key, value in structure.items():
            flatten(value, new_key, path + "_" + key, flattened)
    return flattened

# Write fields
fields = []
for result in data[metric]:
    flattened = flatten(data[metric][0])
    for k, v in flattened.iteritems():
        if k not in fields:
            fields.append(k)
writer.writerow(fields)

# Write values to csv
for result in data[metric]:
    flattened = flatten(result)
    row = []
    for field in fields:
        if field in flattened.iterkeys():
            row.append(flattened[field])
        else:
            row.append("")
    writer.writerow(row)
