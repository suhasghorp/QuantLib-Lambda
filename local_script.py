import boto3
import time
import csv
import numpy as np
from io import StringIO
import datetime

startTime = datetime.datetime.now()

session = boto3.session.Session(region_name='us-east-1')

client_lambda = session.client('lambda')
response = client_lambda.invoke(
  FunctionName='lambda_calc_exposures',
  InvocationType='Event',
)
s3_client = session.client('s3')
s3_resource = session.resource('s3')

file_count = 0
time.sleep(60)
while file_count < 500:
    filenames = [e['Key'] for p in s3_client.get_paginator("list_objects_v2").paginate(Bucket='calc-exposures-out') for e in p['Contents']]
    file_count = len(filenames)
    time.sleep(5)

exposures = np.zeros(shape=(500, 262), dtype=float)
filenames = [e['Key'] for p in s3_client.get_paginator("list_objects_v2").paginate(Bucket='calc-exposures-out') for e in p['Contents']]
for f in filenames:
    if f == 'simulated-fixings.csv':
        continue
    sim_num = int(f.split('_')[0])
    csvfile = s3_client.get_object(Bucket='calc-exposures-out', Key=f)
    lines = csvfile['Body'].read().decode('utf-8')
    exposures[sim_num, :] = np.genfromtxt(StringIO(lines), delimiter=",")

print('\nTime elasped: ', datetime.datetime.now() - startTime)

# calculate expected positive and negative exposures
positiveExposures = exposures.copy()
positiveExposures[positiveExposures < 0.0] = 0.0
EPE = np.mean(positiveExposures, axis=0)

negativeExposures = exposures.copy()
negativeExposures[negativeExposures > 0.0] = 0.0
ENE = np.mean(negativeExposures, axis=0)