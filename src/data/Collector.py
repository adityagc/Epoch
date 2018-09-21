from alpha_vantage.timeseries import TimeSeries
import matplotlib.pyplot as plt
import pandas as pd
import boto
import boto.s3
import sys
import csv
import io, os
import time
# print(os.getcwd())
# Getting inforation about industries and companies.
# from boto.s3.key import Key
# df = pd.read_csv('industry-comp.csv')
# symb = "MSFT"

# ts = TimeSeries(key='ALPHA_KEY', output_format='pandas')
# data, meta_data = ts.get_intraday(symbol=symb,interval='1min', outputsize='full')
# close = data['4. close']
#
# close.to_csv(str(symb) +".csv")
print os.environ('ALPHA_KEY')
# for index, row in df.iterrows():
#     symb = row['Symbol']
#     try:
#         ts = TimeSeries(key='ALPHA_KEY', output_format='pandas')
#         data, meta_data = ts.get_intraday(symbol=symb,interval='1min', outputsize='full')
#         close = data['4. close']
#         close.to_csv(str(symb) +".csv")
#     except:
#         time.sleep(5)



# print df.head()


# Extracting time series data using API.


# Writing to S3 bucket
# conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
#         AWS_SECRET_ACCESS_KEY)

# bucket_name = "insightde2018bucket"
# bucket = s3_connection.get_bucket('bucket_name')
# key = boto.s3.key.Key(bucket, 'industry-comp.csv')
# with open('industry-comp.csv') as f:
#     key.send_file(f)
#
# s3_connection = boto.connect_s3()
# bucket = s3_connection.get_bucket('insightde2018bucket')
# key = boto.s3.key.Key(bucket, 'industry-comp.csv')
# bucket.
# with open('industry-comp.csv') as f:
#     key.send_file(f)
# bucket = conn.create_bucket(bucket_name,
#     location=boto.s3.connection.Location.DEFAULT)

# testfile = "replace this with an actual filename"
# print 'Uploading %s to Amazon S3 bucket %s' % \
#    (testfile, bucket_name)
#
# def percent_cb(complete, total):
#     sys.stdout.write('.')
#     sys.stdout.flush()
#
#
# k = Key(bucket)
# k.key = 'my test file'
# k.set_contents_from_filename(testfile,
#     cb=percent_cb, num_cb=10)
