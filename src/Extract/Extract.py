from alpha_vantage.timeseries import TimeSeries
import config
import pandas as pd
import sys
import csv
import io, os
import time
import s3fs

#Get tickers
#mybucket = 's3a://insightde2018bucket/'
#industry_file_path = mybucket+'industry_comp.csv'

#Read as pandas dataframe
#df = pd.read_csv(industry_file_path)
df = pd.read_csv('./src/industry-comp.csv')

for index, row in df.iterrows():
	symb = row['Symbol']
	ts = TimeSeries(key=symb, datatype='csv')
	data, meta_data = ts.get_intraday(symbol=symb,interval='1min', datatype='csv', outputsize='full')

'''
# Writing to S3 bucket
conn = boto.connect_s3(AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY)

def percent_complete(complete, total):
    sys.stdout.write('.')
    sys.stdout.flush()

bucket_name = "insightde2018bucket"
bucket = s3_connection.get_bucket('bucket_name')
key = boto.s3.key.Key(bucket, 'industry-comp.csv')
with open('industry-comp.csv') as f:
key.send_file(f)

for index, row in df.iterrows():
    symb = row['Symbol']
    filepath = os.path.join(dir_name, str(symb) + "." + format)
    close.to_csv( +"_new.csv")
    key.set_contents_from_filename(testfile,
    cb=percent_cb, num_cb=10)
print 'Uploading %s to Amazon S3 bucket %s'
'''
