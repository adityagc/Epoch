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
