import ts.flint
from ts.flint import FlintContext
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import SparkSession
from datetime import date
from ts.flint import summarizers
import psycopg2
from qpython import qconnection
import getpass
import pandas as pd
from inspect import signature
from qpython import qconnection

#Global definitions
qhost = '10.0.0.10'
qport = 5001
bucket_name = 's3a://insighttmpbucket1/'
index_name = bucket_name + 'index.txt'

#Getting list of all stocks:
def get_stock_list(index):
    df = pd.read_csv(index, names=["tickers"])
    return df["tickers"].tolist()


#Creating Spark, SQL and Flint contexts:
def connect(q_host, q_port):
	spark = SparkSession.builder.appName("ts").getOrCreate()
	sqlContext = SQLContext(spark)
	fc = FlintContext(sqlContext)
	q = qconnection.QConnection(host = q_host, port = q_port, pandas = True)
	q.open()
	return q, fc


#Creating reference table:
def get_reference_table(bucket):
	stock = stocks[0]
	all_returns = spark.read.option('header', True).option('inferSchema', True).csv(folder_name + stock + '.csv'). \
    withColumnRenamed('date', 'time').withColumnRenamed('1. open', 'open').withColumnRenamed('4. close', 'close'). \
    withColumnRenamed('2. high', 'high').withColumnRenamed('3. low', 'low').withColumnRenamed('5. volume', 'volume')
	all_returns = flintContext.read.dataframe(all_returns)
	all_returns = all_returns.select('time')
	return all_returns

#Iterating through all stocks:
def merge_rdds(stocklist, reference_table):
	for stock in stocks:
		flint_stock = spark.read.option('header', True).option('inferSchema', True).csv(folder_name + stock + '.csv'). \
		withColumnRenamed('date', 'time').withColumnRenamed('1. open', 'open').withColumnRenamed('4. close', 'close'). \
		withColumnRenamed('2. high', 'high').withColumnRenamed('3. low', 'low').withColumnRenamed('5. volume', 'volume')
		flint_stock = flintContext.read.dataframe(flint_stock)
		stock_return = flint_stock.withColumn( stock , 100 * (flint_stock['close'] - flint_stock['open']) / flint_stock['open']).select('time', stock)
		all_returns = all_returns.futureLeftJoin(stock_return, key='time', tolerance = '120s')
	return all_returns

#Display table for reference:
all_returns.fillna(0)

#Sync all_returns table:
q.sync('{returns::x}',all_returns.toPandas())

#Summarizing correlation coefficients:
def get_correlations(stocklist, returns):
	for stock in stocks:
		current = [stock]
		excluding = list(filter(lambda stock: stock  not in current, stocks))
		corr = all_returns.summarize(summarizers.correlation(stock, other = excluding))
		query = "{" + stock + "::x}"
		print(query)
		q.sync(query,corr.toPandas())

def push_to_kdb(rdd, qcontext, stockname):
	pandas_df = returns.toPandas()

   	q.sync('{::x}',pdf)

