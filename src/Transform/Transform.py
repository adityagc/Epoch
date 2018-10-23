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
	return q, fc, spark


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
	#fillna if needed
	return all_returns

#Summarizing correlation coefficients:
def get_correlations(stocklist, returns):
	for stock in stocks:
		current = [stock]
		excluding = list(filter(lambda stock: stock  not in current, stocks))
		corr = all_returns.summarize(summarizers.correlation(stock, other = excluding))
		query = "{" + stock + "::x}"
		print(query)
		q.sync(query,corr.toPandas())

#Push to kdb
def push_to_kdb(rdd, qcontext, stockname):
	#query = str(stockname[:-4 or None]) + "::" + rdd.toPandas()
	#print(query)
	#qcontext.sync('{y :: x}', str(stockname[:-4 or None]), rdd.toPandas())
	pdf = rdd.toPandas()
	query = "{" + str(stockname[:-4 or None]) + "::x}"
	qcontext.sync(query,pdf)

def push_raw_table(qcon, sparkcontext, flintcon, bucketname, stocklist):
	for stock in stocklist:
		stock_rdd = sparkcontext.read.option('header', True).option('inferSchema', True).csv(bucketname + stock).withColumnRenamed('date', 'time')
		ts_rdd = flintcon.read.dataframe(stock_rdd)
		print(ts_rdd.show())
		#close_rdd = ts_rdd['close']
		push_to_kdb(ts_rdd, qcon, stock)

def get_returns(rdd,  stockname):
	returnsrdd = rdd.withColumn('open', 100 * (rdd['close']-rdd['open']) / rdd['open']).select('time', 'open')
	return returnsrdd

def push_returns(qcon, sparkcontext, flintcon, bucketname, stocklist):
	for stock in stocklist[0:4]:
		stock_rdd = sparkcontext.read.option('header', True).option('inferSchema', True).csv(bucketname + stock).withColumnRenamed('date', 'time')
		ts_rdd = flintcon.read.dataframe(stock_rdd)
		returns_rdd = get_returns(ts_rdd, stock)
		print(returns_rdd.show())
		push_to_kdb(returns_rdd, qcon, "a_" + stock)
