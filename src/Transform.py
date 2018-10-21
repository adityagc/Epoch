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


#Creating Spark, SQL and Flint contexts:
spark = SparkSession.builder.appName("ts").getOrCreate()
sqlContext = SQLContext(spark)
flintContext = FlintContext(sqlContext)
q = qconnection.QConnection(host = '10.0.0.10', port = 5001, pandas = True)
q.open()

#Reading dataframes
#Automate this using boto
date = '10-5-2018'
#Reading files manually:
stocks = ['ABT', 'AOS', 'ATVI', 'ABBV']
bucket_name = 's3a://insightde2018bucket/'
folder_name = bucket_name + date + '/'
#Creating reference table:
stock = stocks[0]
all_returns = spark.read.option('header', True).option('inferSchema', True).csv(folder_name + stock + '.csv'). \
    withColumnRenamed('date', 'time').withColumnRenamed('1. open', 'open').withColumnRenamed('4. close', 'close'). \
    withColumnRenamed('2. high', 'high').withColumnRenamed('3. low', 'low').withColumnRenamed('5. volume', 'volume')
all_returns = flintContext.read.dataframe(all_returns)
all_returns = all_returns.select('time')
print(all_returns.show())

#Iterating through all stocks:
for stock in stocks:
	flint_stock = spark.read.option('header', True).option('inferSchema', True).csv(folder_name + stock + '.csv'). \
	withColumnRenamed('date', 'time').withColumnRenamed('1. open', 'open').withColumnRenamed('4. close', 'close'). \
	withColumnRenamed('2. high', 'high').withColumnRenamed('3. low', 'low').withColumnRenamed('5. volume', 'volume')
	flint_stock = flintContext.read.dataframe(flint_stock)
	stock_return = flint_stock.withColumn( stock , 100 * (flint_stock['close'] - flint_stock['open']) / flint_stock['open']).select('time', stock)
	all_returns = all_returns.futureLeftJoin(stock_return, key='time', tolerance = '120s')

#Display table for reference:
print(all_returns.show())
all_returns.fillna(0)

#Sync all_returns table:
q.sync('{returns::x}',all_returns.toPandas())

#Summarizing correlation coefficients:
for stock in stocks:
	current = [stock]
	excluding = list(filter(lambda stock: stock  not in current, stocks))
	corr = all_returns.summarize(summarizers.correlation(stock, other = excluding))
	query = "{" + stock + "::x}"
	print(query)
	q.sync(query,corr.toPandas())


