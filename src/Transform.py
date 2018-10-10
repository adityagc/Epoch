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
	#print(stock_return.show())

#Display table for reference:
print(all_returns.show())
all_returns.fillna(0)
#Summarizing correlation coefficients:
corr = all_returns.summarize(summarizers.correlation(stocks[0], other = stocks[1:]))

#jdbcHostname = "timeseries.c07kxyw4xlcx.us-east-1.rds.amazonaws.com"
#jdbcDatabase = ""
#jdbcPort = 5432
#jdbcUrl = "jdbc:mysql://{0}:{1}/{2}?".format(jdbcHostname, jdbcPort, jdbcDatabase)
'''
curs = conn.cursor()
curs.execute('create table CUSTOMER'\
              '("CUST_ID" INTEGER not null,'\
              ' "NAME" VARCHAR not null,'\
              ' primary key ("CUST_ID"))'\
             )
curs.execute("insert into CUSTOMER values (1, 'John')")
curs.execute("select * from CUSTOMER")
print(curs.fetchall())
curs.close()

conn.close()
'''
#with qconnection.QConnection(host = '10.0.0.10', port = 5001, pandas = True) as q:
#    q.sync('{table1::x}',corr)

#filename = 'hdfs:///user/{}/filename.parquet'.format(getpass.getuser())
#filename1 = '/home/ubuntu/' + date + '.parquet'


#fname = 'hdfs://ec2-52-204-67-201.compute-1.amazonaws.com:9000/user/hdfs/folder/file.csv'
f2 = '/usr/local/hadoop/hadoop_data/hdfs/namenode/tempo.parquet'
filename = '/user/{}/filename.parquet'.format(getpass.getuser())

# self.data_stats.toDF(schema=["time"].append(stocks).write.format("org.apache.spark.sql.cassandra").mode("append").options(table=self.cassandra_table, keyspace=self.cassandra_keyspace).save()

#myschema = ["time"].append(stocks)
#ar = spark.createDataFrame(all_returns, schema = myschema)
#df = pd.DataFrame(all_returns)
#df.to_csv("fucking.csv")
#all_returns.write.parquet(f2)
print(dir(all_returns))
pdf = all_returns.toPandas()
print(pdf.head())


with qconnection.QConnection(host = '10.0.0.10', port = 5001, pandas = True) as q:
    q.sync('{table2::x}',pdf)

