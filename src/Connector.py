import pandas as pd
import getpass

df = pd.DataFrame({'sym':['abc','def','ghi'],'price':[10.1,10.2,10.3]})
print(getpass.getuser())
#fname = 'hdfs://ec2-52-204-67-201.compute-1.amazonaws.com:9000/user/hdfs/folder/file.csv'
f2 = 'hdfs:///ec2-52-204-67-201.compute-1.amazonaws.com:9000/usr/local/hadoop/hadoop_data/hdfs/namenode'
df.to_parquet(f2)
from qpython import qconnection
with qconnection.QConnection(host = '10.0.0.10', port = 5001, pandas = True) as q:
    q.sync('{table1::x}',df)
