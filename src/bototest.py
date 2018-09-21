import boto3
import json

kinesis = boto3.client('kinesis')

""" Create kinesis stream, and wait until it is active. 
Without waiting, you will get errors when putting data into the stream
"""

stream = "myStream"
kinesis = boto3.client('kinesis')
if stream not in [f for f in kinesis.list_streams()['StreamNames']]:
    print 'Creating Kinesis stream %s' %  stream
    kinesis.create_stream(StreamName=stream, ShardCount=1)
else:
    print 'Kinesis stream %s exists' %  stream
while kinesis.describe_stream(StreamName=stream)['StreamDescription']['StreamStatus'] == 'CREATING':
    time.sleep(2)

i = 0
while 1==1:
     kinesis.put_record(StreamName=stream, Data=json.dumps(i), PartitionKey="partitionkey")
     i = i + 1