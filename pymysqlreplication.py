from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import *
import json
import sys
import boto3
import logging
from botocore.exceptions import ClientError
import datetime

client = boto3.client('s3')

MYSQL_SETTINGS = {
    "host": "db-dns",
    "port": 3306,
    "user": "user_name",
    "passwd": "pass"
}

now = datetime.datetime.now()

def sendKinesis(data,hash_id):

  kinesis_client = boto3.client('kinesis',region_name='us-east-2')

  try:
    response = kinesis_client.put_record(StreamName ='kinesisStreamName', Data = data, PartitionKey = hash_id)
    
  except ClientError as e: 
      logging.error(e)  
      return False
  return True    

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.put_object(Body=file_name, Bucket=bucket, Key=object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def main():

   # server_id is your slave identifier, it should be unique.
   # set blocking to True if you want to block and wait for the next event at
   # the end of the stream
   stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                               server_id=3,
                               blocking=True,
                               only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent]) # specify wich event on the table will be listened by binlog events
   for binlogevent in stream:
       event = {"schema": binlogevent.schema, "table": binlogevent.table}
       if (event['table']): # define wich table will be called
           
           for row in binlogevent.rows:

               event = {
               'table':event['table'],
               'origin':'DataBaseName',
               'event':row
               }

               file_binlog = json.dumps(event, default=str)
               
               hash_id = str(event['table']) + '-' + str(MYSQL_SETTINGS["host"]) + '-' + event['table'] + '-' + str(datetime.datetime.now())  
               sendKinesis(file_binlog,hash_id)

   stream.close()

if __name__ == "__main__":
  main()