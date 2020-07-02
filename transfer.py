import boto3, os
from dotenv import load_dotenv
load_dotenv()

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

files = []
destination = os.environ['DEST_BUCKET']
source_bucket = os.environ['SOURCE_BUCKET']
get_bucket_contents =  s3_client.list_objects_v2(Bucket=source_bucket)
if 'Contents' not in get_bucket_contents:
  print("Bucket is empty")
for key in get_bucket_contents['Contents']:
  files.append(key['Key'])

while get_bucket_contents['IsTruncated']:
  continuation_key = get_bucket_contents['NextContinuationToken']
  get_bucket_contents = s3_client.list_objects_v2(Bucket=source_bucket, ContinuationToken=continuation_key)
  for key in get_bucket_contents['Contents']:
    files.append(key['Key'])

for obj in files:
  copy_source = {'Bucket': source_bucket,'Key': obj}
  get_encryption = s3_client.get_object(Bucket=source_bucket, Key=obj)
  encryption = None
  if ('ServerSideEncryption' in get_encryption):
    encryption = get_encryption['ServerSideEncryption']
    s3_client.copy_object(CopySource=copy_source, Bucket=destination, Key=obj, ACL='bucket-owner-full-control', ServerSideEncryption=encryption)
  else:
    s3_client.copy_object(CopySource=copy_source, Bucket=destination, Key=obj, ACL='bucket-owner-full-control')

