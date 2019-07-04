from io import BytesIO, StringIO
import boto3
import csv
import sys
import time
import hashlib


_bucket = input("Bucket: ")
hsh = hashlib.sha256(_bucket.encode()).hexdigest()[:8]
bucket = f"{_bucket}-{hsh}"

def read():
    athena = boto3.client('athena')
    s3 = boto3.client('s3')
    q = athena.start_query_execution(
            QueryString='SELECT * FROM email ORDER BY unixtime DESC',
            QueryExecutionContext={'Database': 'www'},
            ResultConfiguration={
                "OutputLocation": f"s3://{bucket}"})
    key = q['QueryExecutionId'] + '.csv'
    buf = BytesIO()
    attempts = 5
    num = 0
    dur = 0.1
    sys.stdout.write('Waiting')
    while 1:
        if num > attempts:
            return
        try:
            s3.download_fileobj(bucket, key, buf)
            print()
            break
        except Exception:
            time.sleep(dur)
            sys.stdout.write('.')
            sys.stdout.flush()
            num += 0
    buf.seek(0)
    data = buf.read()
    try:
        data = data.decode()
    except Exception:
        print("Error decoding emails from bytes to str")
        return
    buf_s = StringIO(data)
    c = csv.DictReader(buf_s)
    for entry in c:
        print("Name:", entry['name'])
        print("Email:", entry['email'])
        print("IP:", entry['ip'])
        print("Date:", entry['asctime'])
        print(entry['body'])
        print('-'*80)

read()
