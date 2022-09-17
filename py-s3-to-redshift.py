import json
import psycopg2
import os


# Data transfer from S3 to Redshift

def lambda_handler(event, context):
    print("event collected is {}".format(event))
    for record in event['Records']:
        s3_bucket = record['s3']['bucket']['name']
        print("Bucket name is {}".format(s3_bucket))
        s3_key = record['s3']['object']['key']
        print("Bucket key name is {}".format(s3_key))
        from_path = "s3://{}/{}".format(s3_bucket, s3_key)
        print("from path {}".format(from_path))
        dbname = os.getenv('dbname')
        host = os.getenv('host')
        rs_user = os.getenv('user')
        rs_password = os.getenv('password')
        tablename = os.getenv('tablename')
        connection = psycopg2.connect(
            dbname=dbname,
            host=host,
            port='5439',
            user=rs_user,
            password=rs_password
        )
        print('after connection....')
        curs = connection.cursor()
        print('after cursor....')
        xquery = "CSV".format(tablename, from_path)
        print("query is {}".format(xquery))
        print('after querry....')
        curs.execute(xquery)
        connection.commit()
        # print(curs.fetchmany(3))
        print('after execute....')
        curs.close()
        print('after curs close....')
        connection.close()
        print('after connection close....')
        print('wow..executed....')
