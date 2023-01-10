'''
MIT License

Copyright (c) [2021] [John Edwin Danson]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''
import gzip
import boto3, io, time, os
from pkg_resources import cleanup_resources
import pandas as pd, zipfile
from io import BytesIO
import datetime
from io import StringIO
from datetime import datetime, timezone
from pytz import timezone
from datetime import datetime
import random

def lambda_handler(event, context):

    region = "us-east-1"

    strDeltFolder = 'tbc'

    s3r = boto3.resource('s3', 'us-east-1')
    
    s3c = boto3.client('s3', 'us-east-1')
    
    lambda1 =  boto3.client('lambda', 'us-east-1')
    
    org = boto3.client('organizations')
    
    aId = boto3.client('sts').get_caller_identity().get('Account')
    

    bucket = os.environ["ENV_ROWS"]
    
    bucketname = os.environ["ENV_AWS_ROWS"]
    
    env_user = os.environ["ENV_USR_EML"]
    
    bucket = s3r.Bucket(bucketname)
    
    bucket.objects.filter(Prefix="//"+ strDeltFolder).delete()
    
    bucket2 = "xxxxxxxx"

    key1 = "F-00001.csv.gz"

    with gzip.open(key1, 'rb') as f:
        file_content = f.read()
    
    pos1 = key1.find("-")
    key2 = key1[pos1:9]
    resultState = False
    
    try:
        root_id    = org.list_roots()['Roots'][0]['Id']
        paginator = org.get_paginator('list_accounts')
        page_iterator = paginator.paginate()
        acctx = ""
        acctz = ""
        epoxy1 = 100000000000
        epoxy = 0
    
        old_account = '123000000000'
        resultState = False
        for page in page_iterator:        
            for acct in page['Accounts']:
                accid = str(acct["Id"])
                acctx = acctx + '\n' + acct["Id"]     
                acctz = acctz + '\n' + acct["Id"]    
                epoxy0 = acct["JoinedTimestamp"]
                epoch = datetime(1970, 1, 1)
                p = '%Y-%m-%d %H:%M:%S'
    
                epoxy2 = str(epoxy0)
                epoxy2 = epoxy2[0:19]
                xx = (int(time.mktime(time.strptime(epoxy2, p)))) 
                if xx <= int(epoxy1):
                    epoxy1 = str(xx)
                    old_account = str(accid)
                else:
                    print("...")
                acctx = acctx + " " + str(xx)
                acctz = acctz
    except:
        acctx = ""
        acctz = ""
        epoxy1 = 100000000000
        epoxy = 0     
   
    objects = s3c.list_objects(Bucket=bucketname)
    
    for o in objects["Contents"]:
    
        if resultState == True:
            break
    
        if key2 in str(o["Key"]):
            oLast = o["LastModified"]
            key3 = str(o["Key"])

            oLast = str(oLast)[0:10]
            todayStr = str(todayStr)[0:10]
          
            zip_obj = s3r.Object(bucket_name=bucketname, key=str(key3))
            buffer = BytesIO(zip_obj.get()["Body"].read())
            ll1 = aId[:4]
            ll1 = ll1[::-1]
            if "zip" in key3:
                z = zipfile.ZipFile(buffer)
    
                for filename in z.namelist():
                    if resultState == True:
                        break
                    file_info = z.getinfo(filename)
                    s3r.meta.client.upload_fileobj(
                        z.open(filename),
                        Bucket=bucketname,
                        Key='result_files/' + f'{filename}')
    
                    obj = s3c.get_object(Bucket=bucketname, Key='result_files/' + filename)
    
                    #df = pd.read_csv(io.BytesIO(obj['Body'].read()), skiprows=0)
                    df = pd.read_csv(io.BytesIO(obj['Body'].read()), engine='python',  skiprows=0)
                    df = df[['linkedaccountname', 'zonal', 'productname', 'invoicedate', 'totalcost']]  
    
                    df['linkedaccountname'] = df['linkedaccountname'].map(str)
                    df2 = df['linkedaccountname'].unique()
                    
                    xW = df2.size
                    x = -1
    
                    id2 = "" 
    
                    while x < xW-1:
                        x = x+1
                        if len(x1) == 10:
                            x1 = "00" + x1
                        if len(x1) == 11:
                            x1 = "0" + x1
                        df['linkedaccountname'] = df['linkedaccountname'].replace(x0, x1)

                        try:
                            name1 = boto3.client('organizations').describe_account(AccountId=x1).get('Account').get('Name')
                            df.loc[df["linkedaccountname"] == str(x1), "linkedaccountname"] = name1     
                        except:
                            name1 = "Your-AWS-Account"
                            df.loc[df["linkedaccountname"] == str(x1), "linkedaccountname"] = name1        

                    
                    df = df.groupby(['invoicedate', 'zonal', 'linkedaccountname', 'productname'], as_index = False)['totalcost'].sum()
                    
                    filename = str(datetime.today)
                    
                    filenameStr = filename[0:21] + "|" + env_user
                    
                    env_user = str(env_user).replace(":", "-")
                
                    l1l = random.randrange(16000, 99000)
    
                    filenameStr = env_user +  "-" + ll1 + ".csv"                                     
    
                    csv_buffer = StringIO()
                    df.to_csv(csv_buffer)

                    s3r.Object(bucket, filenameStr).put(Body=csv_buffer.getvalue()) 
                    
                    # write-only bucket, registration read, delete on receiving key, by trigger.  
                    
                    # https://docs.aws.amazon.com/cur/latest/userguide/product-columns.html
                    
                    bucket = s3r.Bucket(bucketname)
                    try:
                        bucket.object_versions.filter(Prefix="//").delete()
                    except:  
                        print("1")                  
                    s3c.put_object(Bucket=bucketname, Key=('/'+'/'))
                    try:
                        bucket.object_versions.filter(Prefix="result_files/").delete()
                    except:  
                        print("1")     
    
    
    
    else:
        print("not a gzip file")
