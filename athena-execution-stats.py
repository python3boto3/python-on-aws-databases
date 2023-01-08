import boto3
import json
import datetime
from datetime import datetime
import os
from contextlib import suppress

session = boto3.session.Session(profile_name="s", region_name = "us-east-1")
athc = session.client('athena')
s3c = session.client('s3')
s3r = session.resource('s3')

ex_response = athc.list_query_executions(
    MaxResults=50,
    WorkGroup='primary'
)
#print(ex_response)
x = 0
q = (ex_response['QueryExecutionIds'])
for i in q:
#for QueryExecutionIds in ex_response:   
    id_response = athc.get_query_execution(
        QueryExecutionId=i # positive tester 'bb7df340-a33d-45c4-a573-1e869264eb45'
    )
    print(id_response)
    id_response_Str = str(id_response)
    xnameStr = ""
    bool = False
    if 's4_gateway_customer_element_agg' in id_response_Str:
        xnameStr = "v_customer_element_2"
        bool = True
    if 's4_gateway_transaction_base' in id_response_Str:
        xnameStr = 'v_transaction_records'
        bool = True
    if 's4_gateway_batch_detail' in id_response_Str:
        xnameStr = 'v_gateway_detail'
        bool = True
    if bool == True:
    # Milliseconds to execute
        etm = id_response['QueryExecution']['Statistics']['TotalExecutionTimeInMillis']
        etm = etm/1000
        etm2 = '%.3f' %etm
        etmStr = '%.3f' %etm + ' seconds'
        #with suppress(KeyError):
        sdt = id_response['QueryExecution']['Status']['SubmissionDateTime']
        dt_submitted = datetime.strftime(sdt, '%H:%M:%S %m/%d/%Y')
        cdt = id_response['QueryExecution']['Status']['CompletionDateTime']
        dt_completed = datetime.strftime(cdt, '%H:%M:%S %m/%d/%Y')
        status_Str = id_response['QueryExecution']['Status']['State']
        message2 = "TEST ONLY  IGNORE - Athena query exceeded time limit ... Execution id: " + i + ' ... ' + 'Execution time: ' + etm2 + ' seconds' + ' ... '
        message2 = message2 + 'Submission time = ' + dt_submitted + ' ... '  + 'Completion time = ' + dt_completed + ' ... ' + 'Status = ' + status_Str
        message2 = "Critical alert: " + xnameStr + ' ... ' + message2
        # os.environ["s3_dict_path"]:  (replace)
        os.environ["bucket"] = 'v-dev'
        bucket = os.environ["bucket"]
        os.environ["s3_dict_path"] = 'raw/payments/transactions/archive/tmpF/'
        fault_dict = os.environ["s3_dict_path"]
        # Q. Does the key exist in previous faults?
        bucket_name = "testbucket-frompython-2"
        response = s3c.list_objects_v2(Bucket=bucket, Prefix=fault_dict)
        files = response.get("Contents")
        bool_sent_already = False
        for file in files:
            print(file['Key'])
            print(fault_dict+i)
            if fault_dict+i == file['Key']:
                bool_sent_already = True
                break
        arn = 'arn:aws:sns:us-east-1:854012215868:bi-test-sns-failure'
        message = message2
        snsc = session.client('sns')
        os.environ["timeout_seconds"] = "2.00"
        env_secs = os.environ["timeout_seconds"]
        if bool_sent_already == False and float(etm2) >= float(env_secs):
            s3c.put_object(Body='', Bucket=bucket, Key=fault_dict + i)
            response = snsc.publish(TargetArn=arn, Message=json.dumps(message))
        else:
            print("Not required, already alerted.")
        # Write a token execution query id to the s3 fault dictionary path
    else:
        x = x+1
    '''
Above - latest 50 queries
Newer sessions are listed first; older sessions are listed later.
https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/athena.html#Athena.Client.list_query_executions
'''