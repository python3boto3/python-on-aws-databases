import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import boto3
import psycopg2
import pandas as pd
from io import StringIO
import pyarrow as pa
import pyarrow.parquet as pq

def lambda_handler(event, context):
      
    s3r = boto3.resource('s3', 'us-east-1')
    s3c = boto3.client('s3', 'us-east-1')
    
    # define, instantiate the dataframe
    
    df = pd.DataFrame()

    # reset all counters
    
    results=""
    rows = ""
    
    #list the fieldnames.
    
    fieldnames = ['ze', 'zd', 'zc', 'zb', 'za']
    # default null on all field values - with redshift, forum stakeholders are adamant that actively nulling base values  
    # is MANDATORY to avoid errors
    
    ze_ = "null"  
    zd_ = "null" 
    zc_ = "null"
    zb_ = "null"
    za_ = "null"

    
    rows2 = ""
    
    #open the redshift connection
    client = boto3.client(service_name='redshift',
                            region_name='us-east-1')                                                        # # # # # # # # # # # # # 
    
    # using boto3 to get the Database token instead of hardcoding it in the code
    
    cluster_creds = client.get_cluster_credentials(
                                DbUser='awsuser',                                                           # # # # # # # # # # # # # 
                                DbName='dev',                                                               # # # # # # # # # # # # # 
                                ClusterIdentifier='redshift-cluster-1',                                     # # # # # # # # # # # # # 
                                AutoCreate=False)
    
    try:
        # Database connection below that uses the DbPassword that boto3 returned
        conn = psycopg2.connect(        
                    host = 'redshift-cluster-1.abcx1l3fghg2.us-east-1.redshift.amazonaws.com',              # # # # # # # # # # # # # 
                    port = '5439',
                    user = cluster_creds['DbUser'],
                    password = cluster_creds['DbPassword'],
                    database = 'dev'
                    )                
    
        # Verifies that the connection worked
        cursor = conn.cursor()
        #cursor.execute("select airlineorderinfo FROM dev.public.rs_input_table")
        cursor.execute("select distinct zc, za FROM dev.public.rs_input_table")           # # # # # # # # # # # # # 
        results = cursor.fetchall()
        sqlResult = results
        if (results is None):    
            print("Could not find results")
        else:
            # returns the collection of all rows and offer replacement formatting values. 
    
            # CAUTION - think twice before removing...
            # Rf: https://theasciicode.com.ar/
            # The reason for replacements below: one of the airlineorderinfo columns contains 
            # many !@##$% typical special characters ... the ones chosen below are more inaccessible 
            # as chrs 191, 218 are on the extended ascii characterset, they do not interfere with the pattern.
    
            # By doing so, we can navigate rows, and navigate field key pairs within each row. 
    
            # replaces key pair separator with ascii 254
            sqlResult2 = str(sqlResult).replace("%3D","■")
    
            # replaces field end with ascii char 186
            sqlResult2 = str(sqlResult2).replace("%0D%0A","║")
    
            # create row start markers, replace apostrophe
            sqlResult2 = str(sqlResult2).replace("'zd", "┌zd")
            sqlResult2 = str(sqlResult2).replace("[(┌","")
    
            # create row end markers
            # replaces first record start character ' [  with ascii char 217, 218
            # replaces record all end characters except final with ascii char 218
            #sqlResult2 = str(sqlResult2).replace("',)]","┘")
            #sqlResult2 = str(sqlResult2).replace(",), (┌","┘┌")
            #sqlResult2 = str(sqlResult2).replace("'┘","┘")
    
            sqlResult2 = str(sqlResult2).replace(", ┌","║")
            
            sqlResult2 = str(sqlResult2).replace("[(","┌zc■")
            sqlResult2 = str(sqlResult2).replace(" ("," (┌zc■")
            sqlResult2 = str(sqlResult2).replace(", '","║")
    
            sqlResult2 = str(sqlResult2).replace("')","┘')")
            sqlResult2 = str(sqlResult2).replace('║")','║┘")')
    
            sqlResult2 = str(sqlResult2).replace('║"┘','║┘"')
    
            # Interesting malformation, some occasionally come in with DBL QUOTES, spaces and commas...
            sqlResult2 = str(sqlResult2).replace('"za', 'za')
            sqlResult2 = str(sqlResult2).replace(', za', '║za')
            sqlResult2 = str(sqlResult2).replace(',za', '║za')
            
            # calculate end rows count for chr 217 ┘ 
            x = sqlResult2.count("┘")
    
            entireLen = len(sqlResult2)
            #'15,519'
            beginRowchar=0
            # print(entireLen)
            i = 0 # initial value
            # (i) now = the counter for the number of rows (x))
            # null strings
    
            za_ = 0
    
            zb_ = 0
    
            zd_ = 0
    
            zc_ = 0
    
            ze_ = 0
    
            # compound string
            valueStrX = ""
            headerStrX = ""
            keyPair = ""
            sqlResult2 = str(sqlResult2).replace("┌","")
    
            # row data calcs ...............................................................................
    
    
            for i in range(0, x):            
                #print(x)# max rows
                #print(i)# current row
                # find the start char (beg) and ending char (end) for the row
                lastPos = entireLen-beginRowchar
                endRowChar = sqlResult2.find('┘', beginRowchar)
                currentRowStr = sqlResult2[beginRowchar:endRowChar]
                # print(currentRowStr)
                #print(" ")
                # calculate number of fields in row 1 
                currentRowFieldCount = str(currentRowStr).count("║")
                #print(currentRowFieldCount)
                beginFieldchar = 0
    
                # field data calcs...........................................................................
                
    
                beginRowchar = endRowChar+1
    
                endFieldCharNum = 0
                beginFieldchar = 0
                midDefNum = 0
                v = 0
                currentRowFieldCount = currentRowFieldCount
    
                tableDestRowStr1 = "NULL"
                tableDestRowStr2 = "NULL"
                
    
                for v in range(0, currentRowFieldCount):
                    #print(i)
    
                    #Field finder
    
                    endFieldCharNum = str(currentRowStr).find('║', beginFieldchar)
                    midDefNum = str(currentRowStr).find('■', beginFieldchar)
    
                    #Field split character
    
                    totField = currentRowStr[beginFieldchar:endFieldCharNum]
                    #print(totField)
    
                    midFieldNum = str(totField).find('■')
    
                    #Header
    
                    headerStr = totField[0:midFieldNum]
    
                    #print(headerStr)
    
                    #Value
    
                    valueStr = totField[midFieldNum+1:]
    
                    #print(valueStr)
    
                    # increment of the counter position based on the last found position
    
                    beginFieldchar = endFieldCharNum+1
    
                    #valueStrX = valueStrX + ", " + valueStr
                    #headerStrX = headerStrX + ", " + headerStr
    
                    headerStr = headerStr.lower()
    
                    keyPair = keyPair + headerStr + ": " + valueStr + "\r\n"
    
                    gapIntStr = ": "

    
                # ensure integer, 7 characters for the zc
                zc1 = str(zc_)
                zc1 = int(zc1[:7])
    
    
                # alternative trim function, cleans trailing spaces and any special characters
                zc_ = zc1
    
                # Use each stripped value and add to the relevant columns in the dataframe
    
                dict = {'za': za_, 'zb': zb_, 'zc': zc_, 'zd': zd_, 'ze': ze_}
    
                # append each of the 313 test items
    
                df = df.append(dict, ignore_index = True)    
    
            # post these to S3 as csv for the upload to redshift
            
            #print(11)
            
            # - - - - - -

            csv_buffer = StringIO()
            # redshift expects a headerless import, passing the dataframe to csv. 
            df.to_csv(csv_buffer, header=False, index=False)
            # adding a key store name
            key1 = "x4.csv"
            # target bucket (alter)
            bucket4 = "output-s3"
            # put 
            s3r.Object(bucket4, key1).put(Body=csv_buffer.getvalue())
            
            #print(12)
            
            # upload into redshift 
            sql_query = """Truncate "dev"."public"."rs_output_table";"""
            cursor = conn.cursor()
            cursor.execute(sql_query)
            conn.commit()
            
            #print(13)
            # Copy data
            cursor = conn.cursor()
            sql_query = """COPY dev.public.rs_output_table FROM 's3://output-s3/x4.csv' IAM_ROLE 'arn:aws:iam::998842241133:role/service-role/AmazonRedshift-CommandsAccessRole-20221031T140999' FORMAT AS CSV DELIMITER ',' REGION AS 'us-east-1'"""
            cursor.execute(sql_query)
            
            conn.commit()
            conn.close()

            #print(14)
            # https://stackoverflow.com/questions/41066582/python-save-pandas-data-frame-to-parquet-file
            table = pa.Table.from_pandas(df)
            
            pq.write_table(table, '/tmp/file_name.parquet', compression='SNAPPY')
            #print(15)

            s3c.upload_file('/tmp/file_name.parquet', 'output-s3', 'x.parquet')
            #print(16)

    except:
    
        conn.close()
    
        print('Failed to open database connection.')
    
    