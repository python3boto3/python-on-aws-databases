import boto3

# local session variable only
session = boto3.session.Session(profile_name="red")

# ordering sort programmatically for 1,800 items ... 
# returning date, size, key name, date last modified ...

#s3 object placeholders, old series [r], new serices [c]

s3r = session.resource('s3', 'us-east-1')
s3c = session.client('s3', 'us-east-1')

myBucket = s3r.Bucket('sagemaker-101')
x = 0
def obj_last_modified(myobj):
    return myobj.last_modified
    
#Search all folders, subfolders ina named bucket - return values for latest file. 
sortedObjects = sorted(myBucket.objects.all(), key=obj_last_modified, reverse=True)
#print(bucket name)

print("  ")

print("s3 bucket search name: " + str(myBucket)[15:-1])
print("- - - - - - - - - - - - - - - - - - - - - - - ")
#print(lastmodified date in sortedObjects)
print("date of object found = " + str(sortedObjects[0].last_modified))
#print(lastmodified size of item)
print("file size obj. found = " + str(sortedObjects[0].size)+ " bytes")
#print(lastmodified size of key)
print("file name and /path/ = " + str(sortedObjects[0].key) )
print("- - - - - - - - - - - - - - - - - - - - - - - ")
# Get fast head() count in entire collection
x = sum(1 for _ in myBucket.objects.all())
print("total: objects found = " + str(x))

print("  ")


