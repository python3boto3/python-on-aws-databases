# <span style="color:gray" fontstyle="bold">REDSHIFT LAYER: </span><span style="color:green">PSYCOPG2-BINARY</span>

# Postgresql connector. 

## <span style="color:gray" fontstyle="bold">Redshift Config: </span>

_Ignore this step for the Redshift if it communicates within the vpc internally._

(Ex-VPC, over the wire):


To allow Redshift accessibility outside the vpc: add an elastic address (EIP)

On the cluster console, add Actions: Modify publicly accessible setting.

Enable .. and reference the EIP

Wait 10 minutes to allow the cluster to modify.

---
# Redshift specifics
Ensure that all variables  (field values are overtly null at inception) 

or this will cause errors.

---
 
## <span style="color:gray" fontstyle="bold">Create lambda layer: </span>
On Cloud9 only  basic default t2.micro ami linux settings
 

---
## <span style="color:gray" fontstyle="bold">Cloud9 script: </span>
_(Window new terminal)_

virtualenv v-env

source ./v-env/bin/activate    

pip install psycopg2-binary 

deactivate

mkdir python

cd python

cp -r ../v-env/lib64/python3.7/site-packages/* .

cd ..

zip -r psycopg2_bin_layer.zip python

aws lambda publish-layer-version --layer-name psycopg2binary --zip-file fileb://psycopg2_bin_layer.zip --compatible-runtimes python3.7

---


## <span style="color:gray" fontstyle="bold">Permission config: </span>
Add these permissions (and probably more granular versions of these, relating 

to specific regions and resources) to the lambda ExecutionRole

AmazonRedshiftFullAccess	
AmazonRedshiftAllCommandsFullAccess
AmazonRedshiftDataFullAccess

---
## <span style="color:gray" fontstyle="bold">Resource config: </span>
Timeout, set to minutes, seconds not the default, set up to 10240 MB memory for speed.

---
## <span style="color:gray" fontstyle="bold">Notes: </span>
On lambda use psycopg2-binary / you can only use psycopg2 on local machines (on 64-bit python)

This configuration is for python 3.7

The COPY command is 30 times faster than an equivalent insert command 

Tested on 15,000 fields. 

PyArrow is faster at parquet conversion than most smilar packages. Pyarrow is a development platform for in-memory analytics. It contains a set of technologies that enable big data systems to store, process and move data fast.
