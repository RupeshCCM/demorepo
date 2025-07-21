import sys
import boto3
import base64
import json
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def get_private_key(secret_arn):
    client = boto3.client("secretsmanager", region_name="us-east-2")
    #secret_value = client.get_secret_value(SecretId=secret_arn)["SecretString"]
    
    response = client.get_secret_value(SecretId=secret_arn)
    if 'SecretString' in response:
        private_key = response['SecretString']
    else:
        private_key = base64.b64decode(response['SecretBinary']).decode("utf-8")
    
    # secret_dict = json.loads(secret_value)
    # private_key = base64.b64decode(secret_dict["private_key"])
    
    key_bytes = base64.b64decode(private_key)
    private_key_obj = serialization.load_der_private_key(
        key_bytes,
        password=None,
        backend=default_backend()
    )
    return private_key

# Load private key


try:
    # Load private key
    #private_key = get_private_key(secret_arn)
    private_key = get_private_key("arn:aws:secretsmanager:us-east-2:477725826137:secret:test/RECRUITINGAPI/PrivateKey-o4FoHe")

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user="API_RECRUITING_TEST",
        account="vg35602.us-east-1",
        private_key=private_key,
        authenticator="SNOWFLAKE_JWT",
        warehouse="WH_API_RECRUITING_TEST",
        database="KIOSK_TEST",
        schema="APP_ENTERPRISE",
        role="API_RECRUITING_TEST"
    )

    cursor = conn.cursor()
    cursor.execute("SELECT current_user(), current_date()")
    results = cursor.fetchall()
    print(f"[INFO] Query Results: {results}")

except snowflake.connector.errors.Error as sf_err:
    print(f"[ERROR] Snowflake connection/query failed: {str(sf_err)}")
    raise

except Exception as e:
    print(f"[ERROR] General exception occurred: {str(e)}")
    raise

finally:
    job.commit()

job.commit()