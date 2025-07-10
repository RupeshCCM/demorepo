import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1713164643863 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://entityresolutions/customer_output/ER-customer-wf1/ER-customer-wf1/9b6fd494a1b74a1aba5211fedc083729/success/run-1713164258528-part-r-00000"]}, transformation_ctx="AmazonS3_node1713164643863")

# Script generated for node Drop Duplicates
DropDuplicates_node1713164720737 =  DynamicFrame.fromDF(AmazonS3_node1713164643863.toDF().dropDuplicates(["matchid"]), glueContext, "DropDuplicates_node1713164720737")

# Script generated for node Amazon S3
AmazonS3_node1713164793768 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1713164720737, connection_type="s3", format="csv", connection_options={"path": "s3://entityresolutions/customer_output/ER-customer-wf1/ER-customer-wf1-merge/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1713164793768")

job.commit()