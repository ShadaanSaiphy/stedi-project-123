import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# load accelerometer readings from landing zone
accelerometer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://stedi-project-123/accelerometer/landing/"],
        "recurse": True
    },
    format="json",
    transformation_ctx="accelerometer_landing"
)

# load trusted customers so we can filter by consent
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted"
)

accel_df = accelerometer_landing.toDF()
customer_df = customer_trusted.toDF()

# inner join on email - only keep readings from consented customers
# select only accelerometer columns, drop customer fields
joined = accel_df.join(customer_df, accel_df.user == customer_df.email, "inner") \
                 .select(accel_df["*"])

accelerometer_trusted_frame = DynamicFrame.fromDF(joined, glueContext, "accelerometer_trusted_frame")

# write out and keep catalog schema up to date
glueContext.write_dynamic_frame.from_catalog(
    frame=accelerometer_trusted_frame,
    database="stedi",
    table_name="accelerometer_trusted",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE"
    },
    transformation_ctx="accelerometer_trusted_sink"
)

job.commit()
