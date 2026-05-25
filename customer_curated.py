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

# pull in trusted customers and trusted accelerometer data
customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted"
)

customer_df = customer_trusted.toDF()
accel_df = accelerometer_trusted.toDF()

# only keep customers who actually have accelerometer data
# distinct to avoid duplicates from the one-to-many join
curated = customer_df.join(accel_df, customer_df.email == accel_df.user, "inner") \
                     .select(customer_df["*"]) \
                     .distinct()

customer_curated_frame = DynamicFrame.fromDF(curated, glueContext, "customer_curated_frame")

glueContext.write_dynamic_frame.from_catalog(
    frame=customer_curated_frame,
    database="stedi",
    table_name="customer_curated",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE"
    },
    transformation_ctx="customer_curated_sink"
)

job.commit()
