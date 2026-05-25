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

# load step trainer records from landing zone
step_trainer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://stedi-project-123/step_trainer/landing/"],
        "recurse": True
    },
    format="json",
    transformation_ctx="step_trainer_landing"
)

# use curated customers as the filter - these are confirmed consented + have accel data
customer_curated = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="customer_curated"
)

step_df = step_trainer_landing.toDF()
customer_df = customer_curated.toDF()

# serial number is not unique so a regular join causes duplicates/wrong counts
# use semi join instead - keeps step trainer rows that exist in curated customers
# without multiplying rows due to the non-unique serial number issue
trusted = step_df.join(customer_df, step_df.serialnumber == customer_df.serialnumber, "leftsemi")

step_trainer_trusted_frame = DynamicFrame.fromDF(trusted, glueContext, "step_trainer_trusted_frame")

glueContext.write_dynamic_frame.from_catalog(
    frame=step_trainer_trusted_frame,
    database="stedi",
    table_name="step_trainer_trusted",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE"
    },
    transformation_ctx="step_trainer_trusted_sink"
)

job.commit()
