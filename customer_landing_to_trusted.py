import sys
from pyspark.sql.functions import col
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

# load customer landing data from s3
customer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://stedi-project-123/customer/landing/"],
        "recurse": True
    },
    format="json",
    transformation_ctx="customer_landing"
)

# only keep customers who agreed to share data for research
# drop anyone with a blank sharewithresearchasofdate
customer_df = customer_landing.toDF()
consented = customer_df.filter(col("sharewithresearchasofdate").isNotNull())

customer_trusted_frame = DynamicFrame.fromDF(consented, glueContext, "customer_trusted_frame")

# write to trusted zone and update the glue catalog schema dynamically
glueContext.write_dynamic_frame.from_catalog(
    frame=customer_trusted_frame,
    database="stedi",
    table_name="customer_trusted",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE"
    },
    transformation_ctx="customer_trusted_sink"
)

job.commit()
