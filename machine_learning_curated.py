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

# both inputs are already filtered to consented customers only
step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted"
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted"
)

step_df = step_trainer_trusted.toDF()
accel_df = accelerometer_trusted.toDF()

# join on matching timestamps - this links each step trainer reading
# to the accelerometer reading captured at the same moment
ml_df = step_df.join(accel_df, step_df.sensorreadingtime == accel_df.timestamp, "inner") \
               .select(
                   step_df.sensorreadingtime,
                   step_df.serialnumber,
                   step_df.distancefromobject,
                   accel_df.user,
                   accel_df.x,
                   accel_df.y,
                   accel_df.z
               )

ml_curated_frame = DynamicFrame.fromDF(ml_df, glueContext, "ml_curated_frame")

glueContext.write_dynamic_frame.from_catalog(
    frame=ml_curated_frame,
    database="stedi",
    table_name="machine_learning_curated",
    additional_options={
        "enableUpdateCatalog": True,
        "updateBehavior": "UPDATE_IN_DATABASE"
    },
    transformation_ctx="ml_curated_sink"
)

job.commit()
