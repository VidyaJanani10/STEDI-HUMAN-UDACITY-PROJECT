machinelearning_curated

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1769060133803 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-project-vj/step_trainer/trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1769060133803")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1769060134864 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-project-vj/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1769060134864")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from a join s on a.timestamp = s.sensorreadingtime;
'''
SQLQuery_node1769060139206 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"a":accelerometer_trusted_node1769060134864, "s":step_trainer_trusted_node1769060133803}, transformation_ctx = "SQLQuery_node1769060139206")

# Script generated for node machinelearning_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769060139206, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769057155658", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
machinelearning_curated_node1769060143963 = glueContext.getSink(path="s3://stedi-human-project-vj/machinelearning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machinelearning_curated_node1769060143963")
machinelearning_curated_node1769060143963.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machinelearning_curated")
machinelearning_curated_node1769060143963.setFormat("json")
machinelearning_curated_node1769060143963.writeFrame(SQLQuery_node1769060139206)
job.commit()
