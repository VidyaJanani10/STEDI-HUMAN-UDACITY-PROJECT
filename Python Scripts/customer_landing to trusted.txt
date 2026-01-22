Python Script of Customer_landing to trusted
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

# Script generated for node customer_landing1
customer_landing1_node1769057192158 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-human-project-vj/customer/"], "recurse": True}, transformation_ctx="customer_landing1_node1769057192158")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from customer_landing
where shareWithResearchAsOfDate IS NOT NULL;
'''
SQLQuery_node1769057195537 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"customer_landing":customer_landing1_node1769057192158}, transformation_ctx = "SQLQuery_node1769057195537")

# Script generated for node cutomer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1769057195537, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1769057155658", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
cutomer_trusted_node1769057200428 = glueContext.getSink(path="s3://stedi-human-project-vj/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="cutomer_trusted_node1769057200428")
cutomer_trusted_node1769057200428.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="customer_trusted")
cutomer_trusted_node1769057200428.setFormat("json")
cutomer_trusted_node1769057200428.writeFrame(SQLQuery_node1769057195537)
job.commit()

