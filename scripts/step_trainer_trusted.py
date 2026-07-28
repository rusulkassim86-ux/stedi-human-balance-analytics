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

# Script generated for node Customer Curated
CustomerCurated_node1785196354571 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1785196354571")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1785196261913 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1785196261913")

# Script generated for node Filter By Serial Number
SqlQuery1579 = '''
SELECT s.*
FROM steptrainer s
WHERE s.serialnumber IN (SELECT c.serialnumber FROM customer c)
'''
FilterBySerialNumber_node1785196440064 = sparkSqlQuery(glueContext, query = SqlQuery1579, mapping = {"steptrainer":StepTrainerLanding_node1785196261913, "customer":CustomerCurated_node1785196354571}, transformation_ctx = "FilterBySerialNumber_node1785196440064")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=FilterBySerialNumber_node1785196440064, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1785195939831", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1785196531278 = glueContext.getSink(path="s3://stedi-lakehouse-2026/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1785196531278")
AmazonS3_node1785196531278.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1785196531278.setFormat("glueparquet", compression="snappy")
AmazonS3_node1785196531278.writeFrame(FilterBySerialNumber_node1785196440064)
job.commit()