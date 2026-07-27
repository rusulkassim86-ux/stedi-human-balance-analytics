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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1785109689189 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1785109689189")

# Script generated for node Customer Trusted
CustomerTrusted_node1785109775876 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1785109775876")

# Script generated for node Join With Customer
SqlQuery1345 = '''
SELECT a.`user`, a.`timestamp`, a.x, a.y, a.z
FROM accelerometer a
JOIN customer c ON a.`user` = c.email
'''
JoinWithCustomer_node1785109834999 = sparkSqlQuery(glueContext, query = SqlQuery1345, mapping = {"customer":CustomerTrusted_node1785109775876, "accelerometer":AccelerometerLanding_node1785109689189}, transformation_ctx = "JoinWithCustomer_node1785109834999")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=JoinWithCustomer_node1785109834999, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1785113292021", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1785113750130 = glueContext.getSink(path="s3://stedi-lakehouse-2026/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1785113750130")
AmazonS3_node1785113750130.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1785113750130.setFormat("glueparquet", compression="snappy")
AmazonS3_node1785113750130.writeFrame(JoinWithCustomer_node1785109834999)
job.commit()