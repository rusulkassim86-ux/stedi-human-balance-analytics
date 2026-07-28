# STEDI Human Balance Analytics

A data lakehouse built on AWS Glue, S3, and Athena that prepares Step Trainer and
mobile accelerometer data for machine learning, while respecting which customers
consented to share their data for research.

## Background

STEDI makes a hardware Step Trainer that measures steps and detects the distance
to nearby objects. A companion mobile app records accelerometer readings from the
customer's phone. Data science needs both streams joined on the moment each
reading was taken, but only for customers who opted in to research.

## Architecture

Data moves through three zones.

**Landing** holds the raw JSON exactly as delivered. Tables are registered in the
Glue Data Catalog with Athena DDL. Nothing is filtered here.

**Trusted** holds only records belonging to customers who consented to research.
The privacy filter happens at the boundary between landing and trusted, so no
downstream job has to remember to apply it.

**Curated** holds joined tables ready for analysis and model training.

## Row counts

| Zone | Table | Rows |
| --- | --- | --- |
| Landing | customer_landing | 956 |
| Landing | accelerometer_landing | 81,273 |
| Landing | step_trainer_landing | 28,680 |
| Trusted | customer_trusted | 482 |
| Trusted | accelerometer_trusted | 40,981 |
| Trusted | step_trainer_trusted | 14,460 |
| Curated | customer_curated | 482 |
| Curated | machine_learning_curated | 43,681 |

## Glue jobs

**customer_landing_to_trusted.py** drops customers with no value in
sharewithresearchasofdate. 956 rows in, 482 out.

**accelerometer_landing_to_trusted.py** joins accelerometer readings to
customer_trusted on the customer's email, keeping only the five accelerometer
columns so no personal data reaches the trusted table.

**customer_trusted_to_curated.py** keeps only customers who actually have
accelerometer readings. The query uses SELECT DISTINCT because each customer
matches many readings.

**step_trainer_trusted.py** keeps Step Trainer readings whose serial number
belongs to a curated customer.

**machine_learning_curated.py** joins Step Trainer readings to accelerometer
readings taken at the same timestamp, producing the training table.

## A note on the serial number filter

Serial numbers repeat across customer records in this dataset, so a conventional
inner join on serialnumber multiplies every sensor reading by the number of
customers sharing that serial. step_trainer_trusted.py uses an IN subquery
instead, which tests membership and returns each source row at most once.

## Repository structure

    sql/          Athena DDL for the three landing tables
    scripts/      The five Glue job scripts
    screenshots/  Athena query results for each table

## Technologies

AWS Glue Studio, Apache Spark, Amazon S3, AWS Glue Data Catalog, Amazon Athena,
Parquet
