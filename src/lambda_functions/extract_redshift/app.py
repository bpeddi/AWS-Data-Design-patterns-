from os import environ, getenv
import datetime
from decimal import Decimal

# imports added by Lambda layer
# pylint: disable=import-error
# third party
import awswrangler as wr
import boto3, botocore
import pandas as pd
import json

# imports added by Lambda layer
# pylint: disable=import-error
from aws_lambda_powertools import Logger, Metrics, Tracer
from aws_lambda_powertools.metrics import MetricUnit

LOGGER = Logger()
TRACER = Tracer()
METRICS = Metrics()

METRICS.add_dimension(name="Direction", value="Unloading")
METRICS.add_dimension(name="Task", value="RedshiftUnload")



SecretArn = getenv("DBSECRETARN")
RedshiftDbName = getenv("REDSHIFT_DB_NAME")
RedshiftIAMRole = getenv("REDSHIFT_IAM_ROLE")
ClusterIdentifier = getenv("CLUSTER_IDENTIFIER")
DYNAMODB_META_TABLE_NAME = getenv("DYNAMODB_META_TABLE_NAME")
Aws_Region = getenv("REGION")




# MAX_INPUT_RECORD_COUNT = int(getenv("MAX_INPUT_RECORD_COUNT", "300000"))
DYNAMODB_RESOURCE = boto3.resource("dynamodb")
# DYNAMODB_TABLE_KEYS = [schema["AttributeName"] for schema in DYNAMODB_TABLE.key_schema]

#Following function saves Step function token in Dynamodb
#this token used after succesful completion of running stored procedure in redshift
def put_item_dynamo_db(data):

    DYNAMODB_TABLE = DYNAMODB_RESOURCE.Table(DYNAMODB_META_TABLE_NAME)
    try:
        result = DYNAMODB_TABLE.put_item(
            Item={
                "statementName": data["statementName"],
                "token": data["token"],
            }
        )
    except botocore.exceptions.ClientError as e:
        LOGGER.info("Not Found")
        raise Exception("Error in wrting Token ")

    return True


@LOGGER.inject_lambda_context
def lambda_handler(event, context):

    token = event.get("token")
    #stored procedure names passed from Stepfunction
    spname = event.get("spname")
    event_message = event.get("event_message")

    current_time = datetime.datetime.now().timestamp()
    statementName = "redshift-storedproc-event-|" + str(current_time)

    data = dict(statementName=statementName,token=token)

    response = put_item_dynamo_db(data)

    if response:
        #Calling stored procedure
        SqlStatement = f"""
                        call {spname} ('{event_message}');
            """
        LOGGER.info("Sending SQL Statement = %s", SqlStatement)

        #AWS Redshift Data API Client connection
        vRedshiftDataClient = boto3.client(
            service_name="redshift-data",
            region_name=Aws_Region
        )

        #Submit the stored proc asyncrounosly. String provided with StatementName is returned by redshift to Event Bridge with status.
        # Check EventBridgeRedshiftEventRule in Redshift_stored_proc.yaml file.
        #https://awscli.amazonaws.com/v2/documentation/api/latest/reference/redshift-data/execute-statement.html
        #--statement-name (string)
        #The name of the SQL statement. You can name the SQL statement when you create it to identify the query.
        #--with-event = True
        #A value that indicates whether to send an event to the Amazon EventBridge event bus after the SQL statement runs.


        redshiftResponse = vRedshiftDataClient.execute_statement(
            ClusterIdentifier=ClusterIdentifier,
            Database=RedshiftDbName,
            SecretArn=SecretArn,
            Sql=SqlStatement,
            StatementName=statementName,
            WithEvent=True,
        )

        LOGGER.info("Completed Sending SQL statement using Redshift Data API ")

    return {
        "redshiftCallResponse": json.dumps(redshiftResponse, default=str),
        "status": "success",
    }
