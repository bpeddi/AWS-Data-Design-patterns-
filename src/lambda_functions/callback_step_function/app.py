from os import environ, getenv
from datetime import datetime
from decimal import Decimal
from aws_lambda_powertools.shared.constants import LOGGER_LOG_EVENT_ENV

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

DYNAMODB_META_TABLE_NAME = getenv("DYNAMODB_META_TABLE_NAME")
DYNAMODB_RESOURCE = boto3.resource("dynamodb")

# After succesful processing, Get current row from DynamoDB , this row is added into history table
def get_item_from_dynamodb(statementName):

    DYNAMODB_TABLE = DYNAMODB_RESOURCE.Table(DYNAMODB_META_TABLE_NAME)
    try:
        response = DYNAMODB_TABLE.get_item(
            Key={"statementName": statementName}, ConsistentRead=True
        )
        LOGGER.info(response)
        if "Item" in response:
            return response["Item"]
    except botocore.exceptions.ClientError as e:
        raise Exception(
            "Error in getting Token NSC.EDM.Vault.Lambda.MissingDynamoDBObject"
        )


def get_error_from_redshift(statementid, token):
    try:
        mysession = boto3.session.Session()
        vRedshiftDataClient = mysession.client(service_name="redshift-data")
        LOGGER.info("getting job status for statement id = %s", statementid)
        redshiftResponse = vRedshiftDataClient.describe_statement(Id=statementid)
    except Exception as ex:
        send_failed_message_to_sfn(
            token, "Error in getting error message from Redshift" + str(ex)
        )
        raise Exception("Error in getting error message from Redshift" + str(ex))

    return redshiftResponse


def send_failed_message_to_sfn(token, message):

    mysession = boto3.session.Session()
    vSFNClient = mysession.client(
        service_name="stepfunctions",
    )
    sfnResponse = vSFNClient.send_task_failure(
        taskToken=token,
        error="RedshiftDataAPI.Failed",
        cause=json.dumps(message, default=str),
    )
    LOGGER.info("sfnResponse = %s", sfnResponse)


def send_success_message_to_sfn(token, message):

    mysession = boto3.session.Session()
    vSFNClient = mysession.client(
        service_name="stepfunctions",
    )
    sfnResponse = vSFNClient.send_task_success(
        taskToken=token,
        output=json.dumps(message, default=str),
    )
    LOGGER.info("sfnResponse = %s", sfnResponse)


@LOGGER.inject_lambda_context
def lambda_handler(event, context):

    statementname = event.get("detail").get("statementName")
    statementid = event.get("detail").get("statementId")
    job_status = event.get("detail").get("state")
    LOGGER.info(
        "statement name %s ,statement id %s , job_status %s",
        statementname,
        statementid,
        job_status,
    )
    response = get_item_from_dynamodb(statementname)
    token = response["token"]
    if job_status == "FINISHED":
        LOGGER.info("## Send Task Success to Step Functions")

        send_success_message_to_sfn(token, response)
    else:
        redshiftResponse = get_error_from_redshift(statementid, token)
        send_failed_message_to_sfn(token, redshiftResponse)
    return event

