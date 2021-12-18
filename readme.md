# Building Event Driven Application to load data into DynamoDB from Redshift using AWS Lambda, Amazon Redshift Data API, Step Function and Event Bridge

## Introduction

Event driven applications are becoming popular with many customers, where application execution happens in response to events. A primary benefit of this architecture is the decoupling of producer and consumer processes, allowing greater flexibility in application design and building decoupled processes. An example of an event driven application that we implemented at our client NSC is an automated Step Function workflow being triggered by an event, which unloads large volumes of Redshift data asynchronously using Redshift Data API and loads this data into DynamoDB table by invoking parallel Lambda function using Step function Map state. This work flow leverages _[Amazon Redshift](https://aws.amazon.com/redshift/),_[AWS Lambda](https://aws.amazon.com/lambda/), [Amazon EventBridge](https://aws.amazon.com/eventbridge/) and [Amazon DynamoDB](https://aws.amazon.com/sns/).

## Solution architecture

This event driven, cost effective serverless architecture offers greater extensibility and simplicity, making it easier to maintain, faster to release new features and also reduce the impact of changes. It also simplifies adding other components or third-party products to the application without much changes.

The following architecture diagram highlights the end-to-end solution:

![](RackMultipart20210506-4-u57ayk_html_4f41bfe408b14a64.png)

1. Step Function invokes AWS Lambda by passing task token as payload
2. AWS Lambda function sends Redshift Unload command asynchronously using Redshift Data API. Since we are making asynchronous call using Redshift Data API, Lambda will not run into timeout issues. This function also save the task token into DynamoDB table.
3. Redshift Unloads the data into S3 bucket into multiple files and sends event to Amazon Event Bridge
4. Amazon EventBridge Event Rule triggers Lambda function with unload status
5. Lambda function return unload query status and original task token by querying DynamoDB to resume the step function work flow.
6. Step function invokes a Lambda function to list all files in S3 folder
7. Step function uses MAP iterator to launch parallel Lambda functions for each file returned from step 6.
8. Each Lambda uses DynamoDB batch writer with multithreading to load the data into DynamoDB

## Pre-requisites

As a pre-requisite for creating the application explained in this blog, you should need to setup an Amazon Redshift cluster and associate it with an [AWS Identity and Access Management (IAM) Role](https://docs.aws.amazon.com/redshift/latest/mgmt/authorizing-redshift-service.html). If you don&#39;t have that provisioned in your AWS account, please follow [Amazon Redshift getting started guide](https://docs.aws.amazon.com/redshift/latest/gsg/getting-started.html) to set it up.

We have used [Amazon Customer Reviews Dataset](https://s3.amazonaws.com/amazon-reviews-pds/readme.html). We loaded this data set intoAmazon redshift

Connect to redshift using your favorite client like [SQL Workbench](https://www.sql-workbench.eu/) , copy and paste the code from scripts/setup\_redshift\_table.sql. This code will create table called amazon\_reviews\_parquet.

This stack will load &quot;amazon\_reviews\_parquet &quot; table into DynamoDB.

##
 Deploy CloudFormatin template using clould9 and SAM

# You can launch [AWS clould9](https://aws.amazon.com/cloud9/) instance and making sure you install python3.8 and create a virtual environment.

- _sudo amazon-linux-extras enable python3.8_
- _ **sudo yum install** _ _python3.8_
- _ **git clone \&lt;This repo\&gt;** _
- _ **cd redshift-to-dynmodb** _
- _ **python3.8 –m venv myvenv** _
- _ **source myvenv/bin/activate** _
- _ **pip install –r requirement.txt** _
- _ **sam build** _
- _ **sam deploy –guided** _

During guided deployment stack will except following parameters to be passed.

- StackPrefix– Provide a friendly name.
- DynamoDbBillingMode – Specify how you are charged for read and write throughput and how you manage capacity.
- DynamoDbPointInTimeRecovery– Enable point in time recovery DynamoDB table backups
- DynamoDbTtl– Specifies the DynamoDb TTL attribute. Leave empty to disable
- SubnetIds– Specify the VPC subnets to deploy the lambda functions. The subnet IDs are comma separated with no spaces. Only needed if using a VPC deployment.
- SecGroup– Specify Security group to deploy the lambda functions. The Security group are comma separated with no spaces. Only needed if using a VPC deployment.
- DataS3Bucket– S3 bucket to Land Redshift data
- ShouldEnableTracing– Enable tracing XRay Tracing for Step Functions and Lambda RedshiftDbName– Redshift data base name, this parameter used for Redshift Data API.
- ClusterIdentifier– Redshift Cluster ID, This parameter used for Redshift Data API.
- DynamodbMetaTable– DynamoDB metadata table to store Step Function Token.
- DynamodbTargetTable– Target DynamoDB table name

## Deployed Artifacts:

## Lambda functions -

This stack will deploy 4 lambda functions

1. **RedshiftUnloadFunction: -** This function invoked by step function to send unload SQL statement to Redshift. This function invokes Redshift Data API asynchronously and save the token provided by Step Function into DynamoDB metadata table.
2. **StepFunctionCallBack** :- This function invoked by event bridge event rule upon completion of redshift unload command, this function retrieves task token from DynamoDB and pass it back to step function to resume the process.
3. **RedshiftUnloadS3ListFunction** : - This function uses [awswrangler](https://github.com/awslabs/aws-data-wrangler) python library to list all files unloaded into python array list and pass this payload to next step in step function.
4. **LoadDynamoFunction** : This function invoked in parallel by step function map state to load data into DynamoDB.

## Lambda Layers -

This stack will deploy 2 lambda layers.

1. **AwsDataWranglerLayer :** This layer installs awswrangle python library. [awswrangler](https://github.com/awslabs/aws-data-wrangler) is [AWS Professional Service](https://aws.amazon.com/professional-services) open source initiative
2. **AwsLambdaPowertoolsPythonLayer :** This layer uses [AWS Lambda Power tools](https://github.com/awslabs/aws-lambda-powertools-python) (Python), A suite of Python utilities for AWS Lambda functions to ease adopting best practices such as tracing, structured logging, custom metrics, and more

## Step Functions –

## RedshiftToDynamoProcesStateMachine: This state machine is used for orchestration of various AWS services discussed in this bog. [AWS Step Functions](https://aws.amazon.com/step-functions/?step-functions.sort-by=item.additionalFields.postDateTime&amp;step-functions.sort-order=desc) is a serverless function orchestrator that makes it easy to sequence AWS Lambda functions and multiple AWS services into business-critical applications.

## DynamoDB tables –

1.
## DynamoDbTableMeta : - This table is used for storing and retrieving step function call back token before and after the unload.For additional information about call back token refer to this[link](https://docs.aws.amazon.com/step-functions/latest/dg/connect-to-resource.html)
2.
## DynamoDbTableTarget:- Target table to load data from redshift.

##


## EventBridge Rule –

1. **EventBridgeRedshiftEventRule** : Amazon EventBridge rule, to automatically capture redshift unload completion event, generated by redshift unload command. This triggers call back Lambda function again with unload status.

##


# Testing the code:

1. After setting up above solution, you should have a step function as shown below, you can manually start the step function by passing the below payload. You can setup Amazon EventBridge scheduled rule&#39;s cron expression to run this workflow at fixed intervals . You may view Amazon CloudWatch logs and troubleshoot issues if any in the lambda function fails. Below is an example of the execution logs for reference:

![](RackMultipart20210506-4-u57ayk_html_fe5075302f823ec3.png)

Upon successful run your step function should look as shown below.

![](RackMultipart20210506-4-u57ayk_html_4cb1f3d1573ca7ad.png)

1. You could also view DynamoDB table to validate if the data is moved from redshift into DynamoDB table.

2. Any failures this process send input payload message to dead letter queue, you can subscribe SNS topic to dead letter queue and receive the notification email in case of any failures.

**Conclusion**

Step Functions is a serverless orchestration and Amazon Redshift Data API enables you to painlessly interact with Amazon Redshift and enables you to build event-driven and cloud native applications. We demonstrated how to build an event driven application that can push billions of rows from Redshift into DynamoDB. To learn more about Amazon Redshift Data API, please visit the [documentation](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api.html).

**Security**

**License**