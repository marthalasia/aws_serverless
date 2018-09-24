import json
import os
from datetime import date, datetime
import boto3

DESIRED_COUNT = int(os.environ['DESIRED_COUNT'])
OUTPUT_BUCKET = os.environ['OUTPUT_BUCKET']
OUTPUT_KEY = os.environ['OUTPUT_KEY']
SQS_NAME = os.environ['RESOURCE_NAME']
ECS_CLUSTER = os.environ['RESOURCE_NAME']
TASK_DEFINITION = os.environ['RESOURCE_NAME']
SUBNET = os.environ['SUBNET']
SECURITY_GROUP = os.environ['SECURITY_GROUP']

s3 = boto3.resource('s3')
sqs = boto3.resource('sqs')
ecs_client = boto3.client('ecs')


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    return TypeError("Type %s is not serializable." % type(obj))


def lambda_handler(event, context):
    event_info = event["Records"][0]["s3"]
    queue = sqs.get_queue_by_name(QueueName=SQS_NAME)
    message = json.dumps(dict(
        bucket=event_info['bucket']['name'],
        key=event_info['object']['key'],
        output_bucket=OUTPUT_BUCKET,
        output_key=OUTPUT_KEY))

    print("Add {0} message to queue".format(message))
    response = queue.send_message(MesageBody=message)

    if len(ecs_client.list_services(cluster=ECS_CLUSTER)["serviceArns"]) == 0:
        print("Running ECS task.")

        response = ecs_client.run_task(
            cluster=ECS_CLUSTER,
            taskDefinition=TASK_DEFINITION,
            count=DESIRED_COUNT,
            launchType='FARGATE',
            networkConfiguration=dict(
                awsvpcConfiguration=dict(subnets=[SUBNET],
                                         securityGroups=[SECURITY_GROUP],
                                         assignPublicIp='ENABLED')

            ),
        )
    else:
        print("ECS cluster already running, lay back.")

    result = {"status code": 200, "message": json.dumps(response, default=json_serial)}
    return result
