from src.model.transform import *
from src.model import *
import os
import boto3


def run_batch_transform_job():
    sqs = boto3.resource('sqs', region_name=ModelWrapper().config['AWS_REGION'])
    btjob = BatchTransformJob(sqs, os.environ["SQS_QUEUE"])

    t0 = time.time()
    btjob.fetch_messages()

    count = 0
    while len(btjob.messages) == 0:
        count += 1
        coffee_break = 2**count
        print("No messages ready, back off for {0} seconds.".format(coffee_break))

        time.sleep(coffee_break)
        btjob.fetch_messages()

        if(time.time() - t0) > 900:
            print("Maximum time exceeded, close the container.")
            return False

        print("Found messages, processing the queue.")
        btjob.process_queue()
