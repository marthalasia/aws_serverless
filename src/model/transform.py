'''
Fetch messages from SQS Queue which we assume are in json format:

{   bucket: <input-data-bucket>,
    key: <input-data-key>,
    output_bucket: <storage-bucket>,  # Location to write the data to
    output_key: <storage-key>
}
'''


import json
import io
import time
from datetime import datetime
from pathlib import Path
import pandas as pd
from src.model import ModelWrapper
from src.cloudhelper import open_s3_file, write_s3_file


class BatchTransformJob:

    def __init__(self, sqs, queue_name):
        self.queue = sqs.get_queue_by_name(QueueName=queue_name)
        self.messages = None
        self.modelwrapper = ModelWrapper()

    def fetch_messages(self):
        self.messages = self.queue.receive_messages()
        return self.messages

    def process_queue(self):
        for message in self.messages:
            m = json.loads(message.body)

            print("Downloading key : {0} form bucket: {1}".format(m["key"], m["bucket"]))

            file = open_s3_file(m["bucket"], m["key"])
            df = pd.read_csv(file)

            print("Invoked with {0} records".format(df.shape[0]))

            predictions = self.modelwrapper.predict(df)

            file = io.StringIO()
            predictions.to_csv(file, index=False)
            key_ = Path(str(m["output_key"]) + datetime.now().strftime("%d-%m-%Y") + str(int(time.time())) + ".csv")

            if write_s3_file(bucket=m["output_bucket"], key=key_, file=file):
                print("Success, delete message.")
                message.delete()
