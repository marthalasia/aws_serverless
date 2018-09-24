import io
import boto3


def open_s3_file(bucket, key):
    file = io.BytesIO()
    bucket = boto3.resource('s3').Bucket(bucket)
    bucket.Object(key).download_fileobj(file)
    file.seek(0)
    return file


def write_s3_file(bucket, key, file):
    file.seek(0)
    bucket = boto3.resource('s3').Bucket(bucket)
    bucket.Object(key).upload_fileobj(file)


def write_s3_string(bucket, key, file):
    try:
        file.seek(0)
        buffer = io.BytesIO()
        buffer.write(file.read().encode("utf-8"))
        buffer.seek(0)
        bucket = boto3.resource('s3').Bucket(bucket)
        bucket.Object(key).upload_fileobj(buffer)
        file.close()  # might cause problems as I haven't tested this
    except Exception as e:
        print('Exception: ', e)
    return True
