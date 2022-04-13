# Import required libraries
import boto3
import os

# AWS credentials of Admin-User
ACCESS_KEY = "<access_key>"
SECRET_KEY = "<secret_access_key>"

# Boto3 client setup for firehose
firehose = boto3.client('firehose',
                        aws_access_key_id = ACCESS_KEY,
                        aws_secret_access_key = SECRET_KEY,
                        region_name = "<region_name>")

# Access files from the folder using FOR loop
for file in os.listdir("<Folder path in which files will be stored and will be uploaded>"):

    # If the file endswith '.json'
    if file.endswith('.json'):
        # Printing the filename
        print(file)
        # Open the file and deliver to the S3 via DeliveryStreamName
        files = open(os.path.join("<Folder path in which files will be stored and will be uploaded>", file))
        firehose.put_record(DeliveryStreamName="<delivery stream name>", Record={'Data': files.read()})
