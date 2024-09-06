import time
import requests
from requests_aws4auth import AWS4Auth
import boto3
import os

source_opensearch_endpoint = os.getenv('SOURCE_OPENSEARCH_ENDPOINT', 'example')
destination_opensearch_endpoint = os.getenv('DESTINATION_OPENSEARCH_ENDPOINT', 'example')
region = os.getenv('AWS_REGION', 'eu-west-1')
service = 'es'
repository_name = 'example-logs-example'
snapshot_name = f"my-snapshot-{int(time.time())}"  # Creating unique snapshot name with timestamp
role_arn = os.getenv('ROLE_ARN', 'arn:aws:iam::example:role/OpenSearchSnapshotRole')
bucket_name = os.getenv('BUCKET_NAME', 'poc-aws-opensearch')

# Set up AWS credentials and authentication
session = boto3.Session()
credentials = session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

# Function to create a repository in the source OpenSearch
def create_repository_in_source():
    url = f"{source_opensearch_endpoint}/_snapshot/{repository_name}"
    payload = {
        "type": "s3",
        "settings": {
            "bucket": bucket_name,
            "region": region,
            "role_arn": role_arn
        }
    }
    r = requests.put(url, auth=awsauth, json=payload)
    print(f"Source repository creation status: {r.status_code}")
    print(r.text)

# Function to create a snapshot in the source OpenSearch
def create_snapshot_in_source():
    url = f"{source_opensearch_endpoint}/_snapshot/{repository_name}/{snapshot_name}"
    payload = {
        "indices": "app-2024.*", 
        "ignore_unavailable": True,
        "include_global_state": False
    }
    r = requests.put(url, auth=awsauth, json=payload)
    if r.status_code == 200 or r.status_code == 202:
        print(f"Snapshot creation accepted: {r.status_code}")
    else:
        print(f"Snapshot creation failed: {r.status_code}")
        print(r.text)

# Function to check the status of the snapshot in the source OpenSearch
def check_snapshot_status():
    url = f"{source_opensearch_endpoint}/_snapshot/{repository_name}/{snapshot_name}/_status"
    r = requests.get(url, auth=awsauth)
    if r.status_code == 200:
        snapshot_status = r.json()
        print("Snapshot status: ", snapshot_status)
        return snapshot_status
    else:
        print(f"Snapshot status check failed: {r.status_code}")
        print(r.text)
        return None

# Function to create a repository in the destination OpenSearch
def create_repository_in_destination():
    url = f"{destination_opensearch_endpoint}/_snapshot/{repository_name}"
    payload = {
        "type": "s3",
        "settings": {
            "bucket": bucket_name,
            "region": region,
            "role_arn": role_arn
        }
    }
    r = requests.put(url, auth=awsauth, json=payload)
    if r.status_code == 200:
        print("Repository created in destination.")
    elif r.status_code == 500 and "repository that is currently used" in r.text:
        print("Repository is currently in use. Skipping creation.")
    else:
        print(f"Destination repository creation failed: {r.status_code}")
        print(r.text)

# Function to restore the snapshot in the destination OpenSearch
def restore_snapshot_in_destination():
    url = f"{destination_opensearch_endpoint}/_snapshot/{repository_name}/{snapshot_name}/_restore"
    payload = {
        "indices": "app-2024.*",
        "include_global_state": False
    }
    r = requests.post(url, auth=awsauth, json=payload)
    if r.status_code == 200 or r.status_code == 202:
        print(f"Snapshot restore accepted: {r.status_code}")
    else:
        print(f"Snapshot restore failed: {r.status_code}")
        print(r.text)

# Execute the process
create_repository_in_source()
create_snapshot_in_source()

# Check if snapshot creation is completed before attempting to restore it
snapshot_status = None
while not snapshot_status:
    snapshot_status = check_snapshot_status()
    if snapshot_status:
        # Only proceed if snapshot is fully created
        create_repository_in_destination()
        restore_snapshot_in_destination()
    else:
        print("Waiting for snapshot creation to complete...")
        time.sleep(10)
