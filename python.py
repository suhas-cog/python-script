import requests
import zipfile
import os
import json
import csv
import boto3

from botocore.config import Config

# Constants
GITHUB_TOKEN = os.getenv('MY_GITHUB_TOKEN')
REPO_OWNER = 'suhas-cog'
REPO_NAME = 'python-script'
ARTIFACT_NAME = 'jmeter-html-reports'
S3_BUCKET_NAME = 'github-csv'
S3_KEY = 'output.csv'


def get_artifact_id():
    url = f'https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/artifacts'
    headers = {'Authorization': f'token {GITHUB_TOKEN}'}
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to fetch artifacts: {response.status_code} {response.text}")
    
    response_json = response.json()
    
    if 'artifacts' not in response_json:
        raise Exception(f"'artifacts' key not found in the response: {response_json}")
    
    artifacts = response_json['artifacts']
    for artifact in artifacts:
        if artifact['name'] == ARTIFACT_NAME:
            return artifact['id']
    
    raise Exception('Artifact not found')

# Step 2: Download the artifact from GitHub
def download_artifact(artifact_id):
    url = f'https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/artifacts/{artifact_id}/zip'
    headers = {'Authorization': f'token {GITHUB_TOKEN}'}
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to download artifact: {response.status_code} {response.text}")
    
    with open('artifact.zip', 'wb') as f:
        f.write(response.content)

# Step 3: Unzip the downloaded artifact
def unzip_artifact():
    with zipfile.ZipFile('artifact.zip', 'r') as zip_ref:
        zip_ref.extractall('artifact')

# Step 4: Convert JSON to CSV
def json_to_csv(json_file, csv_file):
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(data[0].keys())  # Write CSV header
        for row in data:
            writer.writerow(row.values())

# Step 5: Upload the CSV to AWS S3
def upload_to_s3(file_name, bucket, key):
    s3 = boto3.client('s3', config=Config(region_name='us-west-2'))
    s3.upload_file(file_name, bucket, key)

# Main function
def main():
    try:
        artifact_id = get_artifact_id()
        download_artifact(artifact_id)
        unzip_artifact()
        
        json_file = 'artifact/statistics.json'  # Update with the actual JSON file path
        csv_file = 'output.csv'
        
        json_to_csv(json_file, csv_file)
        upload_to_s3(csv_file, S3_BUCKET_NAME, S3_KEY)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    main()
# Step 1: Get the artifact ID from GitHub
# def get_artifact_id():
#     url = f'https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/artifacts'
#     headers = {'Authorization': f'token {GITHUB_TOKEN}'}
#     response = requests.get(url, headers=headers)
#     artifacts = response.json()['artifacts']
#     for artifact in artifacts:
#         if artifact['name'] == ARTIFACT_NAME:
#             return artifact['id']
#     raise Exception('Artifact not found')

# # Step 2: Download the artifact from GitHub
# def download_artifact(artifact_id):
#     url = f'https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions/artifacts/{artifact_id}/zip'
#     headers = {'Authorization': f'token {GITHUB_TOKEN}'}
#     response = requests.get(url, headers=headers)
#     with open('artifact.zip', 'wb') as f:
#         f.write(response.content)

# # Step 3: Unzip the downloaded artifact
# def unzip_artifact():
#     with zipfile.ZipFile('artifact.zip', 'r') as zip_ref:
#         zip_ref.extractall('artifact')

# # Step 4: Convert JSON to CSV
# def json_to_csv(json_file, csv_file):
#     with open(json_file, 'r') as f:
#         data = json.load(f)
    
#     with open(csv_file, 'w', newline='') as f:
#         writer = csv.writer(f)
#         writer.writerow(data[0].keys())  # Write CSV header
#         for row in data:
#             writer.writerow(row.values())

# # Step 5: Upload the CSV to AWS S3
# def upload_to_s3(file_name, bucket, key):
#     s3 = boto3.client('s3')
#     s3.upload_file(file_name, bucket, key)

# # Main function
# def main():
#     artifact_id = get_artifact_id()
#     download_artifact(artifact_id)
#     unzip_artifact()
    
#     json_file = 'jmeter-html-reports/statistics.json'  # Update with the actual JSON file path
#     csv_file = 'output.csv'
    
#     json_to_csv(json_file, csv_file)
#     upload_to_s3(csv_file, S3_BUCKET_NAME, S3_KEY)

# if __name__ == '__main__':
#     main()