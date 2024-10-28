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
S3_KEY = '/output.csv'


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
    
    with open('jmeter-html-reports', 'wb') as f:  # No .zip extension
        f.write(response.content)
    
    print("Artifact downloaded successfully.")

# Step 3: Unzip the downloaded artifact
def unzip_artifact():
    if not os.path.exists('jmeter-html-reports'):
        raise Exception("Artifact file not found.")
    
    with zipfile.ZipFile('jmeter-html-reports', 'r') as zip_ref:
        zip_ref.extractall('artifact')
    
    print("Artifact unzipped successfully.")
    print("Contents of the 'artifact' directory after unzipping:")
    for root, dirs, files in os.walk('artifact'):
        for name in files:
            print(os.path.join(root, name))

# Step 4: Find the JSON file inside the unzipped artifact directory
# def find_json_file(directory, filename):
#     for root, dirs, files in os.walk(directory):
#         if filename in files:
#             return os.path.join(root, filename)
#         raise Exception(f"JSON file '(filename)' not found in directory '{directory}'")
 
# Step 4: Convert JSON to CSV
def json_to_csv(json_file, csv_file):
    if not os.path.exists(json_file):
        raise Exception(f"JSON file not found: {json_file}")
    
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(data[0].keys())  # Write CSV header
        for row in data:
            writer.writerow(row.values())
    
    print("JSON converted to CSV successfully.")

# Step 5: Upload the CSV to AWS S3
def upload_to_s3(file_name, bucket, key):
    if not os.path.exists(file_name):
        raise Exception(f"CSV file not found: {file_name}")
    
    s3 = boto3.client('s3', config=Config(region_name='us-east-1'))
    s3.upload_file(file_name, bucket, key)
    
    print("CSV uploaded to S3 successfully.")

# Main function
def main():
    artifact_id = get_artifact_id()
    download_artifact(artifact_id)
    unzip_artifact()
    
    print("Contents of the 'artifact' directory:")
    for root, dirs, files in os.walk('artifact'):
        for name in files:
            print(os.path.join(root, name))
    json_file = 'artifact/statistics.json'  # Update with the actual JSON file path inside the unzipped directory
    csv_file = 'output.csv'
    
    json_to_csv(json_file, csv_file)
    
    upload_to_s3(csv_file, S3_BUCKET_NAME, S3_KEY)

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