import requests
import zipfile
import os
import json
import csv
import boto3
from datetime import datetime

from botocore.config import Config

# Constants
GITHUB_TOKEN = os.getenv('MY_GITHUB_TOKEN')
REPO_OWNER = 'suhas-cog'
REPO_NAME = 'python-script'
ARTIFACT_NAME = 'jmeter-html-reports'
S3_BUCKET_NAME = 'github-csv'
S3_KEY = 'Performance_test'


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
 
# Step 4: Convert JSON to CSV
def json_to_csv(json_file, csv_file):
    if not os.path.exists(json_file):
        raise Exception(f"JSON file not found: {json_file}")
    
    with open(json_file, 'r') as f:
        data = json.load(f)
    
    # Debugging: Print the contents of the JSON data
    print("Contents of the JSON data:")
    print(json.dumps(data, indent=4))  # Pretty-print the JSON data
    
    if not isinstance(data, dict) or not data:
        raise Exception("JSON data is not a dictionary or is empty.")
    
    # Define the CSV headers
    headers = [
        "Label", "Samples", "errorCount", "Error %", "Average", "Min", "Max",
        "90% Line", "95% Line", "99% Line", "throughput", "receivedKBytesPerSec", "sentKBytesPerSec"
    ]
    
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)  # Write CSV header
        
        for key, value in data.items():
            row = [
                key,
                value.get("sampleCount", ""),
                value.get("errorCount", ""),
                value.get("errorPct", ""),
                value.get("meanResTime", ""),
                value.get("minResTime", ""),
                value.get("maxResTime", ""),
                value.get("pct1ResTime", ""),
                value.get("pct2ResTime", ""),
                value.get("pct3ResTime", ""),
                value.get("throughput", ""),
                value.get("receivedKBytesPerSec", ""),
                value.get("sentKBytesPerSec", "")
            ]
            writer.writerow(row)
    
    print("JSON converted to CSV successfully.")

    with open(csv_file, 'r') as csv_file_obj:
        csv_reader = csv.reader(csv_file_obj)
        rows = list(csv_reader)

        # Find the row with 'Total' in the first column
        total_row = None
        for i, row in enumerate(rows):
            if row[0] == 'Total':
                total_row = rows.pop(i)
                break

        # Append the 'Total' row at the end if it was found
        if total_row:
            rows.append(total_row)

        # Write the updated rows back to the CSV file
        with open(csv_file, 'w', newline='') as csv_file_obj:
            csv_writer = csv.writer(csv_file_obj)
            csv_writer.writerows(rows)


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
    
    json_file = 'artifact/statistics.json'  # Update with the actual JSON file path inside the unzipped directory
    csv_file = 'output1.csv'
    
    json_to_csv(json_file, csv_file)

    if not os.path.exists(csv_file):
        raise Exception(f"csv file not found after conversion : {csv_file}")
    
    # Get the current date and time
    current_time = datetime.now().strftime('%Y%m%d%H%M%S')
    s3_key = f"{S3_KEY}{current_time}.csv"
    
    upload_to_s3(csv_file, S3_BUCKET_NAME, S3_KEY)

if __name__ == '__main__':
    main()