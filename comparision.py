import pandas as pd
import boto3
from io import StringIO

# AWS S3 configuration
s3_bucket = 'your-s3-bucket-name'
output_file_key = 'path/to/output/TaxCal.csv'

# GitHub artifact URL
data3_url = 'https://github.com/your-repo/your-artifact-path/result1.jtl'

# Initialize S3 client
s3_client = boto3.client('s3')

# Function to get the latest and second latest file based on last modified time
def get_latest_s3_keys(prefix):
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
    files = response.get('Contents', [])
    files.sort(key=lambda x: x['LastModified'], reverse=True)
    return files[0]['Key'] if len(files) > 0 else None, files[1]['Key'] if len(files) > 1 else None

# Get the latest and second latest current release files
current_release_key, previous_release_key = get_latest_s3_keys('path/to/current/TaxCal_CurrentRelease')

# Read current release data from S3
current_release_obj = s3_client.get_object(Bucket=s3_bucket, Key=current_release_key)
df1 = pd.read_csv(current_release_obj['Body'])

# Read previous release data from S3
previous_release_obj = s3_client.get_object(Bucket=s3_bucket, Key=previous_release_key)
df2 = pd.read_csv(previous_release_obj['Body'])

# Read data3 from GitHub artifact URL
df_result = pd.read_csv(data3_url)

# Data processing steps
response_500_counts = df_result[df_result['responseCode'] == '500'].groupby('label').size().reset_index(name='count_500')
response_501_counts = df_result[df_result['responseCode'] == '501'].groupby('label').size().reset_index(name='count_501')
response_502_counts = df_result[df_result['responseCode'] == '502'].groupby('label').size().reset_index(name='count_502')
response_503_counts = df_result[df_result['responseCode'] == '503'].groupby('label').size().reset_index(name='count_503')

all_labels = pd.DataFrame({'label': df_result['label'].unique()})
all_labels_with_500 = pd.merge(all_labels, response_500_counts, on='label', how='left', validate="many_to_many").fillna({'count_500': 0})
all_labels_with_501 = pd.merge(all_labels, response_501_counts, on='label', how='left', validate="many_to_many").fillna({'count_501': 0})
all_labels_with_502 = pd.merge(all_labels, response_502_counts, on='label', how='left', validate="many_to_many").fillna({'count_502': 0})
all_labels_with_503 = pd.merge(all_labels, response_503_counts, on='label', how='left', validate="many_to_many").fillna({'count_503': 0})

new_df1 = pd.DataFrame({
    'Label': all_labels_with_500['label'],
    '500 Error Count': all_labels_with_500['count_500'],
    '501 Error Count': all_labels_with_501['count_501'],
    '502 Error Count': all_labels_with_502['count_502'],
    '503 Error Count': all_labels_with_503['count_503']
})

new_df1['Sum of 5xx Error Count'] = new_df1['500 Error Count'] + new_df1['501 Error Count'] + new_df1['502 Error Count'] + new_df1['503 Error Count']
new_df1.drop(columns=['500 Error Count', '501 Error Count', '502 Error Count', '503 Error Count'], inplace=True)

new_df1 = pd.merge(new_df1, df2[['Label', '90% Line']], on='Label', how='left', validate="many_to_one")
new_df1.rename(columns={'90% Line': '90% Line Previous Release'}, inplace=True)
new_df1 = pd.merge(new_df1, df1[['Label', '90% Line']], on='Label', how='left', validate="many_to_one")
new_df1.rename(columns={'90% Line': '90% Line Current Release'}, inplace=True)

new_df1['90% Line Difference'] = new_df1['90% Line Previous Release'] - new_df1['90% Line Current Release']
new_df1['% Response Time Deviation'] = (new_df1['90% Line Difference'] / new_df1['90% Line Current Release']) * 100
new_df1['Status'] = new_df1.apply(lambda row: 'PASS' if (row['Sum of 5xx Error Count'] == 0 and row['% Response Time Deviation'] >= -10) else 'FAIL', axis=1)

total_error_value = float(df1['Error %'].iloc[-1].replace('%', ''))
error_count_sum = new_df1['Sum of 5xx Error Count'].sum()
status = 'FAIL' if (total_error_value > 1 and error_count_sum > 1) else 'PASS'

new_df1 = new_df1.round(2)

# Save the DataFrame to a CSV file in memory
csv_buffer = StringIO()
new_df1.to_csv(csv_buffer, index=False)

# Upload the CSV file to S3
s3_client.put_object(Bucket=s3_bucket, Key=output_file_key, Body=csv_buffer.getvalue())