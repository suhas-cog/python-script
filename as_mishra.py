from zipfile import ZipFile
import json
import csv

with ZipFile('ashutosh_mishra.zip', 'r') as unzipfile:
	print("====unzipfile==",unzipfile)
	for files in unzipfile.namelist():
		if files.endswith('.json'):
			jsonfile = unzipfile.open(files)
			payload = json.load(jsonfile)
			print("====files==",payload)
data = payload
print("====data=====",payload)

file = open('sample_file.csv', 'w')
csv_file_data = csv.writer(file)
head_counter = 0
print("====data====", data.keys())
# head = data['browser_action'].keys()
# csv_file_data.writerow(head)
# csv_file_data.writerow(data['browser_action'].values())

for dat in data.keys():
	print("===dat", data[dat].keys())
	if head_counter == 0:
		head = data[dat].keys()
		csv_file_data.writerow(head)
		head_counter += 1
	csv_file_data.writerow(data[dat].values())
file.close()