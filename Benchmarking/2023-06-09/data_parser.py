import re
import pandas as pd
import xlsxwriter

with pd.ExcelWriter('Benchmarking/2023-06-09/Benchmarks.xlsx', engine='xlsxwriter') as writer:
	with open('Benchmarking/2023-06-09/AllTests.txt', 'r') as f:
		# Read in as text
		file_string = f.read()
		tests = file_string.split('STARTING SBITS VARIABLE DATA TESTS.\n')

		for test in tests[1:]:
			# Parse out test info
			info = re.findall(r'KEY_SIZE: (\d+).*VAR_DATA_SIZE: (\d+).*STORAGE_TYPE: ([\w ]+).*DATASET: ([\w \.]+)', test, re.DOTALL)[0]

			# Parse out stat table
			text_tables = re.findall(r'(Stats for 10000:.*?)(?=\n\nSTARTING SBITS|$)', test, re.DOTALL)
			text_tables = [x.strip() for x in text_tables]

			# Regex pattern to match each row of the table
			pattern = r"([\w| ]+):\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)|(Stats for \d+:)"

			for text_table in text_tables:
				# Extract the matches
				text_rows = re.findall(pattern, text_table)

				# Create a list to store the rows of the table
				table_rows = []

				# Iterate over the matches and create rows
				for text_row in text_rows:
					if text_row[-1] != '':
						table_rows.append([text_row[-1]] + ['' for num in text_row[1:-1]])
					else:
						table_rows.append([text_row[0]] + [int(num) for num in text_row[1:-1]])

				# Create the Pandas DataFrame
				columns = ["Stats", "Run1", "Run2", "Run3", "Avg"]
				df = pd.DataFrame(table_rows, columns=columns)

				# Create sheet name
				key_size = info[0]
				vardata_size = info[1]
				storage_type = info[2]
				dataset = info[3]
				if storage_type == 'Dataflash':
					storage_type = 'df'
				elif 'old' in storage_type.lower():
					storage_type = 'oldSD'
				else:
					storage_type = 'newSD'
					
				if 'ethylene' in dataset:
					dataset = 'ethylene'
				elif 'sea100K' in dataset:
					dataset = 'sea100K'
				elif 'uwa500K' in dataset:
					dataset = 'uwa500K'

				sheet_name = f'{storage_type}_{dataset}_key={key_size}_var={vardata_size}'

				# Write to excel workbook
				df.to_excel(writer, sheet_name=sheet_name, index=False)
