import openpyxl
from openpyxl.styles import Font

xlwb = openpyxl.load_workbook('Benchmarking/2023-06-09/Benchmarks.xlsx')
sheets = xlwb.sheetnames

equations = [
		['Inserts', 'Queries', 'Writes', 'Buf Hits', 'Write Time', 'Insert Time', 'Read Time', 'Query Time', 'Num Reads', 'Inserts/sec', 'Queries/sec'],
		[10000, 1000, '=E3', '=E10', '=E7', '=K3/1000', '=E8', '=M3/1000', '=E9', '=G3/L3', '=H3/N3'],
		[20000, 2000, '=E13', '=E20', '=E17', '=K4/1000', '=E18', '=M4/1000', '=E19', '=G4/L4', '=H4/N4'],
		[30000, 3000, '=E23', '=E30', '=E27', '=K5/1000', '=E28', '=M5/1000', '=E29', '=G5/L5', '=H5/N5'],
		[40000, 4000, '=E33', '=E40', '=E37', '=K6/1000', '=E38', '=M6/1000', '=E39', '=G6/L6', '=H6/N6'],
		[50000, 5000, '=E43', '=E50', '=E47', '=K7/1000', '=E48', '=M7/1000', '=E49', '=G7/L7', '=H7/N7'],
		[60000, 6000, '=E53', '=E60', '=E57', '=K8/1000', '=E58', '=M8/1000', '=E59', '=G8/L8', '=H8/N8'],
		[70000, 7000, '=E63', '=E70', '=E67', '=K9/1000', '=E68', '=M9/1000', '=E69', '=G9/L9', '=H9/N9'],
		[80000, 8000, '=E73', '=E80', '=E77', '=K10/1000', '=E78', '=M10/1000', '=E79', '=G10/L10', '=H10/N10'],
		[90000, 9000, '=E83', '=E90', '=E87', '=K11/1000', '=E88', '=M11/1000', '=E89', '=G11/L11', '=H11/N11'],
		[100000, 10000, '=E93', '=E100', '=E97', '=K12/1000', '=E98', '=M12/1000', '=E99', '=G12/L12', '=H12/N12']
	]
ft = Font(bold=True)

for sheet in sheets:
	ws = xlwb[sheet]
	for rn, row in enumerate(ws['G2':'Q12']):
		for cn, cell in enumerate(row):
			cell.value = equations[rn][cn]
			if rn == 0 or cn in [0,1]:
				cell.font = ft

xlwb.save('Benchmarking/2023-06-09/Benchmarks.xlsx')
