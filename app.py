import time
import pymongo
import csv
import re
import logging
import itertools
import os.path

logger = logging.getLogger(__name__)
logging.basicConfig(
	filename='lab4.log', 
	level=logging.INFO, 
	format='%(asctime)s - %(message)s'
)

client = pymongo.MongoClient('mongodb://localhost:27017')
db = client['database']
col = db['zno_opendata']


def is_num(var):
	var_temp = var.replace(',', '.')
	pattern = r"^-?\d+(\.\d+)?$"
	if re.match(pattern, var_temp):
		return True
	else:
		return False

def make_log(message, _time=time.time()):
	status = (message)
	logger.info(status)
	print(time.ctime(_time) + ' - ' + status)

def write_status(file, batches, batch_size, finished, new=False):
	with open('status.txt', 'r+') as status:
		row = int(file[5:9])-2019+1
		if new:
			status.write('OUTCOME(dynamic):\n')
			status.write('Odata2019File.csv-0-50-0\n')
			status.write('Odata2020File.csv-0-50-0\n')
			status.write('\n')
			status.write('EXPECTED RESULT (lines 2, 3):\n')
			status.write('Odata2019File.csv-7076-50-1\n')
			status.write('Odata2020File.csv-7585-50-1\n')
			status.write('\n')
			status.write('STARTING VALUES (lines 2, 3):\n')
			status.write('Odata2019File.csv-0-50-0\n')
			status.write('Odata2020File.csv-0-50-0\n')
			status.write('\n')
			status.write('FORMAT:\n')
			status.write('file name - exported batches - batch size - finished(int(bool))')
		else:
			lines = status.read().rstrip('\n').split('\n')
			lines[row] = f'{file}-{batches}-{batch_size}-{int(finished)}\n'
			status.seek(0)
			for line in lines:
				line = line.strip('\n')
				status.write(line+'\n')
			status.seek(0)

numeric_cols = []
def prepare_table(file, table='zno_opendata'):
	with open(file, 'r', encoding='cp1251') as csvfile:
		header = csvfile.readline().rstrip('\n').split(';')
		for word in header:
			i = header.index(word)
			word = word.strip('"')
			header[i] = word
		
	with open(file, 'r', encoding='cp1251') as csvfile:
		csv_reader = csv.DictReader(csvfile, delimiter=';')
		for row in csv_reader:
			example_data = list(row.values())
			break

		while 'null' in example_data:
			i = []
			for e in range(len(example_data)):
				if example_data[e] == 'null':
					i.append(e)
			for row in csv_reader:
				temp_data = list(row.values())
				break
			for e in range(len(temp_data)):
				if e in i and temp_data[e] != 'null':
					example_data[e] = temp_data[e]

		for word in header:
			i = header.index(word)
			if is_num(example_data[i]):
				numeric_cols.append(header.index(word))
		make_log(f'PREPARED: table {table} from {file}')

		return header, example_data

def populate_table(file, table='zno_opendata'):
	start_time = time.time()

	with open(file, 'r', encoding='cp1251') as csvfile:
		make_log(f'POPULATE START: file {file}', start_time)
		csv_reader = csv.DictReader(csvfile, delimiter=';')
		batch_size = 50
		batches = 0
		finished = False
		if os.path.isfile('status.txt'):
			if col.estimated_document_count() == 0:
				make_log(f'COLLECTION EMPTY: created status.txt')
				write_status(file, batches, batch_size, finished, True)
			elif os.path.getsize('status.txt') == 0:
				make_log(f'STATUS.TXT EMPTY: collection recreated')
				db.drop_collection(col)
				write_status(file, batches, batch_size, finished, True)
		else:
			make_log(f'STATUS.TXT NOT FOUND: collection recreated')
			db.drop_collection(col)
			with open('status.txt', 'w') as status:
				pass
			write_status(file, batches, batch_size, finished, True)

		while not finished:
			try:
				year = int(file[5:9])
				query = list()
				n = 0

				with open('status.txt', 'r') as status:
					status.readline()
					f, bt, bs, fin = status.readline().split('-')
					while f!=file:
						f, bt, bs, fin = status.readline().split('-')
					bt = int(bt)
					bs = int(bs)
					fin = bool(int(fin))
					if fin:
						make_log(f'TABLE EXIST: skipping file {f}')
						return
					elif bt!=0:
						make_log(f'VALUES EXIST: skipping lines 0-{bt*bs} of file {f}')
						csv_reader = itertools.islice(csv.DictReader(csvfile, delimiter=';'), bt * bs, None)
						batches = bt


				for row in csv_reader:
					n += 1
					for key in row:
						if row[key] == 'null':
							pass
						elif is_num(row[key]):
							row[key] = row[key].replace(',', '.')
							row[key] = float(row[key])
						elif row[key]:
							row[key] = row[key].replace("'", "")
					query.append(dict(zip(header, [int(year)] + list(row.values()))))

					if n == batch_size:
						n = 0
						test_limit = 8000 #400,000 / 50
						if batches == test_limit:
							return
						if not batches % 1000:
							make_log(f'BATCHES INSERTED: {batches}')
							make_log(f'VALUES INSERTED: {(batches*batch_size) + n}')
						try:
							col.insert_many(query)
						except:
							pass
						batches += 1
						query = list()
						write_status(file, batches, batch_size, finished)
					
				if n != 0:
					try:
						col.insert_many(query)
					except:
						pass
					finished = True
					write_status(file, batches, batch_size, finished)
					make_log(f'BATCHES INSERTED: {batches}')
					make_log(f'VALUES INSERTED: {(batches*batch_size) + n}')
					n = 0

			except Exception as e:
				make_log(f'EXCEPTION UNKNOWN: {e}')
				return
	end_time = time.time()
	make_log(f'POPULATE FINISHED: file {file}', end_time)
	make_log(f'TOTAL TIME SPENT ON {file}: {end_time-start_time:.4f} seconds')

	return

def select_table(mark_type, subject, table='zno_opendata'):
	try:
		result = col.aggregate([
			{
				'$match': {
					f'{subject}TestStatus': 'Зараховано'
				}
			},
			{
				'$group': {
					'_id': {'region': '$REGNAME', 'year': '$year'},
					f'{mark_type}': { f'${mark_type}': f'${subject}Ball100' }
				}
			}
		])

		with open(f'{mark_type}-{subject}.csv', 'w', encoding="utf-8") as result_csvfile:
			csv_writer = csv.writer(result_csvfile)
			csv_writer.writerow(['region', 'year', f'{mark_type} mark from {subject}'])
			for k in result:
				row = [k["_id"]["region"],str(k["_id"]["year"]),str(k[mark_type])]
				csv_writer.writerow(row)
			make_log(f'SELECTED: {mark_type} mark from {subject}')
	except Exception as e:
		make_log(f'EXCEPTION UNKNOWN: {e}')


header, example_data = prepare_table('Odata2019File.csv')
header.insert(0, 'year')

populate_table('Odata2019File.csv')
populate_table('Odata2020File.csv')

select_table('avg', 'hist')
