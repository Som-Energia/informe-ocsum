

try:
	from dbconfig import psycopg as config
except ImportError:
	config=None


from namespace import namespace as ns
def fetchNs(cursor):
	"""Wraps a database cursor so that instead of providing data
	as arrays, it provides objects with attributes named
	as the query column names."""
	fields = [column.name for column in cursor.description]
	for row in cursor:
		yield ns(zip(fields, row))
	raise StopIteration

def nsList(cursor) :
	return [e for e in fetchNs(cursor) ]

def csvTable(cursor) :
	fields = [column.name for column in cursor.description]
	return '\n'.join('\t'.join(str(x) for x in line) for line in ([fields] + cursor.fetchall()) )



