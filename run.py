#! /usr/bin/env python3

import os, psycopg2, random, string


DB_NAME = os.environ['PGDATABASE']
DB_USER = os.environ['PGUSER']
DB_PASS = os.environ['PGPASSWORD']
RO_DB_ENDPOINT = os.environ['RO_DB_ENDPOINT']
RW_DB_ENDPOINT = os.environ['RW_DB_ENDPOINT']


def open_connection(endpoint):
	return psycopg2.connect( host=endpoint, dbname=DB_NAME, user=DB_USER, password=DB_PASS)


if os.environ['PERSIST_CONNECTION'] == 'yes':
	PERSIST_CONNECTION = True
	RO_CONN = open_connection(RO_DB_ENDPOINT)
	RW_CONN = open_connection(RW_DB_ENDPOINT)
else:
	PERSIST_CONNECTION = False

LETTERS = string.ascii_lowercase


def generate_name(size):
    return ''.join(random.choice(LETTERS) for i in range(size))

def get_connection(mode):
	if PERSIST_CONNECTION and mode == 'read':
		return RO_CONN
	elif PERSIST_CONNECTION and mode == 'write':
		return RW_CONN
	elif not PERSIST_CONNECTION and mode == 'read':
		return open_connection(RO_DB_ENDPOINT)
	elif not PERSIST_CONNECTION and mode == 'write':
		return open_connection(RW_DB_ENDPOINT)

def do_sql(mode, name=None):
	retries = 3
	for attempt in range(5):
		try:
			conn = get_connection(mode)
			cur = conn.cursor()
			if mode == 'read':
				cur.execute( "SELECT COUNT(*) FROM ( SELECT name FROM test GROUP BY name HAVING COUNT(*) > 1 ) sq" )
				result = cur.fetchone()[0]
			elif mode == 'write':
				cur.execute( "INSERT INTO test (name, added) VALUES ('%s', now())" % name )
				conn.commit()
				cur.execute("SELECT COUNT(*) FROM test")
				result = cur.fetchone()[0]

			cur.close()
			if not PERSIST_CONNECTION:
				conn.close()
					
			return result

		except Exception as e:
			print("Got error in %s : %s" % (mode, str(e)))


i=0
while True:	
	if i % 10 == 0:
		# Do write
		mode = 'write'
		result = do_sql(mode, generate_name(4))
	else:
		# Do read
		mode = 'read'
		result = do_sql(mode)

	if i % 101 == 0:
		print("SQL mode %s, result = %d" % (mode, result))

	i = i + 1