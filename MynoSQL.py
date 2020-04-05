# import the module
from __future__ import print_function
import aerospike
from aerospike import predicates as p
import logging


# Configure the client
config = {
  'hosts': [('127.0.0.1',3000)]
}

# Create a client and connect it to the cluster
try:
  client = aerospike.client(config).connect()
except:
  import sys
  print("failed to connect to the cluster with", config['hosts'])
  sys.exit(1)

#Create index test.phone
try:
	index = client.info('sindex/test/phone')
except Exception as e:
	client.index_integer_create('test','phone','phone','phone')

def add_customer(customer_id, phone_number, lifetime_value):
	key = ('test','phone', customer_id)
	client.put(key, {'phone': phone_number, 'ltv': lifetime_value},policy={'key': aerospike.POLICY_KEY_SEND})

def get_ltv_by_id(customer_id):
	try:
		item = ('test','phone', customer_id)
		(key, metadata, record) = client.get(item)
		return record.get('ltv')
	except Exception as e:
		logging.error('Requested non-existent customer ' + str(customer_id))

def get_ltv_by_phone(phone_number):
	query = client.query('test', 'phone')
	query.select('ltv')
	query.where(p.equals('phone', phone_number))
	rec = query.results()
	if rec:
		return rec[0][2]['ltv']
	else:
		logging.error('Requested phone number is not found ' + str(phone_number))

for i in range(0,15):
    add_customer(i,i,i + 1)

for i in range(0,16):
	get_ltv_by_id(i)
	get_ltv_by_phone(i)

client.close()
