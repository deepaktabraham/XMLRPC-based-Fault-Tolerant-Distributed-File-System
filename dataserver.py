#!/usr/bin/env python

# XMLRPC based Fault Tolerant Distributed File System - Data Server: dataserver.py

from __future__ import print_function
import sys, SimpleXMLRPCServer, getopt, pickle, threading, xmlrpclib, shelve, random
from xmlrpclib import Binary
from anydbm import error as dataStoreError
from socket import error as socketError

# Presents a persistent database with the server ID and replica ID as the keys and 
# a dictionary of their data contents as the value
class DataServer:
	def __init__(self, myID, dsID, dsPorts):
		# Data Server's ID
		self.myID = myID
		
		# Data Server's ID whose redundant data is stored in this Data Server
		self.redundantID = dsID[myID -1] 	#redundant server's data is also stored in me 
		
		# Data Server's Replica 2 ID
		self.replicaID = (myID + 1) % len(dsPorts)	

		print ("\n My ID is ", self.myID)
		print ("\n My Replica 2 Data Server's ID is ", self.replicaID)
		print ("\n Adjacent Data Server's ID storing its redundant data in me is ", self.redundantID)
	    
		# Initialize/Recover Data Server's persistent database
		self.initDataStore(dsPorts)

	# Initializes Data Server's persistent database if starting for the first time
	# Recovers Data Server's persistent database contents from adjacent servers in
	# case of a system crash
	def initDataStore(self, dsPorts):
		
		# Persistent database filename
		filename = 'datastore' + str(self.myID)
		print("\n My persistent database filename is ", filename)

		
		try:
			# Check and open persistent database file for writing, if present, after a system crash
			self.dataStore = shelve.open(filename, flag = 'w', writeback=True)
			
		except dataStoreError:
			# Create persistent database file if not present
			self.dataStore = shelve.open(filename, flag = 'c', writeback=True)
			
			# Initialize persistent database
			self.dataStore[str(self.myID)] = {}
			self.dataStore[str(self.redundantID)] = {}
			
			try:
				# Contact replica 2 Data Server and recover Data Server contents
				serverReplicaPort = "http://localhost:" + dsPorts[self.replicaID]
				serverReplica = xmlrpclib.ServerProxy(serverReplicaPort)
				did = Binary(pickle.dumps(str(self.myID))) 
				recoveredData = pickle.loads(serverReplica.recoverData(did).data)
				print ("\n Recovered my data from my Replica Data Server ")
				self.dataStore[str(self.myID)] = recoveredData
					
			except socketError:
				print("\n My Replica 2 Data Server is down ")
			
			self.dataStore.sync()		
			
			
			try:
				# Contact other adjacent Data Sever to obtain its redundant data
				serverRedundantPort = "http://localhost:" + dsPorts[self.redundantID]
				serverRedundant = xmlrpclib.ServerProxy(serverRedundantPort)
				did = Binary(pickle.dumps(str(self.redundantID)))
				recoveredData = pickle.loads(serverRedundant.recoverData(did).data)	
				print ("\n Recovered Adjacent Data Server's redundant data ")
				self.dataStore[str(self.redundantID)] = recoveredData
				
			except socketError:
				print("\n Adjacent Data Server storing its redundant data in me is down ")
			
			self.dataStore.sync()		
		
		print("\n Initial persistent database contents are ")
		print(self.dataStore)
	
	# Send data belonging to Data Server 'dsid' to respective data Server
	def recoverData(self, dsid):
		print ("\n Sending recovery data ... ")
		dsid = pickle.loads(dsid.data)
		return Binary(pickle.dumps(self.dataStore[dsid]))	

	# Retrieve my ID
	def getID(self):
		return Binary(pickle.dumps(self.myID))
	
	# Retrieve block 'blk' of data 'value' of file 'key' belonging to Data Server 'dsid' from the persistent database	
	def getDataStore(self, dsid, key, blk):
		print("\n Retrieving data from persistent database ... ")
		if (dsid in self.dataStore) and (key in self.dataStore[dsid]):
			
			# Retrieve all blocks of data present in the database for file 'key'
			if (blk == -1):
				return self.dataStore[dsid][key]
			else:
				# Retrieve requested block of data 'blk' present in the database for file 'key'
				return self.dataStore[dsid][key][blk]
		else:
			print ("\n File path not present in the persistent database ")
			return False
		
	# Insert block 'blk' of data 'value' of file 'key' belonging to Data Server 'dsid' into the persistent database	
	def putDataStore(self, dsid, key, value, blk):
		print("\n Inserting data in the persistent database ... ")
		
		# On creating a new file, intialize data to {}
		if (blk == -1):
			self.dataStore[dsid][key] = value[0]
			
		else:
			
			# If file entry is present in another dataserver but this data server does not have this entry
			if key not in self.dataStore[dsid]:
				self.dataStore[dsid][key] = {}	
				
			# Insert block of data 'blk' of file 'key'	
			self.dataStore[dsid][key][blk] = value
		
		self.dataStore.sync()
		self.printDataStore()
		return True
	
	# Remove block of data 'blk' of file 'key' belonging to Data Server 'dsid' from the persistent database	
	def remDataStore(self, dsid, key, blk):
		print("\n Removing data from persistent database ... ")
		try:
			# Remove all blocks of data of file 'key' from the persistent database
			if (blk == -1):
				self.dataStore[dsid].pop(key)
				
			else:
				# Remove block of data 'blk' of file 'key' from the persistent database
				self.dataStore[dsid][key].pop(blk)
		except KeyError:
			pass
		self.dataStore.sync()
		self.printDataStore()
		return True
	
	# Print contents of the persistent database
	def printDataStore(self):
		print("\n **** Persistent Database Contents **** ")
		print(self.dataStore)
	
	# Corrupt one random byte of data from a random block of file 'path'
	def corrupt(self, path):
		# Number of bytes to be corrupted
		nbytes = 1
		path = pickle.loads(path.data)
		did = str(self.myID)
		data = self.getDataStore(did, path, -1)
		
		if (data == False):
			# File not found corresponding to my serverID in the database. Check the redundant data stored in me.	
			did = str(self.redundantID)
			data = self.getDataStore(did, path, -1)
			
			if (data == False):
				# If file not present in database, return False
				print ("\n File not present in this Data Server. No corruption of data done. ")
				return Binary(pickle.dumps([False]))
		
		keys = data.keys()
		corruptBlk = random.choice(keys)
		print ("\n Random block number to be corrupted is ", corruptBlk)
		value = data[corruptBlk][0]
		checksum = data[corruptBlk][1]
		print ("\n Old (correct) data value is:", value)
		print ("\n Corrupting Data... ")
		corruptPos = random.randint(0, len(value)-1)
		corruptValue = value[:corruptPos] + chr(ord(value[corruptPos]) + 1) + value[corruptPos+1:]
		corruptData = (corruptValue, checksum)
		print ("\n Number of bytes corrupted is ", nbytes)
		print ("\n Random byte position corrupted in the block is ", corruptPos+1)
		print ("\n New (corrupt) data value is:", corruptValue)
		retVal = [nbytes, corruptBlk, value, corruptPos, corruptValue]
		retVal.append(self.putDataStore(did, path, corruptData, corruptBlk))
		return Binary(pickle.dumps(retVal))
			
	# Retrieve block 'blk' of data 'value' of file 'key' belonging to Data Server 'dsid'
	def get(self, dsid, key, blk):
		print ("\n Retrieving data from Data Server ... ")
		key = pickle.loads(key.data)
		blk = pickle.loads(blk.data)
		dsid = pickle.loads(dsid.data)
		
		# Retrieve data from the persistent database
		data = self.getDataStore(dsid, key, blk)
		return Binary(pickle.dumps(data))

	# Insert a block of data 'blk' of file 'key' belonging to Data Server 'dsid'
	def put(self, dsid, key, value, blk):
		print ("\n Inserting data into Data Server ... ")
		key = pickle.loads(key.data)
		value = pickle.loads(value.data)
		blk = pickle.loads(blk.data)
		dsid = pickle.loads(dsid.data)
		
		# Insert data into the persistent database
		ack = self.putDataStore(dsid, key, value, blk)
		return Binary(pickle.dumps(ack))
	

    # Remove a block of data 'blk' of file 'key' belonging to Data Server 'dsid'
	def rem(self, dsid, key, blk):
		print ("\n Removing data from Data Server ... ")		   
		key = pickle.loads(key.data)
		blk = pickle.loads(blk.data)
		dsid = pickle.loads(dsid.data)
		
		# Remove data from the persistent database		   
		ack = self.remDataStore(dsid, key, blk)	   
		return Binary(pickle.dumps(ack))

def main():
	# Check for correct arguments
	if len(sys.argv) < 4:
		print("usage: %s <0 indexed server number> <ports for all data servers seperated by spaces (minimum 2 ports)>" % sys.argv[0])
		exit(1)
				   
	# ID of this Data Server
	myID = int(sys.argv[1])
				   
	# List of all port numbers of all the Data Servers
	dsPorts = sys.argv[2:]
	
	# List of IDs of all Data Servers assuming ports were entered by user in order
	dsID = [x for x in range(len(dsPorts))]
	
	# Check whether correct data server ID entered
	if myID not in dsID:
		print("\n Incorrect Data Server ID entered ")
		exit(1)
				   
	port = int(dsPorts[myID])
	serve(port, myID, dsID, dsPorts)

# Start the xmlrpc server
def serve(port, myID, dsID, dsPorts):
	print("\n Starting Data Server ... ")
	file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
	file_server.register_introspection_functions()
	ds = DataServer(myID, dsID, dsPorts)
	file_server.register_function(ds.get)
	file_server.register_function(ds.put)
	file_server.register_function(ds.rem)
	file_server.register_function(ds.getID)
	file_server.register_function(ds.recoverData)
	file_server.register_function(ds.corrupt)
	file_server.serve_forever()

if __name__ == "__main__":
	main()
