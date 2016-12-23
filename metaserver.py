#!/usr/bin/env python

# XMLRPC based Fault Tolerant Distributed File System - Meta Server: metaserver.py

from __future__ import print_function
import sys, SimpleXMLRPCServer, getopt, pickle, threading, xmlrpclib
from xmlrpclib import Binary


# Presents a dictionary with 'file pathname' as the key and a dictionary of the 
# metadata of the given file as the value
class MetaServer:
	def __init__(self):
		self.data = {}

	# Retrieve metadata of file 'key' from the Meta Server
	def get(self, key, getPaths):
		print ("\n Retrieving meta data ... ")
		rv = {}                     # Default return value
		getPaths = pickle.loads(getPaths.data)
		
		# Retrieve required metadata.
		if (getPaths != 1):	
			key = pickle.loads(key.data)
			if key in self.data:
				rv = self.data[key]
		
		# Retrieve all file pathnames
		else:
			rv = self.data.keys()
		self.print_content()
		return Binary(pickle.dumps(rv))


	# Insert metadata 'value' into the Meta Server for file 'key' 
	def put(self, key, value):
		print ("\n Inserting data into Meta Server ... ")
		key = pickle.loads(key.data)
		value = pickle.loads(value.data)
		self.data[key] = value
		self.print_content()
		return True


	# Remove file 'key' and its contents from the Meta Server 
	def rem(self, key):
		print ("\n Removind data from Meta Server ... ")
		key = pickle.loads(key.data)
		try:
			self.data.pop(key)
		except KeyError:
			pass
		self.print_content()
		return True


	# Print the contents of the Meta Server
	def print_content(self):
		print("\n **** Meta Server Contents **** ")
		print(self.data)
		return True


def main():
	#Check for correct arguments
	if len(sys.argv) != 2:
		print("usage: %s <metaserver port>" % sys.argv[0])
		exit(1)
	msPort = int(sys.argv[1])
	serve(msPort)


# Start the xmlrpc server and register the interfaces.
def serve(port):
	print ("\n Starting Meta Server ... ")
	file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
	file_server.register_introspection_functions()
	ms = MetaServer()
	file_server.register_function(ms.get)
	file_server.register_function(ms.put)
	file_server.register_function(ms.rem)
	file_server.register_function(ms.print_content)
	file_server.serve_forever()


if __name__ == "__main__":
	main()
