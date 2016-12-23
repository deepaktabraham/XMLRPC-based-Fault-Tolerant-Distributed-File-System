#!/usr/bin/env python

# Fault Tolerant Distributed File System - Data Server Corruption Simulator: corrupt.py

from __future__ import print_function
import xmlrpclib, pickle
from xmlrpclib import *
from sys import argv

# RPC Interface to simulate the corruption of data present in file 'path'
def rpcCorrupt(path):
	arg = Binary(pickle.dumps(path))
	retVal = pickle.loads((dataServer.corrupt(arg)).data)
	ack = retVal[-1]
	if (ack == True):
		nbytes = retVal[0]
		corruptBlk = retVal[1]
		oldValue = retVal[2]
		print ("\n Old (correct) data value is:", oldValue)
		print ("\n Corrupting Data... \n")
		corruptPos = retVal[3]
		corruptValue = retVal[4]
		print ("\n Random block number to be corrupted is ",corruptBlk)
		print ("\n Number of bytes corrupted is ",nbytes)
		print ("\n Random byte position corrupted in the block is ",corruptPos+1)
		print ("\n New (corrupt) data value is:", corruptValue)
		print ("\n Data successfully corrupted - ", ack)
	else:
		print ("\n Data could not be corrupted - ", ack)	
		

if __name__ == '__main__':
	# Checking correct arguments
	if len(argv) != 3 :
		print("usage: %s <port of the dataserver to be corrupted> <pathname of the file to be corrupted in the context of FUSE filesystem>" % argv[0])
		exit(1)
	
	# Get port number of the Data Server to be corrupted
	ds_port = "http://localhost:" + argv[1]
	
	# Establish connection to Data Server to be corrupted 
	dataServer = xmlrpclib.ServerProxy(ds_port)
	
	# Get file to be corrupted
	path = argv[2]
	
	print ("\n The Data Server requested to be corrupted is at port ", ds_port)
	print ("\n The file requested to be corrupted is ", path)
	
	# Corrupt requested file in requested Data Server
	rpcCorrupt(path)
	
