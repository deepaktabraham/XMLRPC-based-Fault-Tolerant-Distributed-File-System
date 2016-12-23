#!/usr/bin/env python

# XMLRPC based Fault Tolerant Distributed File System - File System Client: distributedFS.py

from __future__ import print_function, absolute_import, division

import logging
import xmlrpclib
import pickle
import hashlib

from xmlrpclib import *
from errno import ENOENT, ENOTEMPTY, ECONNREFUSED
from stat import S_IFDIR, S_IFLNK, S_IFREG, S_ISDIR, S_ISREG, S_ISLNK
from sys import argv, exit
from time import time
from math import ceil, floor
from socket import error as socketError

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

# RPC Interface to retrieve the ID of Data Server 'server'
def rpcGetID(server):
	
	print ("\n RPC call to retrieve the Data Server ID initiated ")
	# Unmarshal and return the received ID
	return pickle.loads((server.getID()).data)

# RPC Interface to retrieve metadata of file 'key' from the Meta Server
def rpcGetMeta(key, getPaths = 0):
	key = str(key)
	
	# Marshal arguments for transmission
	key = Binary(pickle.dumps(key))
	getPaths = Binary(pickle.dumps(getPaths))
	
	print ("\n RPC call to retrieve metadata initiated ")
	# Unmarshal and return the received metadata
	return pickle.loads(metaServer.get(key, getPaths).data)

# RPC Interface to read block of data 'blk' of file 'key'
def rpcGetData(key, blk = -1):
	key = str(key)
	
	# Find the Data Server and its ID from which the data has to be read
	did, server = getDataServer(key, blk)
	
	# Find the Replica Data Server and its ID from which the redundant data has to be read
	didReplica, serverReplica = getReplicaServer(key, blk)
	
	# Marshal arguments for transmission
	key = Binary(pickle.dumps(key))
	blk = Binary(pickle.dumps(blk))
	did = Binary(pickle.dumps(str(did)))
	
	# Fault tolerance for server crashes and data corruption
	# Retrieve data from Main Data Server
	try:
		print ("\n RPC call to read data from main Data Server ", pickle.loads(did.data), " initiated ")
		
		# Retrieve and unmarshal data from main Server 
		data = pickle.loads(server.get(did, key, blk).data)
		print ("\n Retrieved data from Main Data Server ", pickle.loads(did.data))
		
		# Set flag = 1 if data successfully retrieved from main data server only
		foundMainServer = 1
	
	# Retrieve data from Replica Data Server if the main server has crashed
	except socketError:
		
		print ("\n Could not retrieve data from Main Data Server ",pickle.loads(did.data),". Main Data Server ", pickle.loads(did.data)," is down. ")
		print ("\n RPC call to retrieve data from Replica Data Server ", didReplica, " initiated ")
		
		# Retrieve and unmarshal data from Main Server 
		data = pickle.loads(serverReplica.get(did, key, blk).data)
		print ("\n Retrieved data from Replica Data Server ", didReplica)
		
		# Set flag = 0 if data successfully retrieved from the Replica Data Server only
		foundMainServer = 0
	
	# Code segment to handle data corruption
	finally:
		
		# Retrieve Data from Replica Server also once data has been retrieved from Main Data Server
		if (foundMainServer == 1):
			
			# Retrieve data from Replica Data Server and handle data corruption
			try:
				# Retrieve and unmarshal data from the Replica Server 
				print ("\n RPC call to retrieve data from Replica Data Server ", didReplica, " initiated ")
				replicaData = pickle.loads(serverReplica.get(did, key, blk).data)
				print ("\n Retrieved data from Replica Data Server ", didReplica)
				
				dataValue = data[0]
				replicaDataValue = replicaData[0]
				dataChecksum = data[1]
				replicaDataChecksum = replicaData[1]
		
				# If data retireved from Main Data Server and Replica Data Server are not the same
				if (dataValue != replicaDataValue):
					
					# If data obtained from Replica Data Server is corrupt, write correct data from Main Data Server into Replica Data Server
					if (dataChecksum == hashlib.md5(dataValue).hexdigest()):
						print ("\n Data is correct in main data server ",pickle.loads(did.data))
						print ("\n Data is corrupted in replica data server ",didReplica)
						
						correctData = dataValue
						correctChecksum = dataChecksum
						correctValue = (correctData, correctChecksum)
						
						# Marshal the correct data for transmission
						correctValue = Binary(pickle.dumps(correctValue))
						
						# Write correct data into the Replica Data Server
						print ("\n Writing correct data back into the Replica Data Server ")
						serverReplica.put(did, key, correctValue, blk)
						
						# Return correct data
						return correctData
					
					# If data obtained from Main Data Server is corrupt, write correct data from Replica Data Server into Main Data Server
					else:
						print ("\n Data is correct in replica data server ",didReplica)
						print ("\n Data is corrupted in main data server ",pickle.loads(did.data))
						
						correctData = replicaDataValue
						correctChecksum = replicaDataChecksum
						correctValue = (correctData, correctChecksum)
						
						# Marshal the correct data for transmission
						correctValue = Binary(pickle.dumps(correctValue))
						
						# Write correct data into the Main Data Server
						server.put(did, key, correctValue, blk)
						
						# Return correct data
						return correctData
				
				# If data retireved from Main Data Server and Replica Data Server same, return data
				else:
					print ("\n No data corruption in either server ")
					return dataValue
			
			# If Main Data Server is running but Replica Data Server is down, cannot handle data corruption
			except socketError:
				print ("\n Data Corruption cannot be handled. Could not get data from Replica Data Server ",didReplica)
				return data[0]
		
		# If Replica Data Server is running but Main Data Server is down, cannot handle data corruption
		else:
				print ("\n Data Corruption cannot be handled. Could not get data from Main Data Server ",pickle.loads(did.data))
				return data[0]

# RPC Interface to write metadata of file 'key' into the Meta Server			
def rpcPutMeta(key, value):
	key = str(key)
	
	# Marshal arguments for transmission
	key = Binary(pickle.dumps(key))
	value = Binary(pickle.dumps(value))
	
	print ("\n RPC call to write metadata initiated ")
	# Write the metadata of file 'key' into the Meta Server
	metaServer.put(key, value)
	
# Compute and return the Replica Data Server and its ID	
def getReplicaServer(key, blk):
	did, server = getDataServer(key, blk+1)
	return did, server

# Fault tolerance for the RPC put call to handle server crash on write operation
def faultTolerantPut(did, server, didReplica, serverReplica, key, value, blk):	

	# Acknowledgement flag to check if write operation has completed successfully
	commit = False
	printErr = False
	
	# Write data to the Main Data Server and keep retrying the write operation till it succeeds
	print ("\n RPC call to write data to the Main Data Server initiated ")
	while (commit == False):
		try:
			# Write data into the Main Data Server and get acknowledgement from Main Data Server
			commit = server.put(did, key, value, blk)
		
		except socketError:
			
			# If data cannot be written to Main Data Server, keep retrying till a positive acknowledgement is received
			if (printErr == False):
				print ("\n Error in writing data to the Main Data Server", pickle.loads(did.data), ". Data Server", pickle.loads(did.data), " is down. ")
				print ("\n Retrying ... ")
				printErr = True
			commit = False
	
	print ("\n Successfully written to the Main Data Server ", pickle.loads(did.data))
	printErr = False
	commit = False
	
	# Write data to the Replica Data Server and keep retrying the write operation till it succeeds
	print ("\n RPC call to write data to the Replica Data Server initiated ")
	while (commit == False):
		try:
			# Write data into the Replica Data Server and get acknowledgement from Replica Data Server
			commit = serverReplica.put(did, key, value, blk)
			
		except socketError:
			# If data cannot be written to Replica Data Server, keep retrying till a positive acknowledgement is received
			if (printErr == False):
				print ("\n Error in writing to Replica Data Server", didReplica, ". Data Server", didReplica, " is down. ")
				print ("\n Retrying ... ")
				printErr = True
			commit = False	
	
	print ("\n Successfully written to the Replica Data Server ", didReplica)

# RPC Interface to write block 'blk' of data 'value' of file 'key' into the Data Server
def rpcPutData(key, value, blk = -1):
	
	key = str(key)
	
	# Find the Data Server, Replica Data Server and their IDs into which the data has to be written
	# When a new file is created 
	if (blk == -1):
		did, server = getDataServer(key, 0)
		didReplica, serverReplica = getReplicaServer(key, 0)
		checksum = ''
	
	# When writing into an existing file
	else:
		did, server = getDataServer(key, blk)
		didReplica, serverReplica = getReplicaServer(key, blk)
		checksum  = hashlib.md5(value).hexdigest()
	
	value = (value, checksum)
	
	# Marshal arguments for transmission
	key = Binary(pickle.dumps(key))
	value = Binary(pickle.dumps(value))
	blk = Binary(pickle.dumps(blk))
	did = Binary(pickle.dumps(str(did)))
	faultTolerantPut(did, server, didReplica, serverReplica, key, value, blk)

# Fault tolerance for the RPC remove call
def faultTolerantRemove(did, server, didReplica, serverReplica, key, blk):
	
	# Acknowledgement flag to check if remove operation has completed successfully
	remove = False
	printErr = False
	
	# Remove data from the Main Data Server and keep retrying the remove operation till it succeeds
	print ("\n RPC call to remove data from the Main Data Server initiated ")
	while (remove == False):
		try:
			# Remove data from the Main Data Server and get acknowledgement from Main Data Server
			remove = server.rem(did, key, blk)
		except socketError:
			if (printErr == False):
				# If data cannot be removed from the Main Data Server, keep retrying till a positive acknowledgement is received
				print ("\n Error in removing data from Main Data Server", pickle.loads(did.data), ". Data Server", pickle.loads(did.data), " is down. ")
				print ("\n Retrying ... ")
				printErr = True
			remove = False

	print ("\n Data removed from the Main Data Server ", pickle.loads(did.data))
	printErr = False
	remove = False

	# Remove data from the Replica Data Server and keep retrying the remove operation till it succeeds
	print ("\n RPC call to remove data from the Replica Data Server initiated ")
	while (remove == False):
		try:
			# Remove data from the Replica Data Server and get acknowledgement from Replica Data Server
			remove = serverReplica.rem(did, key, blk)
		except socketError:
			if (printErr == False):
				# If data cannot be removed from the Replica Data Server, keep retrying till a positive acknowledgement is received
				print ("\n Error in writing to Replica Data Server", didReplica, ". Data Server", didReplica, " is down.")
				print ("\n Retrying ... ")
				printErr = True
			commit = False

	print ("\n Data removed from the Replica Data Server ", didReplica)
	
# RPC interface to remove metadata of the file 'key' from the Meta Server	
def rpcRemoveMeta(key):
	key = str(key)
	
	# Marshal arguments for transmission
	key = Binary(pickle.dumps(key))
	
	print ("\n RPC call to remove metadata initiated ")
	metaServer.rem(key)

# RPC interface to remove block of data 'blk' the file 'key' from the Data Server	
def rpcRemoveData(key, blk = -1):
	key = str(key)	
	
	# Remove all the data blocks of a file from all the Data Servers
	if (blk == -1):
		
		# Marshal Arguments for transmission
		key = Binary(pickle.dumps(key))
		blk = Binary(pickle.dumps(blk))
		
		# Find the Data Server, Replica Data Server and their IDs from which the data has to be removed
		for did, server in enumerate(data_server):
			didReplica = (did + 1) % dataServerCount
			serverReplica = data_server[ didReplica ]
			did = Binary(pickle.dumps(str(did)))
			
			# Fault tolerance to handle server crashes
			# Keep retrying the remove operation till it succeeds
			faultTolerantRemove(did, server, didReplica, serverReplica, key, blk)
	
	# Remove a single data block of a file from the Data Server
	else:
		
		# Find the Data Server, Replica Data Server and their IDs from which the data has to be removed
		did, server = getDataServer(key, blk)
		didReplica, serverReplica = getReplicaServer(key, blk)
		
		# Marshal arguments for transmission
		key = Binary(pickle.dumps(key))
		blk = Binary(pickle.dumps(blk))
		did = Binary(pickle.dumps(str(did)))			  
		
		# Fault tolerance to handle server crashes
		# Keep retrying the remove operation till it succeeds
		faultTolerantRemove(did, server, didReplica, serverReplica, key, blk)

# Return the Data Server and its ID where the block of data 'blkNum' of file 'path' is stored
def getDataServer(path, blkNum):
	hashKey = hash(path)
	did = (hashKey + blkNum) % dataServerCount
	return did, data_server[did]

# Read, concatenate and return the blocks of data from 'startBlk' to 'numBlks' of file 'path'
def readFileContents(path, startBlk, numBlks):
	data = ''
	for blk in range(numBlks):
		blkData = rpcGetData(path, startBlk+blk)
		data += blkData
	return data

# Presents a distributed file system client which communicates with the Meta Server and the Data Servers through the RPC protocol
class DistributedFS(LoggingMixIn, Operations):

	def __init__(self):
		self.fd = 0
		now = time()
		
		# Path of the root of filesystem
		rootNode = '/'
		metadata = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                        st_mtime=now, st_atime=now, st_nlink=2)
		
		#'BLK_SIZE' specifies the size of each data block in bytes
		self.BLK_SIZE = 512
		
		# RPC call to write metadata to the Meta Server
		rpcPutMeta(rootNode, metadata)

	# Returns the parent directory of the given file 'path'
	def getParentDir(self, path):
		
		# Depth denotes the depth of the path in the file system (root = 0, /dir = 1, /dir/dir = 2 and so on)
		depth = path.rfind('/') 
		if (depth == 0):
			parentDir = '/'
		else:
			parentDir = path[:depth]
		return parentDir
	
	# Returns the pathnames of the contents inside directory 'path'
	def getDirContents(self,path):
		
		# Index denotes the starting position of the pathname excluding the '/'
		# Depth denotes the depth of the path in the file system (root = 0, /dir = 1, /dir/dir = 2 and so on)
		if (path == '/'):
			index = 1
			depth = 0
		else:
			depth = path.count('/')
			index = len(path) + 1
		
		contents = []
		
		# RPC call to get all the pathnames in the system from the Meta Server
		files = rpcGetMeta('', 1)
		
		# Parse the contexts and generate the files and subdirectories contained in the parent directory.
		for x in files:
			if ((x.count('/') == depth+1) and (x.startswith(path) == True) and (x != path) and (x[index-1] == '/')):
				contents.append(x[index:])
		
		return contents
	
	# Filesystem operation to change the mode of the file
	def chmod(self, path, mode):
		metadata = rpcGetMeta(path)
		metadata['st_mode'] &= 0o770000
		metadata['st_mode'] |= mode
		
		# RPC call to write the metadata into the Meta Server
		rpcPutMeta(path, metadata)
		return 0

	# Filesystem operation to change the owner of the file
	def chown(self, path, uid, gid):
		metadata = rpcGetMeta(path)
		metadata['st_uid'] = uid
		metadata['st_gid'] = gid
		
		# RPC call to write the metadata into the Meta Server
		rpcPutMeta(path, metadata)

	# Filesystem operation to create a new file
	def create(self, path, mode):
		metadata = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(), st_blocks = 0)
		
		# RPC call to write the metadata of the created file into the Meta Server
		rpcPutMeta(path, metadata)
		
		# On creation of a new file, the data is currently an empty dictionary
		# RPC call to write the data into the Data Server
		rpcPutData(path, {})
		
		self.fd += 1
		return self.fd

	# Filesystem operation to get the metadata of the file
	def getattr(self, path, fh=None):
		
		files = rpcGetMeta('', 1)
		if (path not in files):
			raise FuseOSError(ENOENT)
		
		# RPC call to get the metadata of the file into the Meta Server
		metadata = rpcGetMeta(path)
		return metadata

	# Filesystem operation to get the extended attributes of the file
	def getxattr(self, path, name, position=0):
		
		# RPC call to get the metadata of the file into the Meta Server
		metadata = rpcGetMeta(path)
		attrs = metadata.get('attrs', {})
		try:
			return attrs[name]
		except KeyError:
			return ''
	
	# Filesystem operation to kist the extended attributes of the file
	def listxattr(self, path):
		
		# RPC call to get the metadata of the file into the Meta Server
		metadata = rpcGetMeta(path)
		attrs = meta.get('attrs', {})
		return attrs.keys()

	# Filesystem operation to create a new directory
	def mkdir(self, path, mode):
		
		# Get the parent directory where the new directory is to be created
		parentDir = self.getParentDir(path)
		
		metadata = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
		
		# RPC call to write the metadata of the new directory into the Meta Server
		rpcPutMeta(path, metadata)
		
		# RPC call to get the metadata of the file from the Meta Server
		metadata = rpcGetMeta(parentDir)
		
		# Update the number of hard links of the parent directory
		metadata['st_nlink'] += 1
		
		# RPC call to write the updated metadata of the parent directory into the Meta Server
		rpcPutMeta(parentDir, metadata)

	# Filesystem operation to open a new file
	def open(self, path, flags):
		self.fd += 1
		return self.fd

	# Filesystem operation to read the data of a new file
	def read(self, path, size, offset, fh):
		
		# RPC call to get the metadata of the file from the Meta Server
		metadata = rpcGetMeta(path)
		
		# Block from which the file read operation has to start
		offsetBlk = int(floor(offset/self.BLK_SIZE))
		
		# Number of blocks of file to be read
		numBlks = min(int(ceil(size/self.BLK_SIZE)), metadata['st_blocks'] - offsetBlk)
		
		# Starting block number
		start = offset % self.BLK_SIZE
		
		# Ending block number
		if (numBlks == (metadata['st_blocks'] - offsetBlk)):
			end = metadata['st_size']
		else:
			end = size
		
		# Read and return concatenated data from the Data Servers
		data = readFileContents(path, offsetBlk, numBlks)
		return data[start:start+end]

	# Filesystem operation to read the contents of a directory
	def readdir(self, path, fh):
		
		# Read the contents of the directory 
		contents = self.getDirContents(path)
		return ['.', '..'] + contents

	# Filesystem operation to read a symbolic link	
	def readlink(self, path):
		
		# RPC call to get the metadata of the link from the Meta Server
		metadata = rpcGetMeta(path)
		
		# Read the link
		data = readFileContents(path, 0, metadata['st_blocks'])
		
		return data

	# Filesystem operation to remove extended attributes of a file
	def removexattr(self, path, name):
		
		# RPC call to get the metadata of the file from the Meta Server
		metadata = rpcGetMeta(path)
		attrs = metadata.get('attrs', {})
		try:
			del attrs[name]
		except KeyError:
			pass
		
		# RPC call to write the updated metadata of the file into the Meta Serve
		rpcPutMeta(path, metadata)

	# Filesystem operation to rename/move a file/directory
	def rename(self, old, new):
		# Get all the pathnames of the filesystem from the Meta Server
		files = rpcGetMeta('', 1)
		# Raises an error if the user tries to move a directory into another directory which is not empty
		if new in files:
			if ((len(self.getDirContents(new)) > 0)):
				raise FuseOSError(ENOTEMPTY)

		#This code updates the 'st_nlinks' and 'st_mtime' parameter upon renaming/moving a file/directory
		# Get metadata of the old path
		metadata = rpcGetMeta(old)
		
		# Parent Directory of the old path
		oldParentDir = self.getParentDir(old)
		# Parent Directory of the new path
		newParentDir = self.getParentDir(new)
		# RPC call to the get the metadata of the old and new parent directories from the Meta Server 
		metadata_oldParentDir = rpcGetMeta(oldParentDir)
		metadata_newParentDir = rpcGetMeta(newParentDir)

		# Update the number of hardlinks only if both the old and new parent directories are not the same
		if (oldParentDir != newParentDir):
			
			# Decrement the number of hard links of the old parent directory if the file being moved is a directory
			if (S_ISDIR(metadata['st_mode']) != 0 ):
				metadata_oldParentDir['st_nlink'] -= 1
				
				# Update the number of hard links of the new parent directory only if the moved directory was not replaced/already present
				if (new not in files):
					metadata_newParentDir['st_nlink'] += 1
					
		# RPC call to the write the metadata of the old and new parent directories into the Meta Server 			
		rpcPutMeta(oldParentDir, metadata_oldParentDir)
		rpcPutMeta(newParentDir, metadata_newParentDir)

		# Modifies the related absolute paths and moves their data if needed 
		for path in files:
			if (path.startswith(old)):
				if ( (len(path) == len(old)) or (len(path)>len(old) and ( path[len(old)] == '/' )) ):
					
					# RPC call to the get the metadata of the old pathname from the Meta Server 
					metadata = rpcGetMeta(path)
					
					# Generate new pathname
					path_new = path.replace(old,new,1)
					
					# RPC call to the write the metadata of the old pathname into the new path into the Meta Server 
					rpcPutMeta(path_new, metadata)
					rpcRemoveMeta(path)
					
					# If a file is being renamed/moved, move their data from the old Data Servers to the new corresponding Data Servers
					if (S_ISREG(metadata['st_mode']) !=0 or S_ISLNK(metadata['st_mode']) != 0):
						# If the file is not empty
						if (metadata['st_blocks'] != 0):
							for blk in range(metadata['st_blocks']):
								
								# RPC call to get a block of data from the old Data Server with old filename
								blkData = rpcGetData(path, blk)
								
								# RPC call to write a block of data into the new Data Server with new filename
								rpcPutData(path_new, blkData, blk)
						else:
							# If file is empty, RPC call to write an empty dictionary into the new Data Server with new filename
							rpcPutData(path_new, {})
							
						# RPC call to remove all the data blocks from the all the Data Servers with old filename
						rpcRemoveData(path)
						
	# Filesystem operation to remove a directory
	def rmdir(self, path):
		
		# Removes the directory only if it is empty
		if (len(self.getDirContents(path)) == 0):
			
			# RPC call to remove the metadata of the directory being removed
			rpcRemoveMeta(path)
			
			# Get the parent directory of the directory being removed
			parentDir = self.getParentDir(path)
			
			# RPC call to get the metadata of the parent directory of the directory being removed
			metadata = rpcGetMeta(parentDir)
			
			# Decrement the number of hard links of the parent directory
			metadata['st_nlink'] -= 1
			
			# RPC call to write the updated metadata into the Meta Server
			rpcPutMeta(parentDir, metadata)
		
		# Raise an error if the directory is not empty
		else:
			raise FuseOSError(ENOTEMPTY)

	# Filesystem operation to set extended attributes of a file
	def setxattr(self, path, name, value, options, position=0):
		
		# RPC call to get the metadata from the Meta Server
		metadata = rpcGetMeta(path)
		attrs = metadata.setdefault('attrs', {})
		attrs[name] = value
		
		# RPC call to write the updated metadata from the Meta Server
		rpcPutMeta(path, metadata)

	# Filesystem operation to get the statistics of the filesystem
	def statfs(self, path):
		return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

	# Filesystem operation to create a symbolic link
	def symlink(self, target, source):
		metadata = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1,
                                  st_size=0, st_blocks=0)
		
		# RPC call to write the metadata of the symbolic link into the Meta Server
		rpcPutMeta(target, metadata)
		
		# RPC call to create an entry in the Data Server
		rpcPutData(target, {})
		
		# Write the symbolic link data into the Data Server
		self.write (target,source,0,None)

	# Filesystem operation to truncate a file
	def truncate(self, path, length, fh=None):
		
		# RPC call to get the metadata from the Meta Server
		metadata = rpcGetMeta(path)
		
		# If truncating length less than file size, truncate file data to given length
		if (length <= metadata['st_size']):
			
			# Block from which truncation has to begin
			truncBlk = int(floor(length/self.BLK_SIZE))
			
			# Bytes in 'truncBlk' from which truncation has to begin
			truncBytes = length % self.BLK_SIZE

			# The loop below is use to remove all data blocks after 'truncBlk'and decrement the 'st_blocks' parameter for every block removed
			# Denotes the first block which has to be truncated
			start = truncBlk
			
			# Last block which has to be truncated
			end = metadata['st_blocks']

			# These statements remove 'truncBlk' if the byte from which truncation has to begin is equal to 0 else they remove the bytes after
			# 'trunc_byte'in the block referred by 'truncBlk'
			
			# Remove bytes from the block if required
			if (truncBytes != 0):
				
				# RPC call to get the block from the Data Server
				blkData = rpcGetData(path, truncBlk)
				blkData = blkData[:truncBytes]
				
				# RPC call to write the truncated data block into the Data Server
				rpcPutData(path, blkData, truncBlk)
				
				# Increment block to be truncated
				start = truncBlk+1
		
			# Remove all the data blocks from 'start' to 'end' from the Data Server
			for blk in range(start, end):
				
				# RPC call to remove data from the Data Server
				rpcRemoveData(path, blk)

			# Update the 'st_size' parameter
			metadata['st_size'] = length
			
			# Update the number of blocks of the file
			metadata['st_blocks'] = int(ceil(metadata['st_size']/self.BLK_SIZE))
			
			# RPC call to write updated metadata into the Meta Server
			rpcPutMeta(path, metadata)
		
		#If truncating length is greater than file size, append Null characters ('\00') to file
		else:
			
			# Start writing NULL from the last byte of the file
			offset = metadata['st_size']
			
			# The entire data needed to be written
			data = '\00' * (length - offset)
			
			# Write data into the file
			self.write(path, data, offset, fh)

	# Filesystem operation to remove a file
	def unlink(self, path):
		
		# RPC call to remove the metadata from the Meta Server
		rpcRemoveMeta(path)
		
		# RPC call to remove the data from all the Data Servers
		rpcRemoveData(path)

	# Filesystem operation to update the timestamp of a file
	def utimens(self, path, times=None):
		now = time()
		atime, mtime = times if times else (now, now)
		
		# RPC call to get the metadata from the Meta Server
		metadata = rpcGetMeta(path)
		metadata['st_atime'] = atime
		metadata['st_mtime'] = mtime
		
		# RPC call to write the updated metadata into the Meta Server
		rpcPutMeta(path, metadata)

	# Filesystem operation to write to a file
	def write(self, path, data, offset, fh):

		# Last block written
		offsetBlk = int(floor(offset/self.BLK_SIZE))
		
		# Last byte written in the last block
		offsetBytes = offset % self.BLK_SIZE

		# RPC call to get the metadata from the Meta Server
		metadata = rpcGetMeta(path)
		
		# "start" and "end" are indexes used to parse data into appropriate block size
		start = 0
		end = 0

		# Write the data into the free space in the last block written and fill it upto BLK_SIZE bytes and update the 'start' and 'end' indexes
		if (offsetBytes != 0):
			
			start = 0
			end = min(len(data), (self.BLK_SIZE - offsetBytes))
			
			# RPC call to get the block from the Data Server
			blkData = rpcGetData(path, offsetBlk)
			blkData = blkData[:offsetBytes]+data[start:end]
			
			# RPC call to write the updated block into the Data Server
			rpcPutData(path, blkData, offsetBlk)
			offsetBlk += 1

		# If the entire data has not been written, write the remaining bytes of data into new blocks of
		# size BLK_SIZE. Increments the 'st_blocks' parameter for every new block written
		if (end != len(data)):
			
			start = end
			remainingBytes = len(data) - start
			newBlks = int(ceil(remainingBytes/self.BLK_SIZE))
			
			for location in range(newBlks):
				if (location == (newBlks - 1)):
					end = len(data)
				else:
					end = start + self.BLK_SIZE
				
				blkNum = offsetBlk + location
				blkData = data[start:end]
				
				# RPC call to write the new block into the Data Server
				rpcPutData(path, blkData, blkNum)
				
				start = end

		# Update 'st_size' parameter
		metadata['st_size'] += len(data)
		
		# Update 'st_blocks' parameter
		metadata['st_blocks'] = int(ceil(metadata['st_size']/self.BLK_SIZE))
		
		# RPC call to write updated metadata into the Meta Server
		rpcPutMeta(path, metadata)
		return len(data)


if __name__ == '__main__':
	
	# Check correct arguments
	if len(argv) < 4 :
		print ("usage: %s <mountpoint> <metaserver port> <dataserver ports>" % argv[0])
		exit(1)	
	ms_port = "http://localhost:" + argv[2]
	
	# Establish a connection to the Data Server
	metaServer = xmlrpclib.ServerProxy(ms_port)
	dataServerCount = len(argv) - 3
	data_server = []
	dsID = []
	for i in range (dataServerCount):
		dataServer_port = "http://localhost:" + argv[i+3]
		
		# Establish a connection to the Data Server
		data_server.append(xmlrpclib.ServerProxy(dataServer_port))
		
		# RPC call to get the ID from the Data Server
		did = rpcGetID(data_server[i])
		dsID.append(did)	
		
	# Sort the Data Server IDs in the order the ports were entered by the user
	# This was done so that the client can be run by entering the data server ports in any order
	data_server = [x for (y,x) in sorted(zip(dsID, data_server))]
	
	logging.basicConfig(level=logging.DEBUG)
	fuse = FUSE(DistributedFS(), argv[1], foreground=True, debug=False)

