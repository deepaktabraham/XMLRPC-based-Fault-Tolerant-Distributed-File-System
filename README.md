# XMLRPC-based-Fault-Tolerant-Distributed-File-System
This is the source code for a distributed File System in Userspace (FUSE), where the data blocks are spread over multiple servers to distribute the load. The code also addresses the concepts of redundancy and fault-tolerance. <br /><br />

###Design Parameters
1. The meta-server stores the metadata of files in the file system. The data-servers store the file data. 
2. There is only one meta-server and it is considered to be extremely reliable and never fails.
3. There can be multiple data-servers, but a minimum of two is required. 
4. Data is stored in the data-servers in a round-robin fashion, in blocks of 512 bytes. Also, the redundant copy of a data block is stored in the next data-server in the ID space.<br />
  Replica 1:&nbsp;&nbsp;[x % N, (x+1) % N, (x+2) % N, (x+3) % N ...], where x is the hash of the file path and N is the number of data-servers.<br />
  Replica 2:&nbsp;&nbsp;[(x+1) % N, (x+2) % N, (x+3) % N, (x+4) % N ...].<br />
5. The data-servers also stores the data blocks in the persistent storage (hard disk), to allow for recovery upon a crash. 
6. In case the data-server process crashes and restarts, the server will recover and resume serving data by using data stored on the local disk.
7. In case the data-server's persistent storage is completely lost and the server restarts, then it will recover by using replica(s) from its adjacent servers, and then copy the data blocks to be stored in itself.
8. To deal with data corruption, the checksums of data blocks is also stored in the data-servers. 
9. When there is a read call from the user, the FUSE client reads from both data-servers (original and redundant copy of data), verify the checksum, and writes back to the corrupted server, if the client detects data corruption.
10. When a server is down, any write calls on the FUSE folder will be a blocking call. The FUSE program will keep retrying the operation till it succeeds, and the write call will not return until it succeeds.
11. Reads will return successfully even if a single replica is available, and the checksum verifies correctly.<br /><br />

###Steps to run the Fault-Tolerant Distributed File-System:
Meta-Server : ***`python metaserver.py <port for metaserver>`***<br />
Data-Server : ***`python dataserver.py <0 indexed server number> <ports for all dataservers separated by spaces>`***<br />
Distributed FUSE Client : ***`python distributedFS.py <fusemount directory> <meta server port> <dataserver ports seperated by spaces>`***<br />
Corruption Simulator : ***`python corrupt.py <port of the dataserver to be corrupted> <pathname of the file to be corrupted in the context of FUSE filesystem>`***<br />

Example (N=5):<br />
python metaserver.py 1111<br />
python dataserver.py 0 2222 3333 4444 5555 6666<br />
python dataserver.py 1 2222 3333 4444 5555 6666<br />
python dataserver.py 2 2222 3333 4444 5555 6666<br />
python dataserver.py 3 2222 3333 4444 5555 6666<br />
python dataserver.py 4 2222 3333 4444 5555 6666<br />
python distributedFS.py fusemount 1111 2222 3333 4444 5555 6666<br />
python corrupt.py 2222 /file1
