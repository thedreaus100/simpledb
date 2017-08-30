# Simple DB #


**Description**

SimpleDB is as its name suggests a simple database.  Its implemented using an
LSM strategy, meaning that SimpleDB writes and retrieves keys from in-memory (memtable)
then periodically dumps this data to disk once it gets past a certain threshold.

***Memtable***

SimpleDB implements memtables using TreeMaps to buffer writes before writing to disk.  By using a TreeMap,
SimpleDB is able to improve insertion performance because of the balanced nature of a TreeMap (Red Black Tree)
and also ensure that keys can be written to the Map in any order and read in sorted
order.  By maintaining a sorted list of keys in the system, SimpleDB is then able to then dump the keys to disk,
while maintaining their order.  This characteristic becomes is important for not only 
sequential access but it allows SimpleDB to maintain a sparse in-memory
index of the data on disk.

***Retrieval***

SimpleDB's sparse in-memory index allows it to quickly traverse on-disk logs in order to effeciently
search for Keys.

***Compaction***

Not Yet Implemented.

***Concurrency***

SimpleDB operates at very highlevels of concurrency and utilizes Atomic variables and other
concurrent data structures as well as ReadWriteLocks.  SimpleDB is able to simutaneously dump
a full Memtable to disk while still writing data to a new non full Memtable.  So writes
are never paused for long ensuring that SimpleDB has incredibly high write throughput.

SimpleDB uses a fair ReentrantReadWriteLock in order to pause writing to a full memtable
so that it can then be dumped to disk.  If a write tries to access a full memtable
a MemtableFullException is thrown and is then blocked until a non-full Memtable is available.  
The ReentranantReadWriteLock that SimpleDB also limits starvation.

***Schema***

SimpleDB is also (Avro Schema)[https://avro.apache.org/docs/1.8.1/index.html] compatible.  The Avro schema allows data to be effeciently compacted
but also allows schemas to be backward and forward compatible.


***Performance***

SimpleDB performance can be customed in the following ways:

* Adjust maxSize threshold for Memtable.  A higher threshold will improve write performance,
but it also uses more memory.  In some cases a highter memory threshold will aso improve GET requests.

* Adjust blockSize for Memtable.  A lower block size improves read performance but it also increases
the size of the Sparse Index, which uses more memory.

* _to be continued_