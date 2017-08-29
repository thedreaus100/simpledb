***Simple DB***

I will create a real README later for now this is essentially a todo list

**Problem**

currently they're are two ways input is inputted into the system

1 - CMD (command line)

2 - API

depending on the type used the system processes input slightly differently

for CMD input each commands is executed in a callable thread which then blocks for 10 seconds
after which the systems returns a TimeoutException for the command

for API input the commands are non-blocking and are executed in seperate short lived threads
each of these threads has access to an output stream which they can write to 
(the underlying output stream should be thread safe so that the responses aren't garbled)

There is a daemon that runs in the background that checks to see if the underlying Memtable is full.
the idea here is that once the memtable fills up it should dump it to a log:

    there are currently a few issues here
    1) I haven't placed a ReentryReadLock write on the system so its possible that during the time I'm checking
       to see if the memtable is full additional writes are being done.
    2) the daemon for manageMemtable runs frequentally but not necessarily fast enough to keep up w/ the writes
       therefore
       
**Solution:**

Add a ReentrantReadWriteLock to the system that locks prioritizes GET ACTIONS
and blocks SET ACTION when there is a GET request or the manageMemtable daemon is
running.

Also after the system is full... the SET CMDS should block until the manageMemtable daemon completes
this way the manage memtable daemon doesn't have to race the SET ACTIONS

**Problem**

JVM serialization is inconsistent.... need a standard way of encoding Inputs.
I also wanto attach a timestamp and other metadata to entries in SimpleDB

**Solution**

Use Avro to encode data before writing to disk, will not only reduce the size slightly but
it will also allow SimpleDB to start using Schemas.

