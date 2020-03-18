## App8


### Problem identified

The data was skewed. This caused most of the data to be shuffled into one of the executors, causing it to run out of memory and forcing it to spill to disk, which then seems to make it crash (lack of disk space?). This was identified with the following error message from the driver
```

20/03/17 14:27:04 ERROR YarnClusterScheduler: Lost executor 1 on iccluster060.iccluster.epfl.ch: Executor heartbeat timed out after 154856 ms

```

and those messages of the executor, which corresponds to one message informing us of the available memory on that executor as well of the last message of that executor

```
20/03/17 14:21:58 INFO MemoryStore: Block broadcast_2_piece0 stored as bytes in memory (estimated size 3.2 KB, free 408.2 MB)

(...)

20/03/17 14:23:38 INFO ExternalAppendOnlyMap: Thread 91 spilling in-memory map of 413.2 MB to disk (5 times so far)

```

### Solution

The main idea of the fix to that problem was to modify the code to forcing the reshuffle of the data after computing the sum associated to each key, thus balancing the load to all executors.