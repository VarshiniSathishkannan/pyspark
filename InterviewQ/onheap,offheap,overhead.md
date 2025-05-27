Each Executor Container has

Onheap ,offheap, overhead memory

When application runs, it requests the YARN for executor memory + overhead + offheap. Ensure the total memory usage does not exceed the physical memory of each node.

# Onheap memory:

Managed by JVM.
spark.executor.memory = 10G

Unified memory (spark.memory.fraction = 0.6(default)) - 6GB
    Execution memory 3GB
        Join, shuffle, sort, groupby
    Storage memory (spark.memory.storageFraction = 0.5(default)) - 3GB
        caching, RDD, DF, Broadcastvariables
    
    the execution and storage can adjust itself based on the needs and availability. storage evicts RDD blocks based on LRU algoithm (Least Recently Used) and allocated memory to storage or execution if needed. Execution has more priority than storage.

Remaining - 4GB
    User memory 
        UDF, variables, Objects
    Reserved memory 
        internal spark running process

# offheap memory:

spark.memory.offheap.enabled = False (default)
spark.memory.offheap.size = 0 (default - 0GB) (In byetes) 
    This setting does not allocate off-heap memory; rather, it sets the upper limit for how much off-heap memory Spark is allowed to consume.

Off-heap memory is memory allocated outside of the JVM heap. Spark uses this memory to store data that is managed directly by Spark, bypassing the JVM's garbage collector.
The benefit of off-heap memory is that it avoids the overhead of garbage collection, which can be a bottleneck in memory-intensive workloads. By storing large amounts of data off-heap, Spark can improve performance by reducing the pressure on the JVM heap.

it allocates off-heap memory directly from the operating system’s memory space

Off-heap memory is allocated directly from the system’s physical memory (RAM). Therefore, the total amount of off-heap memory Spark can use is ultimately constrained by the total available physical memory on the system.

# overhead memory:

spark.executor.memoryOverhead = max(384MB,10% of executor memory)(default)

The main purpose of this configuration is to provide additional memory outside the JVM heap for the executor's overhead.

Overhead memory is used for non-heap operations that are not part of the actual data storage, like:
Spark's internal task metadata.
Shuffle buffers and communication between executors.
Memory used for the JVM's off-heap memory (e.g., when using Tungsten optimizations).
Other Spark-related internal mechanisms that require memory outside the JVM's heap space.

Spark executors require memory beyond what is allocated for the data itself (which resides in the JVM heap) to handle overhead tasks, especially when performing operations like shuffling, caching, sorting, or managing task execution.
If there is insufficient overhead memory, Spark may run into OutOfMemoryErrors or experience significant performance degradation due to frequent garbage collection or inefficient memory management.

The total memory available for an executor in Spark is the sum of:

spark.executor.memory (for the JVM heap),
spark.executor.memoryOverhead (for off-heap overhead).

If you set spark.executor.memory=4g and spark.executor.memoryOverhead=512m, the total memory available for the executor will be 4.5 GB (4 GB for heap + 512 MB for overhead).

Increase the spark.executor.memoryOverhead if you are dealing with large shuffles, heavy transformations (e.g., joins, aggregations), or if you are caching large datasets.
It's especially useful when running complex jobs or workloads that involve large amounts of network I/O or disk I/O.
If you encounter OutOfMemoryError or slow performance, increasing the overhead memory can sometimes resolve the issue, especially in memory-intensive workloads.

# Why off-heap memory is used?

suppose if on heap memory is fully used on a container, then Garbage collection starts, which will clean up the old process to make room. this will halt the process completley which impacts the performance.
Offheap memory comes useful in those cases. since off heap memory is managed by OS, So GC cycles does not take place. But it is the developers responsibility to allocate and deallocate the resources 
This is faster than disk with little network latency but if utilized properly it provides greater performance.

# When to Use Off-Heap Memory

1. Large Datasets: When caching or processing large datasets that might exceed the JVM heap capacity, to avoid GC overhead.
2. Tungsten Optimizations: For utilizing Spark's Tungsten memory management for efficient binary storage and processing.
3. Memory-Intensive Operations: When performing operations like shuffling, joins, aggregations, or sorting, which involve large data transfers and require significant memory.
4. Serialized Data: When dealing with large, serialized data (e.g., Kryo serialization) that would otherwise create heavy memory pressure on the JVM heap.
5. Complex Data Structures: When working with complex or large data structures (like large arrays, deeply nested data) that need to be stored outside the JVM heap.
6. Stream Processing: In streaming jobs with stateful operations, where intermediate results or state information might require more memory than the JVM heap can handle.
7. Shuffle and Join Operations: When performing wide transformations, especially those that shuffle large amounts of data between partitions or nodes.

