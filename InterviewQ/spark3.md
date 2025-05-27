Dataproc 2.0 installs Spark 3

# Adv:
1. GPU support
2. Ability to read binary files
3. Performance improvements
4. Dynamic partition pruning
5. AQE - Adaptive Query Execution

# Dynamically coalescing shuffle partitions
# Dynamically switching join strategies
# Dynamically optimizing skew joins 

1. Too mnay partitions with less data causing IO network overload or less partitions with more data causing long running tasks - AQE solves this by dynamically coalescing shuffle partitions

2. AQE plans the join strategy at runtime based on the most accurate join relation size. 

3. AQE skew join optimization detects the skew automatically from shuffle file statistics. It then splits skewed partitions into smaller sub partitions which will be joined to the corresponding partition from the other side respectively

