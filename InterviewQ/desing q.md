1. You need to move data from multiple sources (CSV, API, database) into a data warehouse daily. How would you design the pipeline?
2. A downstream analytics team is reporting incorrect data. How would you investigate and resolve the issue?
3. You are given a terabyte-sized dataset with frequent updates. How would you design a scalable system to track changes over time?
4. A Spark job is running slower than expected. What steps would you take to diagnose and optimize it?
5. You’re asked to integrate real-time event data into an existing batch processing architecture. How would you approach this?
6. The business wants to track user activity in near real-time. What technologies and architecture would you propose?
7. One of your ETL jobs failed midway and caused partial data load. How would you handle data consistency and rerun logic?
8. You need to implement data versioning in a pipeline where schema changes are expected. How would you handle this?
9. There’s a requirement to mask sensitive data in your pipelines. What would your approach be?
10. You receive JSON data with inconsistent structure from an API. How would you process and store it reliably?
11. You’re asked to design a data lake strategy for a financial services product. What factors will you consider?
12. The data team is struggling with long-running joins in Hive. What optimizations would you suggest?
13. A batch job is processing daily logs but has to be backfilled for the past 3 months. How would you handle this efficiently?
14. You are joining two large datasets (100M+ records each) and running into memory errors. What’s your approach?
15. You must ensure data lineage and auditability for every transformation step. What tools or frameworks would you use?


2

Start with clarity on what’s wrong.

What data is incorrect? (metrics, dimensions, values)

Where? (specific dashboard, table, field)

When? (date range or incident timestamp)

How was it detected? (comparison with source data, anomalies, manual checks)

Log all this to form a hypothesis.