# Advanced Databases – Homework 3

## Homework 2 Revisited: Transaction Throughput Optimization

This project was developed for the Advanced Databases course at Paris Lodron University of Salzburg, Winter Semester 2026/27.

Homework 3 extends our previous work on database deadlocks, transaction recovery, and starvation prevention.

The objective is to simulate a more realistic database workload, measure transaction throughput, identify performance bottlenecks, and improve execution performance while ensuring that transactions commit successfully.

## Technologies Used

- Java 21
- PostgreSQL 16
- PostgreSQL JDBC Driver 42.7.14
- IntelliJ IDEA
- Git and GitHub
- pgAdmin 4

## Project Structure

### TransactionTask.java

Implements four types of database transactions:

| Transaction Type | Probability | Description |
|---|---:|---|
| Multi-point READ | 60% | Reads three different randomly selected rows using three separate SELECT statements and prints their sum |
| INSERT | 10% | Inserts one new tuple into the database |
| Full-table SUM | 10% | Executes SELECT SUM(data_value) FROM blocking_data and prints the result |
| UPDATE | 20% | Executes three separate single-row UPDATE statements in one transaction |

Transaction types are selected using a randomly generated value between 0 and 1.

The implementation uses PostgreSQL transactions with the READ COMMITTED isolation level.

When PostgreSQL reports a deadlock (SQLSTATE 40P01) or serialization failure (SQLSTATE 40001), the affected transaction is rolled back and retried using the same transaction data.

A fair ReentrantReadWriteLock provides an exclusive execution fallback after repeated conflicts, reducing the risk of starvation among tasks in the Java program.

### ConcurrentBenchmark.java

Provides the initial benchmark implementation.

It continuously generates transactions using a configurable number of worker threads and measures the number of committed transactions per second.

We tested 1, 2, 4, and 8 worker threads, including single-threaded serial execution.

### ProfileBenchmark.java

Measures the average duration of each transaction type and the proportion of total worker time spent executing each type.

This helps identify expensive operations and understand their contribution to the workload.

### TimingProbe.java

Measures the individual costs of:

- Opening a JDBC connection
- Setting up a transaction
- Executing SQL statements
- Committing a transaction
- Closing the connection

This allows us to distinguish SQL execution costs from connection-management overhead.

### OptimizedBenchmark.java

Implements the optimized concurrent benchmark.

The main improvements are:

1. Reusing one JDBC connection per worker thread.
2. Reusing PreparedStatement objects for SQL execution.
3. Avoiding unnecessary statement creation during each transaction.
4. Preserving the original order of UPDATE statements.
5. Retrying transactions after detected deadlocks or serialization failures.
6. Using a fair exclusive execution fallback after repeated conflicts.

The program maintains the original workload distribution and prints the results of read-only transactions.

### StatementReuseProbe.java

Compares two approaches for executing identical multi-point SELECT transactions:

- Creating a new PreparedStatement for each transaction.
- Reusing an existing PreparedStatement.

Both approaches use the same JDBC connection and the same query inputs.

This test isolates the effect of statement reuse from the cost of opening JDBC connections.

## Database Configuration

The project uses a local PostgreSQL database with the following settings:

- Host: localhost
- Port: 5432
- Database: adb_hw2
- Username: postgres
- Isolation level: READ COMMITTED

The database contains the blocking_data table introduced in Homework 2, with 50,000 initial rows.

The required columns are:

- data_id
- data_value
- modified_by

ConnectionFactory.java is kept locally in the src directory and is excluded from Git because it contains local database configuration.

The PostgreSQL password is passed through the ADB_DB_PASSWORD environment variable.

Before starting a benchmark, the program removes rows inserted by previous Homework 3 runs. UPDATE operations on existing rows are not reset by this cleanup.

## Running the Benchmarks

Open the project in IntelliJ IDEA, configure Java 21 and the PostgreSQL JDBC driver, and make sure PostgreSQL is running.

Set the ADB_DB_PASSWORD environment variable in the relevant IntelliJ Run configuration.

### Initial Benchmark

Run ConcurrentBenchmark.java.

The number of worker threads and benchmark duration can be changed using the THREADS and DURATION_SECONDS constants.

Setting THREADS to 1 provides the serial execution baseline.

### Optimized Benchmark

Run OptimizedBenchmark.java with the following program arguments:

| Program Arguments | Meaning |
|---|---|
| 1 30 | 1 thread for 30 seconds |
| 2 30 | 2 threads for 30 seconds |
| 4 30 | 4 threads for 30 seconds |
| 8 30 | 8 threads for 30 seconds |

The benchmark reports committed transactions, average transaction times, detected deadlocks, exclusive mode switches, and throughput.

### Additional Analysis

Run ProfileBenchmark.java, TimingProbe.java, or StatementReuseProbe.java to reproduce the corresponding diagnostic measurements.

## Initial Benchmark Results

Each benchmark ran for approximately 30 seconds.

| Threads | Committed Transactions | Throughput (tx/s) |
|---|---:|---:|
| 1 | 341 | 11.34 |
| 2 | 1,196 | 39.82 |
| 4 | 1,982 | 65.90 |
| 8 | 2,703 | 89.94 |

These measurements were collected before the final optimizations.

## PostgreSQL Query Analysis

We used PostgreSQL EXPLAIN (ANALYZE, BUFFERS) to inspect the execution plans of representative SQL queries.

The full-table SUM query used a Sequential Scan and required approximately 7 ms in the recorded analysis.

A SELECT query targeting a single data_id used the primary-key index and completed in approximately 0.04 ms.

These results show that full-table aggregation is substantially more expensive than a single indexed row lookup.

## Transaction Profiling

Using four worker threads, we recorded the following average transaction times:

| Transaction Type | Average Time (ms) | Share of Worker Time |
|---|---:|---:|
| Multi-point READ | 56.055 | 56.9% |
| INSERT | 54.916 | 10.1% |
| Full-table SUM | 68.137 | 13.6% |
| UPDATE | 59.071 | 19.3% |

The profiling results suggested that a substantial portion of transaction time came from operations shared across transaction types.

## JDBC Timing Analysis

Separate timing measurements showed:

| Operation | Multi-point READ | Full-table SUM |
|---|---:|---:|
| JDBC connection | 38.192 ms | 38.560 ms |
| Transaction setup | 0.296 ms | 0.233 ms |
| SQL execution | 3.007 ms | 8.959 ms |
| COMMIT | 0.174 ms | 0.230 ms |
| Connection close | 0.144 ms | 0.152 ms |
| Total | 41.813 ms | 48.135 ms |

Opening a new JDBC connection for every transaction was a major source of overhead in the original implementation.

Although connection management is not the primary optimization target specified in the assignment, we eliminated this unnecessary overhead to obtain a more efficient benchmark infrastructure.

## PreparedStatement Reuse Analysis

To evaluate an optimization more directly related to SQL execution, we measured statement reuse independently of connection creation.

The test used the same database connection and identical query inputs.

| Execution Approach | Average Time per Transaction |
|---|---:|
| New PreparedStatement | 0.281 ms |
| Reused PreparedStatement | 0.236 ms |

Observed speedup: **1.19x**

The results were identical in both approaches.

This corresponds to an approximately 16% reduction in average transaction execution time in this specific microbenchmark.

## Optimized Benchmark Results

Each optimized benchmark ran for approximately 30 seconds.

| Threads | Committed Transactions | Throughput (tx/s) |
|---|---:|---:|
| 1 | 31,789 | 1,059.59 |
| 2 | 58,023 | 1,933.95 |
| 4 | 100,657 | 3,354.84 |
| 8 | 161,959 | 5,397.15 |

All started transactions committed successfully in the recorded benchmark runs.

No deadlocks or exclusive mode switches were observed in these particular executions.

## Before and After Optimization

| Threads | Initial Throughput (tx/s) | Optimized Throughput (tx/s) |
|---|---:|---:|
| 1 | 11.34 | 1,059.59 |
| 2 | 39.82 | 1,933.95 |
| 4 | 65.90 | 3,354.84 |
| 8 | 89.94 | 5,397.15 |

The optimized implementation achieved substantially higher throughput in our measurements.

The large overall improvement is mainly associated with reusing JDBC connections and reducing repeated setup overhead. The separate statement-reuse experiment demonstrates an additional, more targeted improvement in SQL transaction execution.

The measurements