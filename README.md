
# Advanced Databases – Homework 2

## Deadlocks, Transaction Restart & Starvation Prevention

This project was developed as part of the Advanced Databases course at Paris Lodron University of Salzburg, Winter Semester 2026/27.

The objective is to demonstrate database deadlocks, implement automatic transaction recovery, and reduce the risk of transaction starvation.

## Technologies Used

- Java 21
- PostgreSQL 16
- PostgreSQL JDBC Driver
- IntelliJ IDEA
- Git and GitHub

## Project Structure

### Blocking.java

The original program demonstrates deadlocks caused by concurrent database transactions.

Multiple threads execute UPDATE statements on the same database table. Some transactions are aborted because PostgreSQL detects deadlocks.

### RestartBlocking.java

This version implements automatic transaction recovery.

When a deadlock occurs, the program:
1. Detects the PostgreSQL deadlock error (SQLSTATE 40P01).
2. Rolls back the failed transaction.
3. Restarts the entire transaction.
4. Repeats the process until the transaction commits successfully.

### NoLiveLocks.java

This version adds protection against transaction starvation.

It uses a fair ReentrantReadWriteLock to coordinate concurrent execution. After repeated deadlocks, a transaction switches to exclusive execution.

This reduces the risk of a transaction being repeatedly aborted without completing.

## Database Setup

The project uses a local PostgreSQL database.

Configuration:
- Database: adb_hw2
- Host: localhost
- Port: 5432
- Username: postgres
- Isolation level: READ COMMITTED

A local ConnectionFactory.java class is required to establish the JDBC connection.

This file is excluded from the Git repository because it contains local database configuration.

The PostgreSQL password is provided through the ADB_DB_PASSWORD environment variable.

## Running the Programs

1. Install Java 21 and PostgreSQL.
2. Create the adb_hw2 database.
3. Clone this repository.
4. Open the project in IntelliJ IDEA.
5. Configure the PostgreSQL JDBC driver.
6. Create and configure the local ConnectionFactory.java.
7. Set the ADB_DB_PASSWORD environment variable.
8. Run Blocking.java, RestartBlocking.java, or NoLiveLocks.java.

Run the programs separately because each program recreates the same database table.

## Testing Results

Example results from our tests using 30 tasks:

| Program | Completed Transactions |
|---|---|
| Blocking.java | 21 / 30 |
| RestartBlocking.java | 30 / 30 |
| NoLiveLocks.java | 30 / 30 |

The original program experienced deadlocks that caused some transactions to fail.

RestartBlocking.java successfully completed all transactions by retrying aborted transactions.

NoLiveLocks.java also completed all transactions and demonstrated the activation of its fair exclusive execution mechanism.

Results may vary between executions and hardware configurations.

## Team Members

- Ali Hassan
- Rudi Maćešić
- Dario Šaša

## Conclusion

The project demonstrates how concurrent database transactions can cause deadlocks and how automatic transaction retries can improve successful completion.

An additional fair locking mechanism helps reduce the risk of starvation during concurrent execution.
