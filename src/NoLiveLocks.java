import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Martin Schaeler martin.schaeler@plus.ac.at
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
public class NoLiveLocks {

    public static int num_taks = 30;
    public static int num_queries_per_task = 3;

    // After 3 deadlocks, a task switches to exclusive execution.
    private static final int MAX_CONCURRENT_DEADLOCKS = 3;

    // Fair lock to prevent starvation.
    // Read lock allows concurrent transactions.
    // Write lock allows one transaction to execute alone.
    private final ReentrantReadWriteLock transactionGate =
            new ReentrantReadWriteLock(true);

    Random rand = new Random(123456);
    int num_tuple = 50000;
    double max_value = 50.0f;

    AtomicInteger tasks_finished = new AtomicInteger(0);
    AtomicInteger deadlocks = new AtomicInteger(0);
    AtomicInteger exclusiveFallbacks = new AtomicInteger(0);

    // Execute SQL statements for database initialization
    void executeSQL(String sql, Connection con) throws SQLException {
        try (java.sql.Statement stmt = con.createStatement()) {
            stmt.execute(sql);
            System.out.println("Executing " + sql);
        }
    }

    // Create and initialize the database table
    void cerateDB() throws SQLException {
        try (Connection con =
                     ConnectionFactory.getDefaultParameterConnection(
                             ConnectionFactory.POSTGRESQL)) {

            executeSQL("DROP TABLE IF EXISTS blocking_data;", con);

            executeSQL(
                    "CREATE TABLE blocking_data (" +
                            "data_id integer primary key, " +
                            "data_value float, " +
                            "modified_by integer);", con);

            con.setAutoCommit(false);

            try (PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO blocking_data VALUES (?, ?, ?)",
                    ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_UPDATABLE)) {

                for (int i = 0; i < num_tuple; i++) {
                    ps.setInt(1, i);
                    ps.setDouble(2, rand.nextDouble() * max_value);
                    ps.setInt(3, 0);
                    ps.addBatch();
                    ps.clearParameters();
                }

                try {
                    int[] results = ps.executeBatch();
                    con.commit();

                    System.out.println(
                            "Inserted " + results.length + " rows.");

                } catch (SQLException e) {
                    con.rollback();
                    throw e;
                }
            }
        }
    }

    public static void main(String[] args) throws SQLException {

        NoLiveLocks b = new NoLiveLocks();

        // Create and populate the database
        b.cerateDB();

        // Create the tasks
        ArrayList<NoLiveLocks.Blocker> my_tasks =
                new ArrayList<>(num_taks);

        for (int task = 0; task < num_taks; task++) {
            my_tasks.add(b.new Blocker(task));
        }

        // Execute tasks using 4 threads
        ThreadPoolExecutor executor =
                (ThreadPoolExecutor)
                        Executors.newFixedThreadPool(4);

        long start = System.currentTimeMillis();

        for (Blocker task : my_tasks) {
            executor.execute(task);
        }

        executor.shutdown();

        // Wait until all tasks have finished
        while (!executor.isTerminated()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
                break;
            }
        }

        long elapsed = System.currentTimeMillis() - start;

        System.out.println(
                "[DONE] Task execution after roughly " +
                        elapsed + " ms finished: " +
                        b.tasks_finished + " of " + num_taks);

        System.out.println(
                "Total detected deadlocks: " + b.deadlocks);

        System.out.println(
                "Tasks switched to fair exclusive mode: " +
                        b.exclusiveFallbacks);
    }

    /**
     * Each Blocker executes multiple UPDATE statements
     * inside one transaction.
     *
     * After repeated deadlocks, the task gets exclusive
     * access to prevent starvation.
     */
    public class Blocker implements Runnable {

        final int task;
        final double[] start_range;
        final double[] stop_range;

        public Blocker(int id) {
            this.task = id;

            this.start_range =
                    new double[num_queries_per_task];

            this.stop_range =
                    new double[num_queries_per_task];

            for (int q = 0; q < num_queries_per_task; q++) {

                stop_range[q] =
                        rand.nextDouble() * max_value;

                start_range[q] =
                        rand.nextDouble() * stop_range[q];
            }
        }

        @Override
        public void run() {

            int retries = 0;

            // Generate values once for the transaction.
            // All retries use the same values.
            double[] newValues =
                    new double[num_queries_per_task];

            for (int q = 0; q < num_queries_per_task; q++) {
                newValues[q] =
                        (float) (rand.nextDouble() * max_value);
            }

            // Keep retrying until the transaction succeeds.
            while (!Thread.currentThread().isInterrupted()) {

                // First try concurrent execution.
                // After 3 deadlocks, switch to exclusive mode.
                boolean exclusive =
                        retries >= MAX_CONCURRENT_DEADLOCKS;

                Lock gate = exclusive
                        ? transactionGate.writeLock()
                        : transactionGate.readLock();

                gate.lock();

                try {
                    Connection con = null;

                    try {
                        // Each task uses its own connection.
                        con =
                                ConnectionFactory
                                        .getDefaultParameterConnection(
                                                ConnectionFactory.POSTGRESQL);

                        con.setAutoCommit(false);

                        con.setTransactionIsolation(
                                Connection.TRANSACTION_READ_COMMITTED);

                        String sql =
                                "UPDATE blocking_data " +
                                        "SET data_value = ?, modified_by = ? " +
                                        "WHERE data_value >= ? " +
                                        "AND data_value <= ?;";

                        try (PreparedStatement ps =
                                     con.prepareStatement(sql)) {

                            // Execute all UPDATE statements
                            // in their original order.
                            for (int q = 0;
                                 q < num_queries_per_task;
                                 q++) {

                                ps.setDouble(1, newValues[q]);
                                ps.setInt(2, task);
                                ps.setDouble(3, start_range[q]);
                                ps.setDouble(4, stop_range[q]);

                                ps.executeUpdate();
                            }
                        }

                        // All updates succeeded.
                        con.commit();

                        int finished =
                                tasks_finished.incrementAndGet();

                        System.out.println(
                                "Done task " + task +
                                        " after " + retries + " retries. " +
                                        "I am finisher number " + finished);

                        return;

                    } catch (SQLException e) {

                        // Roll back the failed transaction.
                        if (con != null) {
                            try {
                                con.rollback();
                            } catch (SQLException rollbackError) {
                                e.addSuppressed(rollbackError);
                            }
                        }

                        // PostgreSQL deadlock error: 40P01
                        if ("40P01".equals(e.getSQLState())) {

                            deadlocks.incrementAndGet();
                            retries++;

                            System.err.println(
                                    "Task " + task +
                                            ": deadlock detected. Retry " +
                                            retries);

                            // After 3 deadlocks, use the fair
                            // exclusive queue for future attempts.
                            if (retries ==
                                    MAX_CONCURRENT_DEADLOCKS) {

                                exclusiveFallbacks.incrementAndGet();

                                System.out.println(
                                        "Task " + task +
                                                " will use the fair exclusive queue.");
                            }

                        } else {

                            // Stop on other SQL errors.
                            System.err.println(
                                    "Task " + task +
                                            " failed: " + e.getMessage());

                            e.printStackTrace();
                            return;
                        }

                    } finally {

                        // Always close the database connection.
                        if (con != null) {
                            try {
                                con.close();
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        }
                    }

                } finally {

                    // Release the Java lock after each attempt.
                    gate.unlock();
                }
            }
        }
    }
}
