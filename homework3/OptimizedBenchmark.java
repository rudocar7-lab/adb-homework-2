import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OptimizedBenchmark {

    private static final int INITIAL_ROWS = 50_000;
    private static final int QUERIES_PER_TASK = 3;
    private static final int EXCLUSIVE_AFTER_CONFLICTS = 3;

    private static final AtomicInteger NEXT_INSERT_ID =
            new AtomicInteger(INITIAL_ROWS);

    private static final ReentrantReadWriteLock FAIR_GATE =
            new ReentrantReadWriteLock(true);

    private enum Type {
        MULTI_POINT_READ,
        INSERT,
        FULL_TABLE_SCAN,
        UPDATE
    }

    private record TaskPlan(
            Type type,
            int[] ids,
            double[] values,
            int insertId,
            double insertValue) {
    }

    private record WorkerStats(
            long[] counts,
            long[] timeNanos,
            long deadlocks,
            long exclusiveSwitches) {
    }

    public static void main(String[] args) throws Exception {

        int threads = args.length >= 1
                ? Integer.parseInt(args[0]) : 4;

        int durationSeconds = args.length >= 2
                ? Integer.parseInt(args[1]) : 30;

        if (threads < 1 || threads > 64
                || durationSeconds < 1
                || durationSeconds > 600) {

            throw new IllegalArgumentException(
                    "Use: OptimizedBenchmark " +
                            "[threads 1..64] [seconds 1..600]");
        }

        resetInsertedRows();

        List<Worker> workers = new ArrayList<>();
        ExecutorService executor = null;

        try {

            // Create connections and prepared statements once,
            // before measuring transaction throughput.
            for (int i = 0; i < threads; i++) {
                workers.add(new Worker());
            }

            executor = Executors.newFixedThreadPool(threads);

            java.util.concurrent.CountDownLatch go =
                    new java.util.concurrent.CountDownLatch(1);

            java.util.concurrent.atomic.AtomicLong deadline =
                    new java.util.concurrent.atomic.AtomicLong();

            List<Future<WorkerStats>> futures =
                    new ArrayList<>();

            for (Worker worker : workers) {

                futures.add(executor.submit(() -> {

                    go.await();

                    return worker.processUntil(deadline.get());
                }));
            }

            System.out.println("=== OPTIMIZED BENCHMARK ===");
            System.out.println("Threads: " + threads);
            System.out.println(
                    "Target duration: " +
                            durationSeconds + " seconds");

            long start = System.nanoTime();

            deadline.set(
                    start + TimeUnit.SECONDS.toNanos(
                            durationSeconds));

            go.countDown();

            long[] counts = new long[Type.values().length];
            long[] workTime = new long[Type.values().length];

            long deadlocks = 0;
            long exclusiveSwitches = 0;

            try {

                for (Future<WorkerStats> future : futures) {

                    WorkerStats result = future.get();

                    for (int i = 0; i < counts.length; i++) {

                        counts[i] += result.counts()[i];
                        workTime[i] += result.timeNanos()[i];
                    }

                    deadlocks += result.deadlocks();

                    exclusiveSwitches +=
                            result.exclusiveSwitches();
                }

            } catch (ExecutionException e) {

                throw new IllegalStateException(
                        "BENCHMARK INVALID: a transaction failed",
                        e.getCause());
            }

            long end = System.nanoTime();

            long committed = Arrays.stream(counts).sum();

            double elapsedSeconds =
                    (end - start) / 1_000_000_000.0;

            System.out.println();
            System.out.println("=== OPTIMIZED RESULTS ===");

            System.out.println("Threads: " + threads);

            System.out.println(
                    "Committed transactions: " + committed);

            for (Type type : Type.values()) {

                int i = type.ordinal();

                double avgMs = counts[i] == 0
                        ? 0
                        : workTime[i] /
                        (double) counts[i] /
                        1_000_000.0;

                System.out.printf(
                        java.util.Locale.US,
                        "%-17s %6d   avg %.3f ms%n",
                        type.name(),
                        counts[i],
                        avgMs);
            }

            System.out.println(
                    "Detected deadlocks: " + deadlocks);

            System.out.println(
                    "Exclusive mode switches: " +
                            exclusiveSwitches);

            System.out.printf(
                    java.util.Locale.US,
                    "Actual execution time: %.3f seconds%n",
                    elapsedSeconds);

            System.out.printf(
                    java.util.Locale.US,
                    "Throughput: %.2f transactions/second%n",
                    committed / elapsedSeconds);

            System.out.println(
                    "All started transactions committed successfully.");

        } finally {

            if (executor != null) {

                executor.shutdownNow();

                executor.awaitTermination(
                        30, TimeUnit.SECONDS);
            }

            for (Worker worker : workers) {
                worker.close();
            }
        }
    }

    // Clear rows inserted by earlier benchmark runs.
    private static void resetInsertedRows()
            throws SQLException {

        try (Connection con =
                     ConnectionFactory
                             .getDefaultParameterConnection(
                                     ConnectionFactory.POSTGRESQL);

             PreparedStatement ps =
                     con.prepareStatement(
                             "DELETE FROM blocking_data " +
                                     "WHERE data_id >= ?")) {

            ps.setInt(1, INITIAL_ROWS);

            int removed = ps.executeUpdate();

            NEXT_INSERT_ID.set(INITIAL_ROWS);

            System.out.println(
                    "Removed previous HW3 INSERT rows: " +
                            removed);
        }
    }

    // Generate a new transaction with fixed input data.
    private static TaskPlan makePlan() {

        double p = ThreadLocalRandom.current().nextDouble();

        Type type = p < 0.6
                ? Type.MULTI_POINT_READ
                : p < 0.7
                ? Type.INSERT
                : p < 0.8
                ? Type.FULL_TABLE_SCAN
                : Type.UPDATE;

        if (type == Type.FULL_TABLE_SCAN) {

            return new TaskPlan(
                    type, null, null, -1, 0);
        }

        if (type == Type.INSERT) {

            return new TaskPlan(
                    type,
                    null,
                    null,
                    NEXT_INSERT_ID.getAndIncrement(),
                    ThreadLocalRandom.current().nextDouble(50.0));
        }

        int[] ids = new int[QUERIES_PER_TASK];

        for (int i = 0; i < ids.length; i++) {

            int candidate;
            boolean duplicate;

            do {

                candidate = ThreadLocalRandom.current()
                        .nextInt(INITIAL_ROWS);

                duplicate = false;

                for (int j = 0; j < i; j++) {

                    if (ids[j] == candidate) {
                        duplicate = true;
                        break;
                    }
                }

            } while (duplicate);

            ids[i] = candidate;
        }

        double[] values = null;

        if (type == Type.UPDATE) {

            values = new double[QUERIES_PER_TASK];

            for (int i = 0; i < values.length; i++) {

                values[i] = ThreadLocalRandom.current()
                        .nextDouble(50.0);
            }
        }

        return new TaskPlan(
                type, ids, values, -1, 0);
    }

    // Each worker owns one JDBC connection and
    // reuses its prepared SQL statements.
    private static final class Worker
            implements AutoCloseable {

        private final Connection con;

        private final PreparedStatement pointRead;
        private final PreparedStatement insert;
        private final PreparedStatement fullScan;
        private final PreparedStatement update;

        private long deadlocks;
        private long exclusiveSwitches;

        private Worker() throws SQLException {

            con = ConnectionFactory
                    .getDefaultParameterConnection(
                            ConnectionFactory.POSTGRESQL);

            con.setAutoCommit(false);

            con.setTransactionIsolation(
                    Connection.TRANSACTION_READ_COMMITTED);

            pointRead = con.prepareStatement(
                    "SELECT data_value " +
                            "FROM blocking_data WHERE data_id = ?");

            insert = con.prepareStatement(
                    "INSERT INTO blocking_data " +
                            "(data_id, data_value, modified_by) " +
                            "VALUES (?, ?, ?)");

            fullScan = con.prepareStatement(
                    "SELECT SUM(data_value) " +
                            "FROM blocking_data");

            update = con.prepareStatement(
                    "UPDATE blocking_data " +
                            "SET data_value = ?, modified_by = ? " +
                            "WHERE data_id = ?");
        }

        private WorkerStats processUntil(long deadline)
                throws SQLException {

            long[] counts =
                    new long[Type.values().length];

            long[] timeNanos =
                    new long[Type.values().length];

            // Stop starting new tasks after the deadline.
            // Already-started transactions finish normally.
            while (System.nanoTime() < deadline) {

                TaskPlan plan = makePlan();

                long taskStart = System.nanoTime();

                double sum = executeWithRetry(plan);

                // Required console output for read transactions.
                if (plan.type() == Type.MULTI_POINT_READ) {

                    System.out.println(
                            "READ ids=" +
                                    Arrays.toString(plan.ids()) +
                                    " sum=" + sum);

                } else if (plan.type() ==
                        Type.FULL_TABLE_SCAN) {

                    System.out.println(
                            "FULL-TABLE SUM=" + sum);
                }

                long elapsed =
                        System.nanoTime() - taskStart;

                int index = plan.type().ordinal();

                counts[index]++;

                timeNanos[index] += elapsed;
            }

            return new WorkerStats(
                    counts,
                    timeNanos,
                    deadlocks,
                    exclusiveSwitches);
        }

        // Retry the same transaction after transient errors.
        private double executeWithRetry(TaskPlan plan)
                throws SQLException {

            int conflicts = 0;
            boolean exclusive = false;

            while (true) {

                Lock gate = exclusive
                        ? FAIR_GATE.writeLock()
                        : FAIR_GATE.readLock();

                gate.lock();

                try {

                    try {

                        double result = executeSql(plan);

                        con.commit();

                        return result;

                    } catch (SQLException e) {

                        try {

                            con.rollback();

                        } catch (SQLException rollbackError) {

                            e.addSuppressed(rollbackError);

                            throw e;
                        }

                        String state = e.getSQLState();

                        if (!"40P01".equals(state)
                                && !"40001".equals(state)) {

                            throw e;
                        }

                        if ("40P01".equals(state)) {
                            deadlocks++;
                        }

                        conflicts++;

                        if (conflicts >=
                                EXCLUSIVE_AFTER_CONFLICTS
                                && !exclusive) {

                            exclusive = true;
                            exclusiveSwitches++;
                        }
                    }

                } finally {

                    gate.unlock();
                }

                // Retry with exactly the same transaction plan.
            }
        }

        private double executeSql(TaskPlan plan)
                throws SQLException {

            switch (plan.type()) {

                case MULTI_POINT_READ -> {

                    double sum = 0;

                    // Three separate SELECT statements.
                    for (int id : plan.ids()) {

                        pointRead.setInt(1, id);

                        try (ResultSet rs =
                                     pointRead.executeQuery()) {

                            if (!rs.next()) {

                                throw new SQLException(
                                        "Missing row: " + id);
                            }

                            sum += rs.getDouble(1);
                        }
                    }

                    return sum;
                }

                case INSERT -> {

                    insert.setInt(1, plan.insertId());

                    insert.setDouble(
                            2, plan.insertValue());

                    insert.setInt(3, 0);

                    if (insert.executeUpdate() != 1) {

                        throw new SQLException(
                                "INSERT did not affect " +
                                        "exactly one row");
                    }
                }

                case FULL_TABLE_SCAN -> {

                    try (ResultSet rs =
                                 fullScan.executeQuery()) {

                        if (!rs.next()) {

                            throw new SQLException(
                                    "SUM returned no result");
                        }

                        return rs.getDouble(1);
                    }
                }

                case UPDATE -> {

                    // Preserve the original UPDATE order.
                    for (int i = 0;
                         i < plan.ids().length;
                         i++) {

                        update.setDouble(
                                1, plan.values()[i]);

                        update.setInt(2, 1);

                        update.setInt(
                                3, plan.ids()[i]);

                        if (update.executeUpdate() != 1) {

                            throw new SQLException(
                                    "UPDATE did not affect one row: " +
                                            plan.ids()[i]);
                        }
                    }
                }
            }

            return Double.NaN;
        }

        @Override
        public void close() throws SQLException {

            SQLException failure = null;

            for (AutoCloseable resource :
                    new AutoCloseable[]{
                            pointRead,
                            insert,
                            fullScan,
                            update,
                            con}) {

                try {

                    resource.close();

                } catch (Exception e) {

                    if (failure == null) {

                        failure = new SQLException(
                                "Unable to close a JDBC resource",
                                e);

                    } else {

                        failure.addSuppressed(e);
                    }
                }
            }

            if (failure != null) {
                throw failure;
            }
        }
    }
}
