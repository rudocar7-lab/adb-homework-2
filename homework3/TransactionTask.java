import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TransactionTask {

    private static final int NUM_TUPLES = 50000;
    private static final int NUM_QUERIES_PER_TASK = 3;
    private static final double MAX_VALUE = 50.0;

    private static final AtomicInteger NEXT_INSERT_ID =
            new AtomicInteger(NUM_TUPLES);

    // Regular tasks run concurrently.
    // After repeated deadlocks, a task can run exclusively.
    private static final ReentrantReadWriteLock FAIR_GATE =
            new ReentrantReadWriteLock(true);

    private static final AtomicLong DEADLOCKS = new AtomicLong();
    private static final AtomicLong EXCLUSIVE_SWITCHES = new AtomicLong();

    enum Type {
        MULTI_POINT_READ,
        INSERT,
        FULL_TABLE_SCAN,
        UPDATE
    }

    // Keep the same transaction data on every retry.
    private record TaskPlan(
            Type type,
            int[] ids,
            double[] newValues,
            int insertId,
            double insertValue) {
    }

    static Type chooseType() {
        double p = ThreadLocalRandom.current().nextDouble();

        if (p < 0.6) return Type.MULTI_POINT_READ;
        if (p < 0.7) return Type.INSERT;
        if (p < 0.8) return Type.FULL_TABLE_SCAN;

        return Type.UPDATE;
    }

    static int[] chooseDistinctIds() {
        int[] ids = new int[NUM_QUERIES_PER_TASK];

        for (int i = 0; i < ids.length; i++) {
            int candidate;
            boolean repeated;

            do {
                candidate = ThreadLocalRandom.current()
                        .nextInt(NUM_TUPLES);

                repeated = false;

                for (int j = 0; j < i; j++) {
                    if (ids[j] == candidate) {
                        repeated = true;
                        break;
                    }
                }
            } while (repeated);

            ids[i] = candidate;
        }

        return ids;
    }

    private static TaskPlan prepareTask(Type type) {
        return switch (type) {

            case MULTI_POINT_READ ->
                    new TaskPlan(
                            type,
                            chooseDistinctIds(),
                            null,
                            -1,
                            0
                    );

            case INSERT ->
                    new TaskPlan(
                            type,
                            null,
                            null,
                            NEXT_INSERT_ID.getAndIncrement(),
                            ThreadLocalRandom.current()
                                    .nextDouble(MAX_VALUE)
                    );

            case FULL_TABLE_SCAN ->
                    new TaskPlan(type, null, null, -1, 0);

            case UPDATE -> {
                int[] ids = chooseDistinctIds();

                double[] values =
                        new double[NUM_QUERIES_PER_TASK];

                for (int i = 0; i < values.length; i++) {
                    values[i] = ThreadLocalRandom.current()
                            .nextDouble(MAX_VALUE);
                }

                yield new TaskPlan(type, ids, values, -1, 0);
            }
        };
    }

    static void runMultiPointRead() throws SQLException {
        execute(prepareTask(Type.MULTI_POINT_READ));
    }

    static void runInsert() throws SQLException {
        execute(prepareTask(Type.INSERT));
    }

    static void runFullTableScan() throws SQLException {
        execute(prepareTask(Type.FULL_TABLE_SCAN));
    }

    static void runUpdate() throws SQLException {
        execute(prepareTask(Type.UPDATE));
    }

    static long getDeadlockCount() {
        return DEADLOCKS.get();
    }

    static long getExclusiveSwitchCount() {
        return EXCLUSIVE_SWITCHES.get();
    }

    // Run this only before starting benchmark workers.
    static void resetInsertedRows() throws SQLException {

        try (Connection con =
                     ConnectionFactory.getDefaultParameterConnection(
                             ConnectionFactory.POSTGRESQL);

             PreparedStatement ps = con.prepareStatement(
                     "DELETE FROM blocking_data WHERE data_id >= ?")) {

            ps.setInt(1, NUM_TUPLES);

            int removed = ps.executeUpdate();

            NEXT_INSERT_ID.set(NUM_TUPLES);
            DEADLOCKS.set(0);
            EXCLUSIVE_SWITCHES.set(0);

            System.out.println(
                    "Removed previous HW3 inserted rows: " + removed);
        }
    }

    // Execute a task and retry transient transaction errors.
    private static void execute(TaskPlan plan) throws SQLException {

        int deadlocksForThisTask = 0;
        boolean exclusive = false;

        while (true) {

            Lock gate = exclusive
                    ? FAIR_GATE.writeLock()
                    : FAIR_GATE.readLock();

            gate.lock();

            try (Connection con =
                         ConnectionFactory.getDefaultParameterConnection(
                                 ConnectionFactory.POSTGRESQL)) {

                con.setAutoCommit(false);
                con.setTransactionIsolation(
                        Connection.TRANSACTION_READ_COMMITTED);

                try {
                    double sum = executeSql(con, plan);

                    con.commit();

                    // Required read results stay visible.
                    // Unnecessary diagnostic printing is removed.
                    if (plan.type() == Type.MULTI_POINT_READ) {

                        System.out.println(
                                "READ ids=" +
                                        Arrays.toString(plan.ids()) +
                                        " sum=" + sum
                        );

                    } else if (plan.type() == Type.FULL_TABLE_SCAN) {

                        System.out.println(
                                "FULL-TABLE SUM=" + sum
                        );
                    }

                    return;

                } catch (SQLException error) {

                    try {
                        con.rollback();
                    } catch (SQLException rollbackError) {
                        error.addSuppressed(rollbackError);
                    }

                    String sqlState = error.getSQLState();

                    if ("40P01".equals(sqlState)
                            || "40001".equals(sqlState)) {

                        if ("40P01".equals(sqlState)) {

                            DEADLOCKS.incrementAndGet();
                            deadlocksForThisTask++;

                            if (deadlocksForThisTask >= 3
                                    && !exclusive) {

                                exclusive = true;
                                EXCLUSIVE_SWITCHES.incrementAndGet();
                            }
                        }

                        // Retry the same transaction after
                        // releasing the current Java lock.

                    } else {
                        // A non-retryable SQL error is reported.
                        throw error;
                    }
                }

            } finally {
                gate.unlock();
            }
        }
    }

    private static double executeSql(
            Connection con,
            TaskPlan plan) throws SQLException {

        switch (plan.type()) {

            case MULTI_POINT_READ -> {

                double sum = 0;

                String sql =
                        "SELECT data_value FROM blocking_data " +
                                "WHERE data_id = ?";

                try (PreparedStatement ps =
                             con.prepareStatement(sql)) {

                    for (int id : plan.ids()) {

                        ps.setInt(1, id);

                        try (ResultSet rs = ps.executeQuery()) {

                            if (!rs.next()) {
                                throw new SQLException(
                                        "Missing row: " + id);
                            }

                            sum += rs.getDouble(1);
                        }
                    }
                }

                return sum;
            }

            case INSERT -> {

                String sql =
                        "INSERT INTO blocking_data " +
                                "(data_id, data_value, modified_by) " +
                                "VALUES (?, ?, ?)";

                try (PreparedStatement ps =
                             con.prepareStatement(sql)) {

                    ps.setInt(1, plan.insertId());
                    ps.setDouble(2, plan.insertValue());
                    ps.setInt(3, 0);

                    if (ps.executeUpdate() != 1) {
                        throw new SQLException(
                                "INSERT did not affect exactly one row");
                    }
                }
            }

            case FULL_TABLE_SCAN -> {

                String sql =
                        "SELECT SUM(data_value) FROM blocking_data";

                try (PreparedStatement ps =
                             con.prepareStatement(sql);

                     ResultSet rs = ps.executeQuery()) {

                    if (!rs.next()) {
                        throw new SQLException(
                                "SUM returned no result");
                    }

                    return rs.getDouble(1);
                }
            }

            case UPDATE -> {

                String sql =
                        "UPDATE blocking_data " +
                                "SET data_value = ?, modified_by = ? " +
                                "WHERE data_id = ?";

                try (PreparedStatement ps =
                             con.prepareStatement(sql)) {

                    // Preserve the original UPDATE order.
                    for (int i = 0;
                         i < plan.ids().length;
                         i++) {

                        ps.setDouble(1, plan.newValues()[i]);
                        ps.setInt(2, 1);
                        ps.setInt(3, plan.ids()[i]);

                        if (ps.executeUpdate() != 1) {
                            throw new SQLException(
                                    "UPDATE did not affect one row: " +
                                            plan.ids()[i]);
                        }
                    }
                }
            }
        }

        return Double.NaN;
    }

    public static void main(String[] args) {

        try {
            resetInsertedRows();

            System.out.println("--- READ ---");
            runMultiPointRead();

            System.out.println("--- INSERT ---");
            runInsert();
            System.out.println("INSERT committed");

            System.out.println("--- FULL TABLE SCAN ---");
            runFullTableScan();

            System.out.println("--- UPDATE ---");
            runUpdate();
            System.out.println("UPDATE committed");

            System.out.println(
                    "All four test transactions committed successfully.");

        } catch (Exception e) {

            System.err.println("Transaction test failed:");
            e.printStackTrace();
        }
    }
}
