import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

public class TimingProbe {

    private static final int WARMUP_RUNS = 5;
    private static final int MEASURED_RUNS = 30;

    record Timings(
            long connection,
            long setup,
            long sql,
            long commit,
            long close,
            long total) {
    }

    public static void main(String[] args) {
        try {
            System.out.println("=== JDBC AND SQL TIMING PROBE ===");
            System.out.println("Measured runs: " + MEASURED_RUNS);
            System.out.println();

            profile("MULTI_POINT_READ", false);
            profile("FULL_TABLE_SCAN", true);

        } catch (Exception e) {
            System.err.println("Timing probe failed:");
            e.printStackTrace();
        }
    }

    private static void profile(
            String name,
            boolean fullTableScan) throws SQLException {

        // Warm up before collecting measurements.
        for (int i = 0; i < WARMUP_RUNS; i++) {
            measure(fullTableScan);
        }

        long[] totals = new long[6];

        for (int i = 0; i < MEASURED_RUNS; i++) {
            Timings t = measure(fullTableScan);

            totals[0] += t.connection();
            totals[1] += t.setup();
            totals[2] += t.sql();
            totals[3] += t.commit();
            totals[4] += t.close();
            totals[5] += t.total();
        }

        System.out.println("--- " + name + " ---");

        printAverage("JDBC connection", totals[0]);
        printAverage("Transaction setup", totals[1]);
        printAverage("SQL execution", totals[2]);
        printAverage("COMMIT", totals[3]);
        printAverage("Connection close", totals[4]);
        printAverage("TOTAL", totals[5]);

        System.out.println();
    }

    private static void printAverage(
            String label, long totalNanos) {

        double averageMs =
                totalNanos / (double) MEASURED_RUNS
                        / 1_000_000.0;

        System.out.printf(
                Locale.US,
                "%-22s %10.3f ms%n",
                label + ":",
                averageMs);
    }

    private static Timings measure(
            boolean fullTableScan) throws SQLException {

        // Generate random IDs outside the timed interval.
        int[] ids = fullTableScan
                ? null
                : TransactionTask.chooseDistinctIds();

        long t0 = System.nanoTime();

        long t1;
        long t2;
        long t3;
        long t4;

        try (Connection con =
                     ConnectionFactory.getDefaultParameterConnection(
                             ConnectionFactory.POSTGRESQL)) {

            t1 = System.nanoTime();

            con.setAutoCommit(false);
            con.setTransactionIsolation(
                    Connection.TRANSACTION_READ_COMMITTED);

            t2 = System.nanoTime();

            try {
                if (fullTableScan) {
                    executeFullTableScan(con);
                } else {
                    executeMultiPointRead(con, ids);
                }

                t3 = System.nanoTime();

                con.commit();

                t4 = System.nanoTime();

            } catch (SQLException e) {
                con.rollback();
                throw e;
            }
        }

        long t5 = System.nanoTime();

        return new Timings(
                t1 - t0,
                t2 - t1,
                t3 - t2,
                t4 - t3,
                t5 - t4,
                t5 - t0
        );
    }

    private static void executeMultiPointRead(
            Connection con,
            int[] ids) throws SQLException {

        String sql =
                "SELECT data_value FROM blocking_data " +
                        "WHERE data_id = ?";

        double sum = 0.0;

        try (PreparedStatement ps =
                     con.prepareStatement(sql)) {

            for (int id : ids) {
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

        if (!Double.isFinite(sum)) {
            throw new SQLException("Invalid READ sum");
        }
    }

    private static void executeFullTableScan(
            Connection con) throws SQLException {

        String sql =
                "SELECT SUM(data_value) FROM blocking_data";

        try (PreparedStatement ps =
                     con.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            if (!rs.next()) {
                throw new SQLException(
                        "Full-table scan returned no result");
            }

            double sum = rs.getDouble(1);

            if (!Double.isFinite(sum)) {
                throw new SQLException("Invalid table sum");
            }
        }
    }
}
