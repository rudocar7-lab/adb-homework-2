import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Random;

public class StatementReuseProbe {

    private static final int ROWS = 50000;
    private static final int WARMUP = 200;
    private static final int RUNS = 3000;

    private static final String SQL =
            "SELECT data_value FROM blocking_data " +
                    "WHERE data_id = ?";

    private record Result(double milliseconds, double checksum) {
    }

    private static int[][] makePlans(int count) {

        Random random = new Random(42);
        int[][] plans = new int[count][3];

        for (int i = 0; i < count; i++) {
            for (int j = 0; j < 3; j++) {

                int id;
                boolean duplicate;

                do {
                    id = random.nextInt(ROWS);
                    duplicate = false;

                    for (int k = 0; k < j; k++) {
                        if (plans[i][k] == id) {
                            duplicate = true;
                            break;
                        }
                    }
                } while (duplicate);

                plans[i][j] = id;
            }
        }

        return plans;
    }

    private static Result measure(
            Connection con,
            int[][] plans,
            boolean reuse) throws SQLException {

        long start = System.nanoTime();
        double checksum = 0;

        PreparedStatement reusable =
                reuse ? con.prepareStatement(SQL) : null;

        try {
            for (int[] ids : plans) {

                PreparedStatement ps = reuse
                        ? reusable
                        : con.prepareStatement(SQL);

                try {
                    double sum = 0;

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

                    con.commit();
                    checksum += sum;

                } catch (SQLException e) {
                    con.rollback();
                    throw e;

                } finally {
                    if (!reuse) {
                        ps.close();
                    }
                }
            }

        } finally {
            if (reusable != null) {
                reusable.close();
            }
        }

        double milliseconds =
                (System.nanoTime() - start) / 1_000_000.0;

        return new Result(milliseconds, checksum);
    }

    private static void print(
            String label,
            Result result,
            int runs) {

        System.out.printf(
                Locale.US,
                "%-25s %.3f ms/transaction%n",
                label,
                result.milliseconds() / runs);
    }

    public static void main(String[] args)
            throws Exception {

        int[][] warmupPlans = makePlans(WARMUP);
        int[][] plans = makePlans(RUNS);

        try (Connection con =
                     ConnectionFactory.getDefaultParameterConnection(
                             ConnectionFactory.POSTGRESQL)) {

            con.setAutoCommit(false);
            con.setTransactionIsolation(
                    Connection.TRANSACTION_READ_COMMITTED);

            // Warm up both execution modes.
            measure(con, warmupPlans, false);
            measure(con, warmupPlans, true);

            System.out.println("=== STATEMENT REUSE TEST ===");
            System.out.println(
                    "Same connection and same SELECT inputs");
            System.out.println(
                    "Transactions per measurement: " + RUNS);

            // Reverse the order in the second round.
            Result fresh1 = measure(con, plans, false);
            Result reused1 = measure(con, plans, true);

            Result reused2 = measure(con, plans, true);
            Result fresh2 = measure(con, plans, false);

            print("New statement - run 1", fresh1, RUNS);
            print("Reused statement - run 1", reused1, RUNS);
            print("Reused statement - run 2", reused2, RUNS);
            print("New statement - run 2", fresh2, RUNS);

            double freshAverage =
                    (fresh1.milliseconds()
                            + fresh2.milliseconds())
                            / (2.0 * RUNS);

            double reusedAverage =
                    (reused1.milliseconds()
                            + reused2.milliseconds())
                            / (2.0 * RUNS);

            System.out.println();
            System.out.printf(
                    Locale.US,
                    "New statement average: %.3f ms%n",
                    freshAverage);

            System.out.printf(
                    Locale.US,
                    "Reused statement average: %.3f ms%n",
                    reusedAverage);

            System.out.printf(
                    Locale.US,
                    "Speedup: %.2fx%n",
                    freshAverage / reusedAverage);

            boolean sumsMatch =
                    Math.abs(fresh1.checksum()
                            - reused1.checksum()) < 0.000001
                            && Math.abs(fresh2.checksum()
                            - reused2.checksum()) < 0.000001;

            System.out.println(
                    "Same query results: " + sumsMatch);

            if (!sumsMatch) {
                throw new IllegalStateException(
                        "Query results differ!");
            }
        }
    }
}
