import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;

public class ProfileBenchmark {

    private static final int THREADS = 4;
    private static final int DURATION_SECONDS = 30;

    public static void main(String[] args) throws Exception {

        // Prepare the database before measurements.
        TransactionTask.resetInsertedRows();

        TransactionTask.Type[] types =
                TransactionTask.Type.values();

        AtomicLongArray counts =
                new AtomicLongArray(types.length);

        AtomicLongArray totalTimeNanos =
                new AtomicLongArray(types.length);

        AtomicLong committed = new AtomicLong();

        AtomicLong deadline = new AtomicLong();

        AtomicBoolean stop = new AtomicBoolean(false);

        AtomicReference<Throwable> failure =
                new AtomicReference<>();

        CountDownLatch startSignal = new CountDownLatch(1);

        ExecutorService pool =
                Executors.newFixedThreadPool(THREADS);

        try {
            List<Future<?>> workers = new ArrayList<>();

            System.out.println("=== TRANSACTION PROFILING ===");
            System.out.println("Threads: " + THREADS);
            System.out.println(
                    "Duration: " + DURATION_SECONDS + " seconds");

            for (int worker = 0; worker < THREADS; worker++) {

                workers.add(pool.submit(() -> {

                    try {
                        startSignal.await();

                        while (!stop.get()
                                && System.nanoTime() < deadline.get()) {

                            TransactionTask.Type type =
                                    TransactionTask.chooseType();

                            long transactionStart =
                                    System.nanoTime();

                            try {
                                switch (type) {

                                    case MULTI_POINT_READ ->
                                            TransactionTask
                                                    .runMultiPointRead();

                                    case INSERT ->
                                            TransactionTask
                                                    .runInsert();

                                    case FULL_TABLE_SCAN ->
                                            TransactionTask
                                                    .runFullTableScan();

                                    case UPDATE ->
                                            TransactionTask
                                                    .runUpdate();
                                }

                                long duration =
                                        System.nanoTime()
                                                - transactionStart;

                                counts.incrementAndGet(
                                        type.ordinal());

                                totalTimeNanos.addAndGet(
                                        type.ordinal(), duration);

                                committed.incrementAndGet();

                            } catch (Exception e) {
                                failure.compareAndSet(null, e);
                                stop.set(true);
                                break;
                            }
                        }

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        failure.compareAndSet(null, e);
                        stop.set(true);
                    }
                }));
            }

            long start = System.nanoTime();

            deadline.set(
                    start + TimeUnit.SECONDS.toNanos(
                            DURATION_SECONDS));

            startSignal.countDown();

            pool.shutdown();

            for (Future<?> worker : workers) {
                worker.get(90, TimeUnit.SECONDS);
            }

            long end = System.nanoTime();

            if (failure.get() != null) {
                throw new IllegalStateException(
                        "Profiling failed. A transaction did not commit.",
                        failure.get());
            }

            double elapsedSeconds =
                    (end - start) / 1_000_000_000.0;

            long combinedWorkerTime = 0;

            for (int i = 0; i < types.length; i++) {
                combinedWorkerTime += totalTimeNanos.get(i);
            }

            System.out.println();
            System.out.println("=== PROFILING RESULTS ===");

            System.out.printf(
                    "Actual execution time: %.3f seconds%n",
                    elapsedSeconds);

            System.out.println(
                    "Committed transactions: " + committed.get());

            System.out.printf(
                    "Throughput: %.2f transactions/second%n",
                    committed.get() / elapsedSeconds);

            System.out.println();
            System.out.printf(
                    "%-18s %8s %15s %16s%n",
                    "Type", "Count", "Avg time (ms)",
                    "Worker time (%)");

            for (TransactionTask.Type type : types) {

                int index = type.ordinal();

                long count = counts.get(index);
                long nanos = totalTimeNanos.get(index);

                double averageMs = count == 0
                        ? 0
                        : nanos / (double) count / 1_000_000.0;

                double workerShare = combinedWorkerTime == 0
                        ? 0
                        : 100.0 * nanos / combinedWorkerTime;

                System.out.printf(
                        "%-18s %8d %15.3f %15.1f%%%n",
                        type.name(),
                        count,
                        averageMs,
                        workerShare);
            }

            System.out.println();
            System.out.println(
                    "Detected deadlocks: "
                            + TransactionTask.getDeadlockCount());

            System.out.println(
                    "Exclusive switches: "
                            + TransactionTask.getExclusiveSwitchCount());

            System.out.println(
                    "All started transactions committed successfully.");

        } finally {
            pool.shutdownNow();
        }
    }
}
