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

public class ConcurrentBenchmark {

    private static final int THREADS = 8;
    private static final int DURATION_SECONDS = 30;

    public static void main(String[] args) {

        ExecutorService pool = null;

        try {
            // Prepare database before measuring throughput.
            TransactionTask.resetInsertedRows();

            AtomicLong committed = new AtomicLong();
            AtomicLongArray typeCounts = new AtomicLongArray(4);

            AtomicBoolean stop = new AtomicBoolean(false);
            AtomicReference<Throwable> firstFailure =
                    new AtomicReference<>();

            CountDownLatch startSignal = new CountDownLatch(1);

            pool = Executors.newFixedThreadPool(THREADS);

            List<Future<?>> workers = new ArrayList<>();

            System.out.println();
            System.out.println("=== HOMEWORK 3 BENCHMARK ===");
            System.out.println("Threads: " + THREADS);
            System.out.println(
                    "Duration: " + DURATION_SECONDS + " seconds");
            System.out.println();

            // Create workers before starting the timer.
            for (int i = 0; i < THREADS; i++) {

                workers.add(pool.submit(() -> {

                    try {
                        startSignal.await();

                        while (!stop.get()
                                && System.nanoTime() < deadlineHolder[0]) {

                            TransactionTask.Type type =
                                    TransactionTask.chooseType();

                            try {
                                switch (type) {

                                    case MULTI_POINT_READ ->
                                            TransactionTask.runMultiPointRead();

                                    case INSERT ->
                                            TransactionTask.runInsert();

                                    case FULL_TABLE_SCAN ->
                                            TransactionTask.runFullTableScan();

                                    case UPDATE ->
                                            TransactionTask.runUpdate();
                                }

                                committed.incrementAndGet();
                                typeCounts.incrementAndGet(type.ordinal());

                            } catch (Exception e) {

                                firstFailure.compareAndSet(null, e);
                                stop.set(true);
                            }
                        }

                    } catch (InterruptedException e) {

                        Thread.currentThread().interrupt();
                        firstFailure.compareAndSet(null, e);
                        stop.set(true);
                    }
                }));
            }

            // Start all workers with the same deadline.
            long start = System.nanoTime();

            deadlineHolder[0] = start +
                    TimeUnit.SECONDS.toNanos(DURATION_SECONDS);

            startSignal.countDown();

            pool.shutdown();

            // Wait for every already-started transaction to finish.
            for (Future<?> worker : workers) {
                worker.get();
            }

            long end = System.nanoTime();

            double elapsed =
                    (end - start) / 1_000_000_000.0;

            double throughput =
                    committed.get() / elapsed;

            System.out.println();
            System.out.println("=== BENCHMARK RESULTS ===");
            System.out.println("Threads: " + THREADS);
            System.out.println(
                    "Committed transactions: " + committed.get());

            System.out.println(
                    "Multi-point READ: " + typeCounts.get(0));
            System.out.println(
                    "INSERT: " + typeCounts.get(1));
            System.out.println(
                    "Full-table SUM: " + typeCounts.get(2));
            System.out.println(
                    "UPDATE: " + typeCounts.get(3));

            System.out.println(
                    "Detected deadlocks: " +
                            TransactionTask.getDeadlockCount());

            System.out.println(
                    "Exclusive mode switches: " +
                            TransactionTask.getExclusiveSwitchCount());

            System.out.printf(
                    "Actual execution time: %.3f seconds%n",
                    elapsed);

            System.out.printf(
                    "Throughput: %.2f transactions/second%n",
                    throughput);

            if (firstFailure.get() != null) {

                System.err.println();
                System.err.println(
                        "BENCHMARK INVALID: a transaction failed.");

                firstFailure.get().printStackTrace();

                throw new IllegalStateException(
                        "Not all transactions committed successfully.");
            }

            System.out.println(
                    "All started transactions committed successfully.");

        } catch (Exception e) {

            System.err.println("Benchmark failed.");
            e.printStackTrace();

        } finally {

            if (pool != null) {
                pool.shutdownNow();
            }
        }
    }

    // Shared deadline, assigned before workers are released.
    private static final long[] deadlineHolder = new long[1];
}
