package com.tests.main.sanity.delete;

import com.tests.main.sanity.delete.aggregation.SanityDeleteColumnOperations;
import com.tests.main.sanity.delete.aggregation.SanityTableDeleteOperations;
import com.tests.main.sanity.delete.association.SanityAssociationDeleteOperations;
import com.tests.main.sanity.delete.association.SanityAssociationDeleteOperationsInverse;
import com.tests.main.sanity.delete.composition.SanityCategoryCompositionDeleteOperations;
import com.tests.main.sanity.delete.composition.SanityCompositionDeleteOperations;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.tests.main.utils.TestUtil.cleanUpAll;

public class DeleteSanityDriver {
    private static final Logger LOG = LoggerFactory.getLogger(DeleteSanityDriver.class);
    private static final int PARALLEL_THREADS = 1;
    private static final int TEST_TIMEOUT_MINUTES = 30;
    private static final int SHUTDOWN_TIMEOUT_MINUTES = 5;

    public static void main(String[] args) throws Exception {
        try {
            // Create a queue of all test classes
            Queue<Class<? extends TestsMain>> testQueue = new LinkedList<>();
            testQueue.add(SanityDeleteColumnOperations.class);
            testQueue.add(SanityTableDeleteOperations.class);
            testQueue.add(SanityAssociationDeleteOperations.class);
            testQueue.add(SanityAssociationDeleteOperationsInverse.class);
            testQueue.add(SanityCategoryCompositionDeleteOperations.class);
            testQueue.add(SanityCompositionDeleteOperations.class);

            // Create a thread pool for parallel execution
            ExecutorService executor = Executors.newFixedThreadPool(PARALLEL_THREADS);

            try {
                // Process tests in batches of PARALLEL_THREADS
                while (!testQueue.isEmpty()) {
                    List<Future<?>> batchFutures = new ArrayList<>();
                    
                    // Submit up to PARALLEL_THREADS tests to run in parallel
                    for (int i = 0; i < PARALLEL_THREADS && !testQueue.isEmpty(); i++) {
                        Class<? extends TestsMain> testClass = testQueue.poll();
                        batchFutures.add(executor.submit(() -> {
                            try {
                                String testName = testClass.getSimpleName();
                                LOG.info("Starting {}", testName);
                                testClass.getDeclaredConstructor().newInstance().run();
                                LOG.info("Completed {}", testName);
                            } catch (Exception e) {
                                LOG.error("Error in {}", testClass.getSimpleName(), e);
                                throw new RuntimeException(e);
                            }
                        }));
                    }

                    // Wait for current batch to complete
                    for (Future<?> future : batchFutures) {
                        try {
                            future.get(TEST_TIMEOUT_MINUTES, TimeUnit.MINUTES);
                        } catch (Exception e) {
                            LOG.error("Test execution failed", e);
                            throw new RuntimeException(e);
                        }
                    }
                }
            } finally {
                // Shutdown the executor
                executor.shutdown();
                if (!executor.awaitTermination(SHUTDOWN_TIMEOUT_MINUTES, TimeUnit.MINUTES)) {
                    LOG.error("Executor did not terminate in time");
                    executor.shutdownNow();
                }
            }
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }
}
