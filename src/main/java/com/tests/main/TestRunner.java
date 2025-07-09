package com.tests.main;

import com.tests.main.utils.ESUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.concurrent.*;

import static com.tests.main.utils.TestUtil.cleanUpAll;

public class TestRunner {
    private static final Logger LOG = LoggerFactory.getLogger(TestRunner.class);

    private static final int THREAD_COUNT = 2;

    public static void runTests(Class<?> testClass) throws Exception {
        long start = System.currentTimeMillis();

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
        BlockingQueue<Method> queue = new LinkedBlockingQueue<>();

        // Collect all test methods
        for (Method method : testClass.getDeclaredMethods()) {
            if (method.isAnnotationPresent(Test.class)) {
                queue.offer(method);
            }
        }

        // Process methods with 2 threads
        for (int i = 0; i < THREAD_COUNT; i++) {
            executor.submit(() -> {
                while (!queue.isEmpty()) {
                    Method method = queue.poll();
                    if (method != null) {
                        try {
                            Object instance = testClass.getDeclaredConstructor().newInstance();
                            method.setAccessible(true);
                            System.out.println("Executing: " + method.getName() + " on " + Thread.currentThread().getName());
                            Thread.currentThread().setName(method.getName());
                            method.invoke(instance);
                        } catch (Exception e) {
                            System.err.println("Test failed: " + method.getName());
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(30, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            LOG.info("Completed running {} tests, took {} seconds", testClass, (System.currentTimeMillis() - start) / 1000);
            cleanUpAll();
            ESUtil.close();
        }
    }
}
