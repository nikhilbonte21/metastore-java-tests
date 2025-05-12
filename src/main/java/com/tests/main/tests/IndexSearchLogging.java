package com.tests.main.tests;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.discovery.IndexSearchParams;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.tests.main.utils.TestUtil.*;
import static org.junit.Assert.*;


public class IndexSearchLogging implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(IndexSearchLogging.class);

    public static void main(String[] args) throws Exception {
        try {
            new IndexSearchLogging().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running IndexSearchLogging tests");

        long start = System.currentTimeMillis();
        try {

            concurrentIndexsearchRequests();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running IndexSearchLogging tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }


    private static void concurrentIndexsearchRequests() throws Exception {
        LOG.info(">> concurrentIndexsearchRequests");

        int requestCount = 284;

        ExecutorService executorService = Executors.newFixedThreadPool(50, new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("atlas-search-logger-" + Thread.currentThread().getName())
                .build());

        CountDownLatch latch = new CountDownLatch(requestCount);

        for (int i=0; i < requestCount; i++) {
            IndexSearchParams indexSearchParams = new IndexSearchParams();
            Map<String, Object> dsl = mapOf("query", mapOf("match", mapOf("__typeName", "Column")));
            dsl.put("size", 10);

            indexSearchParams.setDsl(dsl);

            executorService.submit(new IndexSearchConsumer(indexSearchParams, latch));
        }

        latch.await();
        LOG.info("Done with executor service");

        LOG.info(">> concurrentIndexsearchRequests");
    }

    static class IndexSearchConsumer implements Runnable {
        private IndexSearchParams params;
        private CountDownLatch latch;

        public IndexSearchConsumer(IndexSearchParams params, CountDownLatch latch) {
            this.params = params;
            this.latch = latch;
        }

        @Override
        public void run() {
            try {
                Thread.sleep(10000);
                LOG.info("Running consumer {}", Thread.currentThread().getId());
                indexSearch(params);
                latch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static void verifyESHasLineage(String entityGuid, boolean expectedNull) {

        SearchHit[] searchHit = ESUtil.searchWithGuid(entityGuid).getHits().getHits();
        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            assertNotNull(sourceAsMap.get("hasLineage"));
            boolean value = (boolean) sourceAsMap.get("hasLineage");

            if (expectedNull) {
                assertFalse(value);
            } else {
                assertTrue(value);
            }
        }
    }
}