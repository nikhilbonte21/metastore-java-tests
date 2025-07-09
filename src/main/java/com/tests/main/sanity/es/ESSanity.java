package com.tests.main.sanity.es;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.tests.main.utils.TestUtil.TYPE_CONNECTION;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getESDoc;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;
import static org.junit.Assert.*;

public class ESSanity implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(ESSanity.class);

    private static long SLEEP = 1000;

    public static void main(String[] args) throws Exception {
        try {
            new ESSanity().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running ESSanity tests");

        long start = System.currentTimeMillis();
        try {
            createAndUpdateConnection();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running ESSanity tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void createAndUpdateConnection() throws Exception {
        LOG.info(">> createAndUpdateConnection");

        AtlasEntity connection = getAtlasEntity(TYPE_CONNECTION, "connection_0");
        connection.setAttribute("adminUsers", Arrays.asList("nikhil.bonte"));
        String connectionGuid = createEntity(connection).getGuidAssignments().values().iterator().next();
        sleep(SLEEP);

        Map<String, Object> sourceAsMap = getESDoc(connectionGuid);
        assertTrue(sourceAsMap.size() > 10);
        ensureTypesForConnection(sourceAsMap);
        assertTrue(sourceAsMap.get("assetSodaLastScanAt") instanceof Integer);


        connection = getEntity(connectionGuid);
        connection.setAttribute("assetSodaLastScanAt", 1744828718244L);
        updateEntity(connection);
        sleep(SLEEP);

        sourceAsMap = getESDoc(connectionGuid);
        assertTrue(sourceAsMap.size() > 10);
        ensureTypesForConnection(sourceAsMap);
        assertTrue(sourceAsMap.get("assetSodaLastScanAt") instanceof Long);


        LOG.info(">> createAndUpdateConnection");
    }

    private static void ensureTypesForConnection(Map<String, Object> sourceAsMap) {
        assertTrue(sourceAsMap.get("viewScore") instanceof Double);
        assertTrue(sourceAsMap.get("popularityScore") instanceof Double);
        assertTrue(sourceAsMap.get("sourceTotalCost") instanceof Double);
        assertTrue(sourceAsMap.get("sourceReadQueryCost") instanceof Double);

        assertTrue(sourceAsMap.get("port") instanceof Integer);
        assertTrue(sourceAsMap.get("queryTimeout") instanceof Integer);
        assertTrue(sourceAsMap.get("lastSyncRunAt") instanceof Integer);
        assertTrue(sourceAsMap.get("assetDbtJobLastRunDequedAt") instanceof Integer);

        assertTrue(sourceAsMap.get("__hasLineage") instanceof Boolean);
        assertTrue(sourceAsMap.get("hasPopularityInsights") instanceof Boolean);

        assertTrue(sourceAsMap.get("adminUsers") instanceof ArrayList);
        assertTrue(((List) sourceAsMap.get("adminUsers")).get(0) instanceof String);
    }
}
