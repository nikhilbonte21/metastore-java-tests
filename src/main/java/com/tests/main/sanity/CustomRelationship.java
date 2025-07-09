package com.tests.main.sanity;

import com.tests.main.IndexSearchParams;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.tests.main.utils.TestUtil.TYPE_DATABASE;
import static com.tests.main.utils.TestUtil.TYPE_TERM;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getObjectId;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.indexSearch;
import static com.tests.main.utils.TestUtil.mapOf;
import static com.tests.main.utils.TestUtil.setOf;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;

public class CustomRelationship implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(CustomRelationship.class);

    private static long SLEEP = 2000;

    public static void main(String[] args) throws Exception {
        try {
            new CustomRelationship().run();
            //TestRunner.runTests(CustomRelationship.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running CustomRelationship tests");

        long start = System.currentTimeMillis();
        try {
            createCustomRelationshipWithOneWay();


        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running CustomRelationship tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void createCustomRelationshipWithOneWay() throws Exception {
        LOG.info(">> createCustomRelationshipWithOneWay");

        AtlasEntity db0 = getAtlasEntity(TYPE_DATABASE, "test_database" + getRandomName());
        String db0_guid = createEntitiesBulk(db0).getCreatedEntities().get(0).getGuid();

        AtlasEntity db1 = getAtlasEntity(TYPE_DATABASE, "test_database" + getRandomName());
        String db1_guid = createEntitiesBulk(db1).getCreatedEntities().get(0).getGuid();

        sleep(SLEEP);

        Map<String, Object> attrMap = mapOf("fromTypeLabel", "is copied from", "toTypeLabel", "is copied to");
        Map<String, Object> finalMap = mapOf("typeName", "UserDefRelationship");
        finalMap.putAll(attrMap);


        db0 = getEntity(db0_guid);
        AtlasObjectId objectId = getObjectId(db1_guid, TYPE_TERM);
        objectId.setAttributes(finalMap);
        db0.setRelationshipAttribute("userDefRelationshipTo", Collections.singletonList(objectId));
        updateEntity(db0);

        sleep(SLEEP);

        // Verify via GET by GUID

        /*db0 = getEntity(db0_guid);
        final Map<String, Object> actualAttributes = (Map<String, Object>) ((Map)((List<Map>) db0.getRelationshipAttribute("userDefRelationshipTo")).get(0).get("relationshipAttributes")).get("attributes");
        attrMap.keySet().forEach(key -> Objects.equals(attrMap.get(key), actualAttributes.get(key)));*/


        // Verify via indexsearch
        IndexSearchParams params = new IndexSearchParams();
        Map<String, Object> dsl = mapOf("query", mapOf("bool", mapOf("must", mapOf("term", mapOf("__guid", db0_guid)))));
        dsl.put("size", 1);
        params.setDsl(dsl);

        params.setAttributes(setOf("userDefRelationshipTo"));

        AtlasSearchResult result = indexSearch(params);

        AtlasEntityHeader entityHeader = result.getEntities().get(0);

        final Map<String, Object> actualAttributesIndexsearch  = (Map<String, Object>) entityHeader.getAttribute("userDefRelationshipTo");
        attrMap.keySet().forEach(key -> Objects.equals(attrMap.get(key), actualAttributesIndexsearch.get(key)));



        LOG.info(">> createCustomRelationshipWithOneWay");
    }
}
