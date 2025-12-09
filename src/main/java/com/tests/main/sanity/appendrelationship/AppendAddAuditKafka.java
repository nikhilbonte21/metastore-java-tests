package com.tests.main.sanity.appendrelationship;

import com.tests.main.KafkaMessage;
import com.tests.main.Test;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.REL_ANCHOR;
import static com.tests.main.utils.TestUtil.TYPE_COLUMN;
import static com.tests.main.utils.TestUtil.TYPE_GLOSSARY;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.TYPE_TERM;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getEntityAudit;
import static com.tests.main.utils.TestUtil.getName;
import static com.tests.main.utils.TestUtil.getObjectId;
import static com.tests.main.utils.TestUtil.getObjectIdsAsList;
import static com.tests.main.utils.TestUtil.getQualifiedName;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntitiesBulk;
import static com.tests.main.utils.TestUtil.updateEntity;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.ENTITY_UPDATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class AppendAddAuditKafka implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(AppendAddAuditKafka.class);

    private static final long SLEEP = 2000;

    public static void main(String[] args) throws Exception {
        try {
            new AppendAddAuditKafka().run();
            //TestRunner.runTests(AppendAddAuditKafka.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running AppendAddAuditKafka tests");

        long start = System.currentTimeMillis();
        try {

            appendAddColumnToTable();

            appendAddTableToColumn();

            appendAddAssetsToTerm();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running AppendAddAuditKafka tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    @Test
    private static void appendAddColumnToTable() throws Exception {
        LOG.info(">> appendAddColumnToTable");

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "table_0");
        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "column_0");
        AtlasEntity column1 = getAtlasEntity(TYPE_COLUMN, "column_1");
        AtlasEntity column2 = getAtlasEntity(TYPE_COLUMN, "column_2");
        AtlasEntity column3 = getAtlasEntity(TYPE_COLUMN, "column_3");
        AtlasEntity column4 = getAtlasEntity(TYPE_COLUMN, "column_4");
        EntityMutationResponse response = createEntitiesBulk(table, column0, column1, column2, column3, column4);

        Map<String, String> guidsMap = response.getGuidAssignments();
        String tableGuid = guidsMap.get(table.getGuid());
        String column0Guid = guidsMap.get(column0.getGuid());

        String column1Guid = guidsMap.get(column1.getGuid());

        String column2Guid = guidsMap.get(column2.getGuid());
        String column3Guid = guidsMap.get(column3.getGuid());

        String column4Guid = guidsMap.get(column4.getGuid());

        sleep(SLEEP);

        // update table to append column0
        long timeInMillis = System.currentTimeMillis();
        table = getEntity(tableGuid);

        table.removeAttribute("columns");
        table.getRelationshipAttributes().clear();
        table.setAppendRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, column0Guid));
        updateEntity(table);
        sleep(SLEEP);

        assertTableUpdate(timeInMillis, tableGuid, column0Guid);
        assertColumnUpdate(timeInMillis, tableGuid, column0Guid);


        // update table to append column1
        timeInMillis = System.currentTimeMillis();
        table = getEntity(tableGuid);

        table.removeAttribute("columns");
        table.getRelationshipAttributes().clear();
        table.setAppendRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, column1Guid));
        updateEntity(table);
        sleep(SLEEP);

        assertTableUpdate(timeInMillis, tableGuid, column1Guid);
        assertColumnUpdate(timeInMillis, tableGuid, column1Guid);


        // update table to append column2, column3
        timeInMillis = System.currentTimeMillis();
        table = getEntity(tableGuid);

        table.removeAttribute("columns");
        table.getRelationshipAttributes().clear();
        table.setAppendRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, column2Guid, column3Guid));
        updateEntity(table);
        sleep(SLEEP);

        assertTableUpdate(timeInMillis, tableGuid, column2Guid, column3Guid);
        assertColumnUpdate(timeInMillis, tableGuid, column2Guid, column3Guid);

        // update table to append existing column3 and new column4
        timeInMillis = System.currentTimeMillis();
        AtlasEntity tableCopy = new AtlasEntity();
        tableCopy.setTypeName(TYPE_TABLE);
        tableCopy.setGuid(tableGuid);
        tableCopy.setAttribute(NAME, getName(table));
        tableCopy.setAttribute(QUALIFIED_NAME, getQualifiedName(table));

        tableCopy.removeAttribute("columns");
        tableCopy.setAppendRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, column3Guid, column4Guid));
        updateEntity(tableCopy);
        sleep(SLEEP);

        assertTableUpdate(timeInMillis, tableGuid, column4Guid);
        assertColumnUpdate(timeInMillis, tableGuid, column4Guid);


        LOG.info(">> appendAddColumnToTable");
    }

    @Test
    private static void appendAddTableToColumn() throws Exception {
        LOG.info(">> appendAddTableToColumn");

        //List<KafkaMessage> messages = null;
        Map<String, KafkaMessage> messages = null;

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "table_0");
        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "column_0");
        AtlasEntity column1 = getAtlasEntity(TYPE_COLUMN, "column_1");
        AtlasEntity column2 = getAtlasEntity(TYPE_COLUMN, "column_2");
        AtlasEntity column3 = getAtlasEntity(TYPE_COLUMN, "column_3");
        AtlasEntity column4 = getAtlasEntity(TYPE_COLUMN, "column_4");
        EntityMutationResponse response = createEntitiesBulk(table, column0, column1, column2, column3, column4);

        Map<String, String> guidsMap = response.getGuidAssignments();
        String tableGuid = guidsMap.get(table.getGuid());
        String column0Guid = guidsMap.get(column0.getGuid());

        String column1Guid = guidsMap.get(column1.getGuid());

        String column2Guid = guidsMap.get(column2.getGuid());
        String column3Guid = guidsMap.get(column3.getGuid());

        String column4Guid = guidsMap.get(column4.getGuid());

        sleep(SLEEP);

        // update column0 to append table
        long timeInMillis = System.currentTimeMillis();
        column0 = getEntity(column0Guid);

        column0.removeAttribute("table");
        column0.getRelationshipAttributes().clear();
        column0.setAppendRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        updateEntity(column0);
        sleep(SLEEP);

        assertTableUpdate(timeInMillis, tableGuid, column0Guid);
        assertColumnUpdate(timeInMillis, tableGuid, column0Guid);


        // update column1, column2 to append table in same request
        timeInMillis = System.currentTimeMillis();
        column1 = getEntity(column1Guid);
        column2 = getEntity(column2Guid);

        column1.removeAttribute("table");
        column1.getRelationshipAttributes().clear();
        column1.setAppendRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));


        column2.removeAttribute("table");
        column2.getRelationshipAttributes().clear();
        column2.setAppendRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));

        updateEntitiesBulk(column1, column2);
        sleep(SLEEP);

        assertTableUpdate(timeInMillis, tableGuid, column1Guid, column2Guid);
        assertColumnUpdate(timeInMillis, tableGuid, column1Guid);
        assertColumnUpdate(timeInMillis, tableGuid, column2Guid);


        // update existing column2 and new column3 to append table in same request
        timeInMillis = System.currentTimeMillis();
        column2 = getEntity(column2Guid);
        column2.removeAttribute("table");
        column2.getRelationshipAttributes().clear();
        column2.setAppendRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));


        AtlasEntity column3Copy = new AtlasEntity();
        column3Copy.setTypeName(TYPE_COLUMN);
        column3Copy.setGuid(column3Guid);
        column3Copy.setAttribute(NAME, getName(column3));
        column3Copy.setAttribute(QUALIFIED_NAME, getQualifiedName(column3));
        column3Copy.removeAttribute("table");
        column3Copy.setAppendRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));

        updateEntitiesBulk(column2, column3Copy);
        sleep(SLEEP);

        assertTableUpdate(timeInMillis, tableGuid, column3Guid);
        assertColumnUpdate(timeInMillis, tableGuid, column3Guid);

        List<EntityAuditEventV2> events = getEntityAudit(column2Guid, timeInMillis);
        assertEquals( 1, events.size());
        assertEquals(ENTITY_UPDATE, events.get(0).getAction());

        Map<String, Object> addedRelationshipAttribute = (Map<String, Object>) events.get(0).getDetail().get("addedRelationshipAttributes");
        assertNull(addedRelationshipAttribute);


        LOG.info(">> appendAddTableToColumn");
    }


    @Test
    private static void appendAddAssetsToTerm() throws Exception {
        LOG.info(">> appendAddAssetsToTerm");

        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "glossary_0");
        String glossaryGuid = createEntity(glossary).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        AtlasEntity term = getAtlasEntity(TYPE_TERM, "termMaster_0");
        term.setRelationshipAttribute(REL_ANCHOR, getObjectId(glossaryGuid, TYPE_GLOSSARY));

        AtlasEntity asset0 = getAtlasEntity(TYPE_TABLE, "asset_0");
        AtlasEntity asset1 = getAtlasEntity(TYPE_TABLE, "asset_1");
        AtlasEntity asset2 = getAtlasEntity(TYPE_TABLE, "asset_2");
        AtlasEntity asset3 = getAtlasEntity(TYPE_TABLE, "asset_3");
        AtlasEntity asset4 = getAtlasEntity(TYPE_TABLE, "asset_4");


        EntityMutationResponse response = createEntitiesBulk(term, asset0, asset1, asset2, asset3, asset4);

        Map<String, String> guidsMap = response.getGuidAssignments();
        String termGuid = guidsMap.get(term.getGuid());
        String asset0guid = guidsMap.get(asset0.getGuid());
        String asset1guid = guidsMap.get(asset1.getGuid());
        String asset2guid = guidsMap.get(asset2.getGuid());
        String asset3guid = guidsMap.get(asset3.getGuid());
        String asset4guid = guidsMap.get(asset4.getGuid());
        sleep(SLEEP);

        // update term to append asset0
        long timeInMillis = System.currentTimeMillis();
        term = getEntity(termGuid);

        term.removeAttribute("assignedEntities");
        term.removeAttribute("lexicographicalSortOrder");
        term.getRelationshipAttributes().clear();
        term.setRelationshipAttribute(REL_ANCHOR, getObjectId(glossaryGuid, TYPE_GLOSSARY));
        term.setAppendRelationshipAttribute("assignedEntities", getObjectIdsAsList(TYPE_TERM, asset0guid));
        updateEntity(term);
        sleep(SLEEP);

        assertTermUpdate(timeInMillis, termGuid, asset0guid);
        assertAssetsUpdate(timeInMillis, termGuid, asset0guid);


        // update term to append asset1
        timeInMillis = System.currentTimeMillis();
        term = getEntity(termGuid);

        term.removeAttribute("assignedEntities");
        term.removeAttribute("lexicographicalSortOrder");
        term.getRelationshipAttributes().clear();
        term.setRelationshipAttribute(REL_ANCHOR, getObjectId(glossaryGuid, TYPE_GLOSSARY));
        term.setAppendRelationshipAttribute("assignedEntities", getObjectIdsAsList(TYPE_TERM, asset1guid));
        updateEntity(term);
        sleep(SLEEP);

        assertTermUpdate(timeInMillis, termGuid, asset1guid);
        assertAssetsUpdate(timeInMillis, termGuid, asset1guid);


        // update term to append asset2, asset3
        timeInMillis = System.currentTimeMillis();
        term = getEntity(termGuid);

        term.removeAttribute("assignedEntities");
        term.removeAttribute("lexicographicalSortOrder");
        term.getRelationshipAttributes().clear();
        term.setRelationshipAttribute(REL_ANCHOR, getObjectId(glossaryGuid, TYPE_GLOSSARY));
        term.setAppendRelationshipAttribute("assignedEntities", getObjectIdsAsList(TYPE_TERM, asset2guid, asset3guid));
        updateEntity(term);
        sleep(SLEEP);

        assertTermUpdate(timeInMillis, termGuid, asset2guid, asset3guid);
        assertAssetsUpdate(timeInMillis, termGuid, asset2guid, asset3guid);

        // update term to append existing asset3 and new asset4
        timeInMillis = System.currentTimeMillis();
        AtlasEntity termCopy = new AtlasEntity();
        termCopy.setTypeName(TYPE_TERM);
        termCopy.setGuid(termGuid);
        termCopy.setAttribute(NAME, getName(term));
        termCopy.setAttribute(QUALIFIED_NAME, getQualifiedName(term));
        termCopy.removeAttribute("assignedEntities");
        termCopy.removeAttribute("lexicographicalSortOrder");
        termCopy.setRelationshipAttribute(REL_ANCHOR, getObjectId(glossaryGuid, TYPE_GLOSSARY));
        termCopy.setAppendRelationshipAttribute("assignedEntities", getObjectIdsAsList(TYPE_TERM, asset3guid, asset4guid));
        updateEntity(termCopy);
        sleep(SLEEP);

        assertTermUpdate(timeInMillis, termGuid, asset4guid);
        assertAssetsUpdate(timeInMillis, termGuid, asset4guid);

        LOG.info(">> appendAddAssetsToTerm");
    }

    private static void assertTableUpdate(long timeInMillis, String tableGuid, String... expectColumnGuids) throws Exception {
        List<EntityAuditEventV2> events = getEntityAudit(tableGuid, timeInMillis);
        assertEquals(1, events.size());
        assertEquals(ENTITY_UPDATE, events.get(0).getAction());

        Map<String, Object> addedRelationshipAttributes = (Map<String, Object>) events.get(0).getDetail().get("addedRelationshipAttributes");
        assertNotNull(addedRelationshipAttributes);

        List<Map<String, Object>> addedColumns = (List<Map<String, Object>>) addedRelationshipAttributes.get("columns");
        assertNotNull(addedColumns);
        assertEquals(expectColumnGuids.length, addedColumns.size());

        List<String> guidList = Arrays.asList(expectColumnGuids);
        for (Map<String, Object> addedColumn : addedColumns) {
            assertEquals("Column", addedColumn.get("typeName").toString());
            assertTrue(guidList.contains(addedColumn.get("guid").toString()));
        }
    }

    private static void assertColumnUpdate(long timeInMillis, String expectTableGuid, String... validateColumnGuids) throws Exception {
        int index = 0;
        for (String columnGuid : validateColumnGuids) {

            List<EntityAuditEventV2> events = getEntityAudit(columnGuid, timeInMillis);
            assertEquals(String.format("Audit was not found after %s successful iterations", index), 1, events.size());
            assertEquals(ENTITY_UPDATE, events.get(0).getAction());

            Map<String, Object> addedRelationshipAttribute = (Map<String, Object>) events.get(0).getDetail().get("addedRelationshipAttributes");
            assertNotNull(addedRelationshipAttribute);

            Map<String, Object> addedTable = (Map<String, Object>) addedRelationshipAttribute.get("table");
            assertNotNull(addedTable);
            assertEquals("Table", addedTable.get("typeName").toString());
            assertEquals(expectTableGuid, addedTable.get("guid").toString());

            index++;
        }
    }

    private static void assertTermUpdate(long timeInMillis, String termGuid, String... expectTermGuids) throws Exception {
        List<EntityAuditEventV2> events = getEntityAudit(termGuid, timeInMillis);
        assertEquals(1, events.size());
        assertEquals(ENTITY_UPDATE, events.get(0).getAction());

        Map<String, Object> addedRelationshipAttributes = (Map<String, Object>) events.get(0).getDetail().get("addedRelationshipAttributes");
        assertNotNull(addedRelationshipAttributes);

        List<Map<String, Object>> addedAssets = (List<Map<String, Object>>) addedRelationshipAttributes.get("assignedEntities");
        assertNotNull(addedAssets);
        assertEquals(expectTermGuids.length, addedAssets.size());

        List<String> guidList = Arrays.asList(expectTermGuids);
        for (Map<String, Object> addedAsset : addedAssets) {
            assertEquals(TYPE_TABLE, addedAsset.get("typeName").toString());
            assertTrue(guidList.contains(addedAsset.get("guid").toString()));
        }
    }

    private static void assertAssetsUpdate(long timeInMillis, String termGuid, String... validateAssetGuids) throws Exception {
        int index = 0;
        for (String assetGuid : validateAssetGuids) {

            List<EntityAuditEventV2> events = getEntityAudit(assetGuid, timeInMillis);
            assertEquals(String.format("Audit was not found after %s successful iterations", index), 1, events.size());
            assertEquals(ENTITY_UPDATE, events.get(0).getAction());

            Map<String, Object> addedRelationshipAttribute = (Map<String, Object>) events.get(0).getDetail().get("addedRelationshipAttributes");
            assertNotNull(addedRelationshipAttribute);

            List<Map<String, Object>> addedTerms = (List<Map<String, Object>>) addedRelationshipAttribute.get("meanings");
            assertNotNull(addedTerms);
            assertEquals(1, addedTerms.size());
            assertEquals(TYPE_TERM, addedTerms.get(0).get("typeName").toString());
            assertEquals(termGuid, addedTerms.get(0).get("guid").toString());

            index++;
        }
    }
}
