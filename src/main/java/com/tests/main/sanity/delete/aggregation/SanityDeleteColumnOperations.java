package com.tests.main.sanity.delete.aggregation;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.tests.main.utils.TestUtil.*;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.junit.Assert.*;

public class SanityDeleteColumnOperations implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(SanityDeleteColumnOperations.class);

    private static long SLEEP = 2000;

    private static final String COLUMNS = "columns";
    private static final String TABLE = "table";
    private static final String DELETE_HANDLER_DEFAULT = "DEFAULT";
    private static final String DELETE_HANDLER_SOFT = "SOFT";
    private static final String DELETE_HANDLER_HARD = "HARD";
    private static final String DELETE_HANDLER_PURGE = "PURGE";

    public static void main(String[] args) throws Exception {
        try {
            new SanityDeleteColumnOperations().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running column delete operations tests");

        testDefaultDelete();
        testSoftDelete();
        testHardDelete();
        testPurgeDelete();
    }

    private void testDefaultDelete() throws Exception {
        LOG.info(">> testDefaultDelete");
        
        // Create a test table with column
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_default_delete" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Create column and link it to the table
        List<String> columnGuids = createColumnsForTable(tableGuid, 1);
        String columnGuid = columnGuids.get(0);
        sleep(SLEEP);

        // Verify initial state
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        List<Map<String, Object>> tableColumns = (List<Map<String, Object>>) table.getRelationshipAttribute(COLUMNS);
        assertNotNull(tableColumns);
        assertEquals(1, tableColumns.size());

        // Perform default delete on column
        EntityMutationResponse response = deleteEntityDefault(columnGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_DEFAULT, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify column is deleted but still retrievable
        AtlasEntity column = getEntity(columnGuid);
        assertEquals(DELETED, column.getStatus());
        verifyESAttributes(columnGuid, mapOf(ATTR_STATE, DELETED.name()));

        // Verify table is still active and has one column relationship as DELETED
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        tableColumns = (List<Map<String, Object>>) table.getRelationshipAttribute(COLUMNS);
        assertNotNull(tableColumns);
        assertEquals(1, tableColumns.size());
        assertEquals("DELETED", ((Map<String, Object>)tableColumns.get(0)).get("relationshipStatus"));
        verifyESAttributes(tableGuid, mapOf(ATTR_STATE, ACTIVE.name()));

        LOG.info("<< testDefaultDelete");
    }

    private void testSoftDelete() throws Exception {
        LOG.info(">> testSoftDelete");
        
        // Create a test table with column
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_soft_delete" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Create column and link it to the table
        List<String> columnGuids = createColumnsForTable(tableGuid, 1);
        String columnGuid = columnGuids.get(0);
        sleep(SLEEP);

        // Verify initial state
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        List<Map<String, Object>> tableColumns = (List<Map<String, Object>>) table.getRelationshipAttribute(COLUMNS);
        assertNotNull(tableColumns);
        assertEquals(1, tableColumns.size());

        // Perform soft delete on column
        EntityMutationResponse response = deleteEntitySoft(columnGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_SOFT, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify column is deleted but still retrievable
        AtlasEntity column = getEntity(columnGuid);
        assertEquals(DELETED, column.getStatus());
        verifyESAttributes(columnGuid, mapOf(ATTR_STATE, DELETED.name()));

        // Verify table is still active and has one column relationship as DELETED
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        tableColumns = (List<Map<String, Object>>) table.getRelationshipAttribute(COLUMNS);
        assertNotNull(tableColumns);
        assertEquals(1, tableColumns.size());
        assertEquals("DELETED", ((Map<String, Object>)tableColumns.get(0)).get("relationshipStatus"));

        LOG.info("<< testSoftDelete");
    }

    private void testHardDelete() throws Exception {
        LOG.info(">> testHardDelete");
        
        // Create a test table with column
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_hard_delete" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Create column and link it to the table
        List<String> columnGuids = createColumnsForTable(tableGuid, 1);
        String columnGuid = columnGuids.get(0);
        sleep(SLEEP);

        // Verify initial state
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        List<Map<String, Object>> tableColumns = (List<Map<String, Object>>) table.getRelationshipAttribute(COLUMNS);
        assertNotNull(tableColumns);
        assertEquals(1, tableColumns.size());

        // Perform hard delete on column
        EntityMutationResponse response = deleteEntityHard(columnGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_HARD, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify column is not retrievable
        try {
            verifyESDocumentNotPresent(columnGuid);
            getEntity(columnGuid);
            fail("Column should not be retrievable after hard delete");
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage.contains("|")) {
                errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
            }
            assertEquals("404", errorMessage);
        }

        // Verify table is still active and has no column relationship
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        verifyESAttributes(tableGuid, mapOf(ATTR_STATE, ACTIVE.name()));
        tableColumns = (List<Map<String, Object>>) table.getRelationshipAttribute(COLUMNS);
        assertNotNull(tableColumns);
        assertEquals(0, tableColumns.size());

        LOG.info("<< testHardDelete");
    }

    private void testPurgeDelete() throws Exception {
        LOG.info(">> testPurgeDelete");
        
        // Create a test table with column
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_purge_delete" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Create column and link it to the table
        List<String> columnGuids = createColumnsForTable(tableGuid, 1);
        String columnGuid = columnGuids.get(0);
        sleep(SLEEP);

        // Verify initial state
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        List<Map<String, Object>> tableColumns = (List<Map<String, Object>>) table.getRelationshipAttribute(COLUMNS);
        assertNotNull(tableColumns);
        assertEquals(1, tableColumns.size());

        // Perform purge delete on column
        EntityMutationResponse response = deleteEntityPurge(columnGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_PURGE, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify column is not retrievable
        try {
            verifyESDocumentNotPresent(columnGuid);
            getEntity(columnGuid);
            fail("Column should not be retrievable after purge delete");
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage.contains("|")) {
                errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
            }
            assertEquals("404", errorMessage);
        }

        // Verify table is still active and has no column relationship
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        verifyESAttributes(tableGuid, mapOf(ATTR_STATE, ACTIVE.name()));
        tableColumns = (List<Map<String, Object>>) table.getRelationshipAttribute(COLUMNS);
        assertNotNull(tableColumns);
        assertEquals(0, tableColumns.size());

        LOG.info("<< testPurgeDelete");
    }

    private List<String> createColumnsForTable(String tableGuid, int numColumns) throws Exception {
        List<String> columnGuids = new ArrayList<>();
        for (int i = 0; i < numColumns; i++) {
            AtlasEntity column = getAtlasEntity(TYPE_COLUMN, "test_column_" + i + "_" + getRandomName());
            column.setRelationshipAttribute(TABLE, getObjectId(tableGuid, TYPE_TABLE));
            EntityMutationResponse response = createEntity(column);
            columnGuids.add(response.getCreatedEntities().get(0).getGuid());
        }
        return columnGuids;
    }
} 