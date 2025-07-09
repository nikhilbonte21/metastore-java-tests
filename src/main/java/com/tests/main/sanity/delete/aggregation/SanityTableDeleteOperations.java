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

/**
 * Tests different delete operations on tables and their impact on associated columns.
 * 
 * This class verifies the behavior of four table delete operations:
 * 1. Default Delete: Marks table as DELETED but keeps it retrievable. Columns remain ACTIVE with ACTIVE relationships.
 * 2. Soft Delete: Marks table as DELETED but keeps it retrievable. Columns remain ACTIVE with ACTIVE relationships.
 * 3. Hard Delete: Completely removes table (404 on retrieval). Columns remain ACTIVE but lose table relationship.
 * 4. Purge Delete: Completely removes table (404 on retrieval). Columns remain ACTIVE but lose table relationship.
 * 
 * Each test:
 * - Creates a table with 2 columns
 * - Performs the specified delete operation on the table
 * - Verifies the table's status and retrievability
 * - Verifies the columns' status and relationship with the table
 * 
 * Note: The difference between default and soft delete is in the delete handler type,
 * but their behavior is identical. Similarly, hard and purge delete differ only in
 * handler type but have the same effect.
 */
public class SanityTableDeleteOperations implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(SanityTableDeleteOperations.class);

    private static long SLEEP = 1000;

    private static final String COLUMNS = "columns";
    private static final String TABLE = "table";
    private static final String DELETE_HANDLER_DEFAULT = "DEFAULT";
    private static final String DELETE_HANDLER_SOFT = "SOFT";
    private static final String DELETE_HANDLER_HARD = "HARD";
    private static final String DELETE_HANDLER_PURGE = "PURGE";

    public static void main(String[] args) throws Exception {
        try {
            new SanityTableDeleteOperations().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running table delete operations tests");

        testDefaultDelete();
        testSoftDelete();
        testHardDelete();
        testPurgeDelete();
    }

    private void testDefaultDelete() throws Exception {
        LOG.info(">> testDefaultDelete");
        
        // Create a test table
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_default_delete" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Create columns and link them to the table
        List<String> columnGuids = createColumnsForTable(tableGuid, 2);
        sleep(SLEEP);

        // Verify initial state
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        List<Map<String, Object>> tableColumns = (List<Map<String, Object>>) table.getRelationshipAttribute(COLUMNS);
        assertNotNull(tableColumns);
        assertEquals(2, tableColumns.size());

        // Perform default delete
        EntityMutationResponse response = deleteEntityDefault(tableGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_DEFAULT, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify table is deleted but still retrievable
        table = getEntity(tableGuid);
        assertEquals(DELETED, table.getStatus());
        verifyESAttributes(tableGuid, mapOf(ATTR_STATE, DELETED.name()));

        // Verify columns are still active but relationships are marked as deleted
        for (String columnGuid : columnGuids) {
            AtlasEntity column = getEntity(columnGuid);
            assertEquals(ACTIVE, column.getStatus());
            Map<String, Object> parentRelation = (Map<String, Object>) column.getRelationshipAttribute(TABLE);
            assertNotNull(parentRelation);
            assertEquals("ACTIVE", parentRelation.get("relationshipStatus"));
            verifyESAttributes(columnGuid, mapOf(ATTR_STATE, ACTIVE.name()));
        }

        LOG.info("<< testDefaultDelete");
    }

    private void testSoftDelete() throws Exception {
        LOG.info(">> testSoftDelete");
        
        // Create a test table
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_soft_delete" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Create columns and link them to the table
        List<String> columnGuids = createColumnsForTable(tableGuid, 2);
        sleep(SLEEP);

        // Verify initial state
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        List<Map<String, Object>> tableColumns = (List<Map<String, Object>>) table.getRelationshipAttribute(COLUMNS);
        assertNotNull(tableColumns);
        assertEquals(2, tableColumns.size());

        // Perform soft delete
        EntityMutationResponse response = deleteEntitySoft(tableGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_SOFT, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify table is deleted but still retrievable
        table = getEntity(tableGuid);
        assertEquals(DELETED, table.getStatus());
        verifyESAttributes(tableGuid, mapOf(ATTR_STATE, DELETED.name()));

        // Verify columns are still active but relationships are marked as deleted
        for (String columnGuid : columnGuids) {
            AtlasEntity column = getEntity(columnGuid);
            assertEquals(ACTIVE, column.getStatus());
            verifyESAttributes(columnGuid, mapOf(ATTR_STATE, ACTIVE.name()));
            Map<String, Object> parentRelation = (Map<String, Object>) column.getRelationshipAttribute(TABLE);
            assertNotNull(parentRelation);
            assertEquals(ACTIVE.name(), parentRelation.get("relationshipStatus"));
            verifyESAttributes(columnGuid, mapOf(ATTR_STATE, ACTIVE.name()));
        }

        LOG.info("<< testSoftDelete");
    }

    private void testHardDelete() throws Exception {
        LOG.info(">> testHardDelete");
        
        // Create a test table
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_hard_delete" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Create columns and link them to the table
        List<String> columnGuids = createColumnsForTable(tableGuid, 2);
        sleep(SLEEP);

        // Verify initial state
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        List<Map<String, Object>> tableColumns = (List<Map<String, Object>>) table.getRelationshipAttribute(COLUMNS);
        assertNotNull(tableColumns);
        assertEquals(2, tableColumns.size());

        // Perform hard delete
        EntityMutationResponse response = deleteEntityHard(tableGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_HARD, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify table is not retrievable
        try {
            verifyESDocumentNotPresent(tableGuid);
            getEntity(tableGuid);
            fail("Table should not be retrievable after hard delete");
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage.contains("|")) {
                errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
            }
            assertEquals("404", errorMessage);
        }

        // Verify columns are still active but don't have table relationship
        for (String columnGuid : columnGuids) {
            AtlasEntity column = getEntity(columnGuid);
            assertEquals(ACTIVE, column.getStatus());
            verifyESAttributes(columnGuid, mapOf(ATTR_STATE, ACTIVE.name()));
            Map<String, Object> parentRelation = (Map<String, Object>) column.getRelationshipAttribute(TABLE);
            assertNull("Column should not have table relationship after hard delete", parentRelation);
        }

        LOG.info("<< testHardDelete");
    }

    private void testPurgeDelete() throws Exception {
        LOG.info(">> testPurgeDelete");
        
        // Create a test table
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_purge_delete" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Create columns and link them to the table
        List<String> columnGuids = createColumnsForTable(tableGuid, 2);
        sleep(SLEEP);

        // Verify initial state
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        List<Map<String, Object>> tableColumns = (List<Map<String, Object>>) table.getRelationshipAttribute(COLUMNS);
        assertNotNull(tableColumns);
        assertEquals(2, tableColumns.size());

        // Perform purge delete
        EntityMutationResponse response = deleteEntityPurge(tableGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_PURGE, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify table is not retrievable
        try {
            verifyESDocumentNotPresent(tableGuid);
            getEntity(tableGuid);
            fail("Table should not be retrievable after purge delete");
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage.contains("|")) {
                errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
            }
            assertEquals("404", errorMessage);
        }

        // Verify columns are still active but don't have table relationship
        for (String columnGuid : columnGuids) {
            AtlasEntity column = getEntity(columnGuid);
            assertEquals(ACTIVE, column.getStatus());
            verifyESAttributes(columnGuid, mapOf(ATTR_STATE, ACTIVE.name()));
            Map<String, Object> parentRelation = (Map<String, Object>) column.getRelationshipAttribute(TABLE);
            assertNull("Column should not have table relationship after purge delete", parentRelation);
        }

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