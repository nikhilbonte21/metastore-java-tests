package com.tests.main.sanity.restore.aggregation;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.tests.main.utils.Constants.COLUMNS;
import static com.tests.main.utils.Constants.DELETE_HANDLER_DEFAULT;
import static com.tests.main.utils.Constants.DELETE_HANDLER_SOFT;
import static com.tests.main.utils.Constants.TABLE;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createColumnsForTable;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.deleteEntityDefault;
import static com.tests.main.utils.TestUtil.deleteEntitySoft;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.mapOf;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;
import static com.tests.main.utils.TestUtil.verifyESAttributes;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RestoreAssetAggregation implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreAssetAggregation.class);

    private static long SLEEP = 1000;

    public static void main(String[] args) throws Exception {
        try {
            new RestoreAssetAggregation().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running column delete RestoreAssetNoRelation tests");

        testRestoreSoftDeletedTable();
        testRestoreSoftDeletedColumn(); //Inverse
    }

    private void testRestoreSoftDeletedTable() throws Exception {
        LOG.info(">> testRestoreSoftDeletedTable");

        // Create a test table
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_default_delete" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Create columns and link them to the table
        List<String> columnGuids = createColumnsForTable(tableGuid, 2);
        sleep(SLEEP);

        // Perform soft delete
        EntityMutationResponse deleteResponse = deleteEntitySoft(tableGuid);
        assertNotNull(deleteResponse);
        assertTrue(deleteResponse.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_SOFT, deleteResponse.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify table is soft deleted
        table = getEntity(tableGuid);
        assertEquals(DELETED, table.getStatus());

        // Verify columns & relationships are still active
        for (String columnGuid : columnGuids) {
            AtlasEntity column = getEntity(columnGuid);
            assertEquals(ACTIVE, column.getStatus());
            Map<String, Object> parentRelation = (Map<String, Object>) column.getRelationshipAttribute(TABLE);
            assertNotNull(parentRelation);
            assertEquals("ACTIVE", parentRelation.get("relationshipStatus"));
        }

        // Restore table by updating status to ACTIVE
        table.setStatus(ACTIVE);
        EntityMutationResponse restoreResponse = updateEntity(table);
        assertNotNull(restoreResponse);
        assertEquals(1, restoreResponse.getUpdatedEntities().size());
        sleep(SLEEP);

        // Verify after restore
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        verifyESAttributes(tableGuid, mapOf(ATTR_STATE, ACTIVE.name()));

        for (String columnGuid : columnGuids) {
            AtlasEntity column = getEntity(columnGuid);
            assertEquals(ACTIVE, column.getStatus());
            Map<String, Object> parentRelation = (Map<String, Object>) column.getRelationshipAttribute(TABLE);
            assertNotNull(parentRelation);
            assertEquals("ACTIVE", parentRelation.get("relationshipStatus"));
        }

        LOG.info("<< testRestoreSoftDeletedTable");
    }

    private void testRestoreSoftDeletedColumn() throws Exception {
        LOG.info(">> testRestoreSoftDeletedColumn");

        // Create a test table with column
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_default_delete" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Create column and link it to the table
        List<String> columnGuids = createColumnsForTable(tableGuid, 2);
        sleep(SLEEP);
        String columnGuidToRestore = columnGuids.get(0);

        // Perform default delete on column
        EntityMutationResponse response = deleteEntitySoft(columnGuidToRestore);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_SOFT, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify column is deleted but still retrievable
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());

        for (String columnGuid : columnGuids) {
            AtlasEntity column = getEntity(columnGuid);
            Map<String, Object> parentRelation = (Map<String, Object>) column.getRelationshipAttribute(TABLE);
            assertNotNull(parentRelation);

            AtlasEntity.Status expectedStatus = columnGuidToRestore.equals(column.getGuid()) ? DELETED : ACTIVE;
            assertEquals(expectedStatus, column.getStatus());
            assertEquals(expectedStatus.name(), parentRelation.get("relationshipStatus"));
            verifyESAttributes(columnGuid, mapOf(ATTR_STATE, expectedStatus.name()));
        }

        // Restore deleted column by updating status to ACTIVE
        AtlasEntity column = getEntity(columnGuidToRestore);
        column.setStatus(ACTIVE);
        EntityMutationResponse restoreResponse = updateEntity(column);
        assertNotNull(restoreResponse);
        assertEquals(1, restoreResponse.getUpdatedEntities().size());
        sleep(SLEEP);


        // Verify after restore
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());

        for (String columnGuid : columnGuids) {
            AtlasEntity columnE = getEntity(columnGuid);
            Map<String, Object> parentRelation = (Map<String, Object>) columnE.getRelationshipAttribute(TABLE);
            assertNotNull(parentRelation);

            assertEquals(ACTIVE, columnE.getStatus());
            assertEquals(ACTIVE.name(), parentRelation.get("relationshipStatus"));
            verifyESAttributes(columnGuid, mapOf(ATTR_STATE, ACTIVE.name()));
        }

        LOG.info("<< testRestoreSoftDeletedColumn");
    }
}
