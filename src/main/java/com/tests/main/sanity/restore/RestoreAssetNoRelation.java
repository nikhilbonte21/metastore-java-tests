package com.tests.main.sanity.restore;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.utils.Constants.DELETE_HANDLER_SOFT;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.deleteEntitySoft;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.mapOf;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;
import static com.tests.main.utils.TestUtil.verifyESAttributes;
import static org.junit.Assert.*;

import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;

public class RestoreAssetNoRelation implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreAssetNoRelation.class);

    private static long SLEEP = 1000;

    public static void main(String[] args) throws Exception {
        try {
            new RestoreAssetNoRelation().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running column delete RestoreAssetNoRelation tests");

        testRestoreSoftDeletedTable();
    }

    private void testRestoreSoftDeletedTable() throws Exception {
        LOG.info(">> testRestoreSoftDeletedTable");

        // Create a test table
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_restore" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Verify initial state
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        verifyESAttributes(tableGuid, mapOf(ATTR_STATE, ACTIVE));

        // Perform soft delete
        EntityMutationResponse deleteResponse = deleteEntitySoft(tableGuid);
        assertNotNull(deleteResponse);
        assertTrue(deleteResponse.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_SOFT, deleteResponse.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify table is soft deleted
        table = getEntity(tableGuid);
        assertEquals(DELETED, table.getStatus());
        verifyESAttributes(tableGuid, mapOf(ATTR_STATE, DELETED.name()));

        // Restore table by updating status to ACTIVE
        table.setStatus(ACTIVE);
        EntityMutationResponse restoreResponse = updateEntity(table);
        assertNotNull(restoreResponse);
        assertTrue(restoreResponse.getUpdatedEntities().size() == 0);
        sleep(SLEEP);

        // Verify table is restored to ACTIVE state
        table = getEntity(tableGuid);
        assertEquals(ACTIVE, table.getStatus());
        verifyESAttributes(tableGuid, mapOf(ATTR_STATE, ACTIVE.name()));

        LOG.info("<< testRestoreSoftDeletedTable");
    }
}
