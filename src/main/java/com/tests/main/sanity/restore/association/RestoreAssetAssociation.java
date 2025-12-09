package com.tests.main.sanity.restore.association;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.tests.main.utils.Constants.DELETE_HANDLER_SOFT;
import static com.tests.main.utils.Constants.DIMENSIONS;
import static com.tests.main.utils.Constants.FACTS;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createDimensionTablesForFact;
import static com.tests.main.utils.TestUtil.createTables;
import static com.tests.main.utils.TestUtil.deleteEntitySoft;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.mapOf;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;
import static com.tests.main.utils.TestUtil.verifyESAttributes;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RestoreAssetAssociation implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreAssetAssociation.class);

    private static long SLEEP = 1000;

    public static void main(String[] args) throws Exception {
        try {
            new RestoreAssetAssociation().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running column delete RestoreAssetAssociation tests");

        testRestoreSoftDeletedFactTable();

        testRestoreSoftDeletedDimensionTable(); //Inverse
    }

    private void testRestoreSoftDeletedFactTable() throws Exception {
        LOG.info(">> testRestoreSoftDeletedFactTable");

        // Create fact tables
        List<String> factTableGuids = createTables( 2);
        sleep(SLEEP);
        String factGuidToRestore = factTableGuids.get(0);

        // Create dimension tables and link them to the fact table
        List<String> dimensionTableGuids = createDimensionTablesForFact(2, factTableGuids.toArray(new String[factTableGuids.size()]));
        sleep(SLEEP);


        // Perform soft delete
        EntityMutationResponse deleteResponse = deleteEntitySoft(factGuidToRestore);
        assertNotNull(deleteResponse);
        assertTrue(deleteResponse.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_SOFT, deleteResponse.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify after delete
        for (String factTableGuid : factTableGuids) {
            AtlasEntity factTable = getEntity(factTableGuid);
            List<Map<String, Object>> dimList = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
            assertNotNull(dimList);
            assertEquals(2, dimList.size());

            AtlasEntity.Status expectedStatus = factGuidToRestore.equals(factTableGuid) ? DELETED : ACTIVE;
            assertEquals(expectedStatus, factTable.getStatus());
            verifyESAttributes(factTableGuid, mapOf(ATTR_STATE, expectedStatus.name()));

            assertEquals(2, dimList.stream().filter(x -> ACTIVE.name().equals(x.get("relationshipStatus"))).count());
        }

        // Restore table by updating status to ACTIVE
        AtlasEntity factTableToRestore = getEntity(factGuidToRestore);
        factTableToRestore.setStatus(ACTIVE);
        EntityMutationResponse restoreResponse = updateEntity(factTableToRestore);
        assertNotNull(restoreResponse);
        assertEquals(1, restoreResponse.getUpdatedEntities().size());
        sleep(SLEEP);

        // Verify dimension tables after restore
        for (String dimensionTableGuid : dimensionTableGuids) {
            AtlasEntity dimTable = getEntity(dimensionTableGuid);
            assertEquals(ACTIVE, dimTable.getStatus());
            List<Map<String, Object>> factsList = (List<Map<String, Object>>) dimTable.getRelationshipAttribute(FACTS);
            assertNotNull(factsList);
            assertEquals(2, factsList.size());
            verifyESAttributes(dimensionTableGuid, mapOf(ATTR_STATE, ACTIVE.name()));

            assertEquals(2, factsList.stream().filter(x -> ACTIVE.name().equals(x.get("relationshipStatus"))).count());
        }

        // Verify facts tables after restore
        for (String factTableGuid : factTableGuids) {
            AtlasEntity factTable = getEntity(factTableGuid);
            List<Map<String, Object>> dimList = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
            assertNotNull(dimList);
            assertEquals(2, dimList.size());

            AtlasEntity.Status expectedStatus = ACTIVE;
            assertEquals(expectedStatus, factTable.getStatus());
            verifyESAttributes(factTableGuid, mapOf(ATTR_STATE, expectedStatus.name()));

            assertEquals(2, dimList.stream().filter(x -> ACTIVE.name().equals(x.get("relationshipStatus"))).count());
        }

        LOG.info("<< testRestoreSoftDeletedFactTable");
    }

    private void testRestoreSoftDeletedDimensionTable() throws Exception {
        LOG.info(">> testRestoreSoftDeletedDimensionTable");

        // Create fact tables
        List<String> factTableGuids = createTables( 2);
        sleep(SLEEP);

        // Create dimension tables and link them to the fact table
        List<String> dimensionTableGuids = createDimensionTablesForFact(2, factTableGuids.toArray(new String[factTableGuids.size()]));
        sleep(SLEEP);
        String dimGuidToRestore = dimensionTableGuids.get(0);


        // Perform soft delete
        EntityMutationResponse deleteResponse = deleteEntitySoft(dimGuidToRestore);
        assertNotNull(deleteResponse);
        assertTrue(deleteResponse.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_SOFT, deleteResponse.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(SLEEP);

        // Verify after delete
        for (String dimTableGuid : dimensionTableGuids) {
            AtlasEntity dimTable = getEntity(dimTableGuid);
            List<Map<String, Object>> factList = (List<Map<String, Object>>) dimTable.getRelationshipAttribute(FACTS);
            assertNotNull(factList);
            assertEquals(2, factList.size());

            AtlasEntity.Status expectedStatus = dimGuidToRestore.equals(dimTableGuid) ? DELETED : ACTIVE;
            assertEquals(expectedStatus, dimTable.getStatus());
            verifyESAttributes(dimTableGuid, mapOf(ATTR_STATE, expectedStatus.name()));

            assertEquals(2, factList.stream().filter(x -> expectedStatus.name().equals(x.get("relationshipStatus"))).count());
        }

        // Restore table by updating status to ACTIVE
        AtlasEntity dimTableToRestore = getEntity(dimGuidToRestore);
        dimTableToRestore.setStatus(ACTIVE);
        EntityMutationResponse restoreResponse = updateEntity(dimTableToRestore);
        assertNotNull(restoreResponse);
        assertEquals(1, restoreResponse.getUpdatedEntities().size());
        sleep(SLEEP);

        // Verify dimension tables after restore
        for (String dimensionTableGuid : dimensionTableGuids) {
            AtlasEntity dimTable = getEntity(dimensionTableGuid);
            assertEquals(ACTIVE, dimTable.getStatus());
            List<Map<String, Object>> factsList = (List<Map<String, Object>>) dimTable.getRelationshipAttribute(FACTS);
            assertNotNull(factsList);
            assertEquals(2, factsList.size());
            assertEquals(ACTIVE.name(), factsList.get(0).get("relationshipStatus"));
            verifyESAttributes(dimensionTableGuid, mapOf(ATTR_STATE, ACTIVE.name()));

            assertEquals(2, factsList.stream().filter(x -> ACTIVE.name().equals(x.get("relationshipStatus"))).count());
        }

        // Verify facts tables after restore
        for (String factTableGuid : factTableGuids) {
            AtlasEntity factTable = getEntity(factTableGuid);
            List<Map<String, Object>> dimList = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
            assertNotNull(dimList);
            assertEquals(2, dimList.size());

            AtlasEntity.Status expectedStatus = ACTIVE;
            assertEquals(expectedStatus, factTable.getStatus());
            verifyESAttributes(factTableGuid, mapOf(ATTR_STATE, expectedStatus.name()));

            assertEquals(2, dimList.stream().filter(x -> ACTIVE.name().equals(x.get("relationshipStatus"))).count());
        }

        LOG.info("<< testRestoreSoftDeletedDimensionTable");
    }
}
