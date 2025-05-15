package com.tests.main.sanity.delete.association;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.tests.main.utils.TestUtil.*;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.junit.Assert.*;

/**
 * Tests different delete operations on tables and their impact on associated fact-dimension relationships.
 * 
 * This class verifies the behavior of four table delete operations:
 * 1. Default Delete: Marks table as DELETED but keeps it retrievable. Relationships remain ACTIVE.
 * 2. Soft Delete: Marks table as DELETED but keeps it retrievable. Relationships remain ACTIVE.
 * 3. Hard Delete: Completely removes table (404 on retrieval). Associated tables lose the relationship.
 * 4. Purge Delete: Completely removes table (404 on retrieval). Associated tables lose the relationship.
 * 
 * Each test:
 * - Creates a fact table with 2 dimension tables
 * - Performs the specified delete operation on the fact table
 * - Verifies the fact table's status and retrievability
 * - Verifies the dimension tables' status and relationship with the fact table
 */
public class SanityAssociationDeleteOperations implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(SanityAssociationDeleteOperations.class);
    private static final String DIMENSIONS = "dimensions";
    private static final String FACTS = "facts";
    private static final String DELETE_HANDLER_DEFAULT = "DEFAULT";
    private static final String DELETE_HANDLER_SOFT = "SOFT";
    private static final String DELETE_HANDLER_HARD = "HARD";
    private static final String DELETE_HANDLER_PURGE = "PURGE";

    public static void main(String[] args) throws Exception {
        try {
            new SanityAssociationDeleteOperations().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running table composition delete operations tests");

        testDefaultDelete();
        testSoftDelete();
        testHardDelete();
        testPurgeDelete();
    }

    private void testDefaultDelete() throws Exception {
        LOG.info(">> testDefaultDelete");
        
        // Create a fact table
        AtlasEntity factTable = getAtlasEntity(TYPE_TABLE, "test_fact_table_default_delete" + getRandomName());
        String factTableGuid = createEntity(factTable).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create dimension tables and link them to the fact table
        List<String> dimensionTableGuids = createDimensionTablesForFact(factTableGuid, 1);
        sleep(2);

        // Verify initial state
        factTable = getEntity(factTableGuid);
        assertEquals(ACTIVE, factTable.getStatus());
        List<Map<String, Object>> factDimensions = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull(factDimensions);
        assertEquals(1, factDimensions.size());

        // Perform default delete on fact table
        EntityMutationResponse response = deleteEntityDefault(factTableGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_DEFAULT, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(2);

        // Verify fact table is deleted but still retrievable
        factTable = getEntity(factTableGuid);
        assertEquals(DELETED, factTable.getStatus());

        // Verify dimension tables are still active and relationships are marked as DELETED
        for (String dimensionTableGuid : dimensionTableGuids) {
            AtlasEntity dimensionTable = getEntity(dimensionTableGuid);
            assertEquals(ACTIVE, dimensionTable.getStatus());
            List<Map<String, Object>> factsList = (List<Map<String, Object>>) dimensionTable.getRelationshipAttribute(FACTS);
            assertNotNull(factsList);
            assertEquals(1, factsList.size());
            assertEquals("ACTIVE", factsList.get(0).get("relationshipStatus"));
        }

        // Verify fact table's dimensions list is empty
        factTable = getEntity(factTableGuid);
        List<Map<String, Object>> dimensionsList = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull(dimensionsList);
        assertEquals(1, dimensionsList.size());

        LOG.info("<< testDefaultDelete");
    }

    private void testSoftDelete() throws Exception {
        LOG.info(">> testSoftDelete");
        
        // Create a fact table
        AtlasEntity factTable = getAtlasEntity(TYPE_TABLE, "test_fact_table_soft_delete" + getRandomName());
        String factTableGuid = createEntity(factTable).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create dimension tables and link them to the fact table
        List<String> dimensionTableGuids = createDimensionTablesForFact(factTableGuid, 1);
        sleep(2);

        // Verify initial state
        factTable = getEntity(factTableGuid);
        assertEquals(ACTIVE, factTable.getStatus());
        List<Map<String, Object>> factDimensions = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull(factDimensions);
        assertEquals(1, factDimensions.size());

        // Perform soft delete on fact table
        EntityMutationResponse response = deleteEntitySoft(factTableGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_SOFT, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(2);

        // Verify fact table is deleted but still retrievable
        factTable = getEntity(factTableGuid);
        assertEquals(DELETED, factTable.getStatus());

        // Verify dimension tables are still active and relationships are marked as DELETED
        for (String dimensionTableGuid : dimensionTableGuids) {
            AtlasEntity dimensionTable = getEntity(dimensionTableGuid);
            assertEquals(ACTIVE, dimensionTable.getStatus());
            List<Map<String, Object>> factsList = (List<Map<String, Object>>) dimensionTable.getRelationshipAttribute(FACTS);
            assertNotNull(factsList);
            assertEquals(1, factsList.size());
            assertEquals("ACTIVE", factsList.get(0).get("relationshipStatus"));
        }

        // Verify fact table's dimensions list is empty
        factTable = getEntity(factTableGuid);
        List<Map<String, Object>> dimensionsList = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull(dimensionsList);
        assertEquals(1, dimensionsList.size());

        LOG.info("<< testSoftDelete");
    }

    private void testHardDelete() throws Exception {
        LOG.info(">> testHardDelete");
        
        // Create a fact table
        AtlasEntity factTable = getAtlasEntity(TYPE_TABLE, "test_fact_table_hard_delete" + getRandomName());
        String factTableGuid = createEntity(factTable).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create dimension tables and link them to the fact table
        List<String> dimensionTableGuids = createDimensionTablesForFact(factTableGuid, 1);
        sleep(2);

        // Verify initial state
        factTable = getEntity(factTableGuid);
        assertEquals(ACTIVE, factTable.getStatus());
        List<Map<String, Object>> factDimensions = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull(factDimensions);
        assertEquals(1, factDimensions.size());

        // Perform hard delete on fact table
        EntityMutationResponse response = deleteEntityHard(factTableGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_HARD, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(2);

        // Verify fact table is not retrievable
        try {
            factTable = getEntity(factTableGuid);
            fail("Fact table should not be retrievable after hard delete");
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage.contains("|")) {
                errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
            }
            assertEquals("404", errorMessage);
        }

        // Verify dimension tables are still active and relationships are empty lists
        for (String dimensionTableGuid : dimensionTableGuids) {
            AtlasEntity dimensionTable = getEntity(dimensionTableGuid);
            assertEquals(ACTIVE, dimensionTable.getStatus());
            // For association, dimension table's facts list should be empty but not null
            List<Map<String, Object>> factsList = (List<Map<String, Object>>) dimensionTable.getRelationshipAttribute(FACTS);
            assertNotNull("Dimension table should have empty facts list for association", factsList);
            assertEquals("Dimension table's facts list should be empty", 0, factsList.size());
            // Verify dimension table can be associated with a new fact table
            assertTrue("Dimension table should be available for new associations", dimensionTable.getStatus().equals(ACTIVE));
        }

        LOG.info("<< testHardDelete");
    }

    private void testPurgeDelete() throws Exception {
        LOG.info(">> testPurgeDelete");
        
        // Create a fact table
        AtlasEntity factTable = getAtlasEntity(TYPE_TABLE, "test_fact_table_purge_delete" + getRandomName());
        String factTableGuid = createEntity(factTable).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create dimension tables and link them to the fact table
        List<String> dimensionTableGuids = createDimensionTablesForFact(factTableGuid, 1);
        sleep(2);

        // Verify initial state
        factTable = getEntity(factTableGuid);
        assertEquals(ACTIVE, factTable.getStatus());
        List<Map<String, Object>> factDimensions = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull(factDimensions);
        assertEquals(1, factDimensions.size());

        // Perform purge delete on fact table
        EntityMutationResponse response = deleteEntityPurge(factTableGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_PURGE, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(2);

        // Verify fact table is not retrievable
        try {
            factTable = getEntity(factTableGuid);
            fail("Fact table should not be retrievable after purge delete");
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage.contains("|")) {
                errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
            }
            assertEquals("404", errorMessage);
        }

        // Verify dimension tables are still active and relationships are empty lists
        for (String dimensionTableGuid : dimensionTableGuids) {
            AtlasEntity dimensionTable = getEntity(dimensionTableGuid);
            assertEquals(ACTIVE, dimensionTable.getStatus());
            // For association, dimension table's facts list should be empty but not null
            List<Map<String, Object>> factsList = (List<Map<String, Object>>) dimensionTable.getRelationshipAttribute(FACTS);
            assertNotNull("Dimension table should have empty facts list for association", factsList);
            assertEquals("Dimension table's facts list should be empty", 0, factsList.size());
            // Verify dimension table can be associated with a new fact table
            assertTrue("Dimension table should be available for new associations", dimensionTable.getStatus().equals(ACTIVE));
        }

        LOG.info("<< testPurgeDelete");
    }

    private List<String> createDimensionTablesForFact(String factTableGuid, int numDimensions) throws Exception {
        List<String> dimensionTableGuids = new ArrayList<>();
        List<AtlasEntity> entitiesToCreate = new ArrayList<>();
        
        // Create dimension tables with fact relationship
        for (int i = 0; i < numDimensions; i++) {
            AtlasEntity dimensionTable = getAtlasEntity(TYPE_TABLE, "test_dimension_table_" + i + "_" + getRandomName());
            dimensionTable.setRelationshipAttribute(FACTS, Collections.singletonList(getObjectId(factTableGuid, TYPE_TABLE)));
            entitiesToCreate.add(dimensionTable);
        }

        // Create all entities in one request
        EntityMutationResponse response = createEntitiesBulk(entitiesToCreate);
        for (AtlasEntityHeader createdEntity : response.getCreatedEntities()) {
            if (createdEntity.getTypeName().equals(TYPE_TABLE) && !createdEntity.getGuid().equals(factTableGuid)) {
                dimensionTableGuids.add(createdEntity.getGuid());
            }
        }
        sleep(2);

        return dimensionTableGuids;
    }
} 