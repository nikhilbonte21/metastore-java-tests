package com.tests.main.sanity.delete.association;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.tests.main.utils.TestUtil.*;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.junit.Assert.*;

/**
 * Tests different delete operations on dimension tables and their impact on associated fact-dimension relationships.
 * This is the inverse of SanityTableAssociationDeleteOperations - testing deletion of dimension tables instead of fact tables.
 * 
 * This class verifies the behavior of four table delete operations:
 * 1. Default Delete: Marks table as DELETED but keeps it retrievable. Relationships remain ACTIVE.
 * 2. Soft Delete: Marks table as DELETED but keeps it retrievable. Relationships remain ACTIVE.
 * 3. Hard Delete: Completely removes table (404 on retrieval). Associated tables lose the relationship.
 * 4. Purge Delete: Completely removes table (404 on retrieval). Associated tables lose the relationship.
 * 
 * Each test:
 * - Creates a fact table with 1 dimension table
 * - Performs the specified delete operation on the dimension table
 * - Verifies the dimension table's status and retrievability
 * - Verifies the fact table's status and relationship with the dimension table
 */
public class SanityAssociationDeleteOperationsInverse implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(SanityAssociationDeleteOperationsInverse.class);
    private static final String DIMENSIONS = "dimensions";
    private static final String FACTS = "facts";
    private static final String DELETE_HANDLER_DEFAULT = "DEFAULT";
    private static final String DELETE_HANDLER_SOFT = "SOFT";
    private static final String DELETE_HANDLER_HARD = "HARD";
    private static final String DELETE_HANDLER_PURGE = "PURGE";

    public static void main(String[] args) throws Exception {
        try {
            new SanityAssociationDeleteOperationsInverse().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running inverse table association delete operations tests");

        testDefaultDelete();
        testSoftDelete();
        testHardDelete();
        testPurgeDelete();
    }

    private void testDefaultDelete() throws Exception {
        LOG.info(">> testDefaultDelete");
        
        // Create a fact table
        AtlasEntity factTable = getAtlasEntity(TYPE_TABLE, "test_fact_table_default_delete_inverse" + getRandomName());
        String factTableGuid = createEntity(factTable).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create dimension table and link it to the fact table
        String dimensionTableGuid = createDimensionTableForFact(factTableGuid);
        sleep(2);

        // Verify initial state
        factTable = getEntity(factTableGuid);
        assertEquals(ACTIVE, factTable.getStatus());
        List<Map<String, Object>> factDimensions = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull(factDimensions);
        assertEquals(1, factDimensions.size());

        // Perform default delete on dimension table
        EntityMutationResponse response = deleteEntityDefault(dimensionTableGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_DEFAULT, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(2);

        // Verify dimension table is deleted but still retrievable
        AtlasEntity dimensionTable = getEntity(dimensionTableGuid);
        assertEquals(DELETED, dimensionTable.getStatus());
        verifyESAttributes(dimensionTableGuid, mapOf(ATTR_STATE, DELETED.name()));

        // Verify fact table is still active and relationship remains ACTIVE
        factTable = getEntity(factTableGuid);
        assertEquals(ACTIVE, factTable.getStatus());
        verifyESAttributes(factTableGuid, mapOf(ATTR_STATE, ACTIVE.name()));
        List<Map<String, Object>> dimensionsList = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull("Fact table should have dimensions list for association", dimensionsList);
        assertEquals("Fact table should maintain relationship after default delete", 1, dimensionsList.size());
        assertEquals("Relationship should remain ACTIVE after default delete", "DELETED", dimensionsList.get(0).get("relationshipStatus"));

        // Verify dimension table's relationship is also ACTIVE
        List<Map<String, Object>> factsList = (List<Map<String, Object>>) dimensionTable.getRelationshipAttribute(FACTS);
        assertNotNull("Dimension table should have facts list for association", factsList);
        assertEquals("Dimension table should maintain relationship after default delete", 1, factsList.size());
        assertEquals("Relationship should remain ACTIVE after default delete", "DELETED", factsList.get(0).get("relationshipStatus"));

        LOG.info("<< testDefaultDelete");
    }

    private void testSoftDelete() throws Exception {
        LOG.info(">> testSoftDelete");
        
        // Create a fact table
        AtlasEntity factTable = getAtlasEntity(TYPE_TABLE, "test_fact_table_soft_delete_inverse" + getRandomName());
        String factTableGuid = createEntity(factTable).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create dimension table and link it to the fact table
        String dimensionTableGuid = createDimensionTableForFact(factTableGuid);
        sleep(2);

        // Verify initial state
        factTable = getEntity(factTableGuid);
        assertEquals(ACTIVE, factTable.getStatus());
        List<Map<String, Object>> factDimensions = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull(factDimensions);
        assertEquals(1, factDimensions.size());

        // Perform soft delete on dimension table
        EntityMutationResponse response = deleteEntitySoft(dimensionTableGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_SOFT, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(2);

        // Verify dimension table is deleted but still retrievable
        AtlasEntity dimensionTable = getEntity(dimensionTableGuid);
        assertEquals(DELETED, dimensionTable.getStatus());
        verifyESAttributes(dimensionTableGuid, mapOf(ATTR_STATE, DELETED.name()));

        // Verify fact table is still active and relationship remains ACTIVE
        factTable = getEntity(factTableGuid);
        assertEquals(ACTIVE, factTable.getStatus());
        verifyESAttributes(factTableGuid, mapOf(ATTR_STATE, ACTIVE.name()));
        List<Map<String, Object>> dimensionsList = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull("Fact table should have dimensions list for association", dimensionsList);
        assertEquals("Fact table should maintain relationship after soft delete", 1, dimensionsList.size());
        assertEquals("Relationship should remain ACTIVE after soft delete", "DELETED", dimensionsList.get(0).get("relationshipStatus"));

        // Verify dimension table's relationship is also ACTIVE
        List<Map<String, Object>> factsList = (List<Map<String, Object>>) dimensionTable.getRelationshipAttribute(FACTS);
        assertNotNull("Dimension table should have facts list for association", factsList);
        assertEquals("Dimension table should maintain relationship after soft delete", 1, factsList.size());
        assertEquals("Relationship should remain ACTIVE after soft delete", "DELETED", factsList.get(0).get("relationshipStatus"));

        LOG.info("<< testSoftDelete");
    }

    private void testHardDelete() throws Exception {
        LOG.info(">> testHardDelete");
        
        // Create a fact table
        AtlasEntity factTable = getAtlasEntity(TYPE_TABLE, "test_fact_table_hard_delete_inverse" + getRandomName());
        String factTableGuid = createEntity(factTable).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create dimension table and link it to the fact table
        String dimensionTableGuid = createDimensionTableForFact(factTableGuid);
        sleep(2);

        // Verify initial state
        factTable = getEntity(factTableGuid);
        assertEquals(ACTIVE, factTable.getStatus());
        List<Map<String, Object>> factDimensions = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull(factDimensions);
        assertEquals(1, factDimensions.size());

        // Perform hard delete on dimension table
        EntityMutationResponse response = deleteEntityHard(dimensionTableGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_HARD, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(2);

        // Verify dimension table is not retrievable
        try {
            verifyESDocumentNotPresent(dimensionTableGuid);
            getEntity(dimensionTableGuid);
            fail("Dimension table should not be retrievable after hard delete");
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage.contains("|")) {
                errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
            }
            assertEquals("404", errorMessage);
        }

        // Verify fact table is still active and relationship list is empty but not null
        factTable = getEntity(factTableGuid);
        assertEquals(ACTIVE, factTable.getStatus());
        verifyESAttributes(factTableGuid, mapOf(ATTR_STATE, ACTIVE.name()));
        List<Map<String, Object>> dimensionsList = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull("Fact table should have empty dimensions list for association", dimensionsList);
        assertEquals("Fact table's dimensions list should be empty", 0, dimensionsList.size());

        LOG.info("<< testHardDelete");
    }

    private void testPurgeDelete() throws Exception {
        LOG.info(">> testPurgeDelete");
        
        // Create a fact table
        AtlasEntity factTable = getAtlasEntity(TYPE_TABLE, "test_fact_table_purge_delete_inverse" + getRandomName());
        String factTableGuid = createEntity(factTable).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create dimension table and link it to the fact table
        String dimensionTableGuid = createDimensionTableForFact(factTableGuid);
        sleep(2);

        // Verify initial state
        factTable = getEntity(factTableGuid);
        assertEquals(ACTIVE, factTable.getStatus());
        List<Map<String, Object>> factDimensions = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull(factDimensions);
        assertEquals(1, factDimensions.size());

        // Perform purge delete on dimension table
        EntityMutationResponse response = deleteEntityPurge(dimensionTableGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(DELETE_HANDLER_PURGE, response.getDeletedEntities().get(0).getDeleteHandler().toUpperCase());
        sleep(2);

        // Verify dimension table is not retrievable
        try {
            verifyESDocumentNotPresent(dimensionTableGuid);
            getEntity(dimensionTableGuid);
            fail("Dimension table should not be retrievable after purge delete");
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage.contains("|")) {
                errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
            }
            assertEquals("404", errorMessage);
        }

        // Verify fact table is still active and relationship list is empty but not null
        factTable = getEntity(factTableGuid);
        assertEquals(ACTIVE, factTable.getStatus());
        verifyESAttributes(factTableGuid, mapOf(ATTR_STATE, ACTIVE.name()));
        List<Map<String, Object>> dimensionsList = (List<Map<String, Object>>) factTable.getRelationshipAttribute(DIMENSIONS);
        assertNotNull("Fact table should have empty dimensions list for association", dimensionsList);
        assertEquals("Fact table's dimensions list should be empty", 0, dimensionsList.size());

        LOG.info("<< testPurgeDelete");
    }

    private String createDimensionTableForFact(String factTableGuid) throws Exception {
        // Create dimension table with fact relationship
        AtlasEntity dimensionTable = getAtlasEntity(TYPE_TABLE, "test_dimension_table_" + getRandomName());
        dimensionTable.setRelationshipAttribute(FACTS, Collections.singletonList(getObjectId(factTableGuid, TYPE_TABLE)));
        
        // Create entity
        EntityMutationResponse response = createEntity(dimensionTable);
        String dimensionTableGuid = response.getCreatedEntities().get(0).getGuid();
        sleep(2);

        return dimensionTableGuid;
    }
} 