package com.tests.main.sanity.delete.composition;

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

import static com.tests.main.utils.TestUtil.ANCHOR;
import static com.tests.main.utils.TestUtil.REL_CATEGORIES;
import static com.tests.main.utils.TestUtil.REL_TERMS;
import static com.tests.main.utils.TestUtil.TYPE_CATEGORY;
import static com.tests.main.utils.TestUtil.TYPE_GLOSSARY;
import static com.tests.main.utils.TestUtil.TYPE_TERM;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.deleteEntityDefault;
import static com.tests.main.utils.TestUtil.deleteEntityHard;
import static com.tests.main.utils.TestUtil.deleteEntityPurge;
import static com.tests.main.utils.TestUtil.deleteEntitySoft;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getObjectId;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.sleep;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
public class SanityCompositionDeleteOperations implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(SanityCompositionDeleteOperations.class);
    private static final String DIMENSIONS = "dimensions";
    private static final String FACTS = "facts";
    private static final String DELETE_HANDLER_DEFAULT = "DEFAULT";
    private static final String DELETE_HANDLER_SOFT = "SOFT";
    private static final String DELETE_HANDLER_HARD = "HARD";
    private static final String DELETE_HANDLER_PURGE = "PURGE";

    private static int NUM_CHILDREN = 1;

    public static void main(String[] args) throws Exception {
        try {
            new SanityCompositionDeleteOperations().run();
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
        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "test_fact_table_default_delete" + getRandomName());
        String glossaryGuid = createEntity(glossary).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create dimension tables and link them to the fact table
        List<String> termGuids = createChildrenTerms(glossaryGuid, NUM_CHILDREN);
        List<String> categoryGuids = createChildrenCategories(glossaryGuid, NUM_CHILDREN);
        sleep(2);

        // Verify initial state
        glossary = getEntity(glossaryGuid);
        assertEquals(ACTIVE, glossary.getStatus());
        List<Map<String, Object>> terms = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_TERMS);
        List<Map<String, Object>> categories = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_CATEGORIES);
        assertNotNull(terms);
        assertEquals(NUM_CHILDREN * 2 , terms.size()); // * 2 since terms linked to Categories are also linked to Glossary
        assertNotNull(categories);
        assertEquals(NUM_CHILDREN, categories.size());
        for (String categoryGuid : categoryGuids) {
            AtlasEntity category = getEntity(categoryGuid);
            List<Map<String, Object>> categoryTerms = (List<Map<String, Object>>) category.getRelationshipAttribute("terms");
            assertNotNull(categoryTerms);
            assertEquals(NUM_CHILDREN, categoryTerms.size());
            assertEquals(NUM_CHILDREN, categoryTerms.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());
        }


        // Delete Glossary
        EntityMutationResponse response = deleteEntityDefault(glossaryGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(1 + (NUM_CHILDREN * 2), response.getDeletedEntities().stream().filter(x -> DELETE_HANDLER_DEFAULT.equals(x.getDeleteHandler().toUpperCase())).count());
        assertEquals(NUM_CHILDREN, response.getDeletedEntities().stream().filter(x -> DELETE_HANDLER_HARD.equals(x.getDeleteHandler().toUpperCase())).count());
        sleep(2);

        // Verify Glossary is deleted but still retrievable
        glossary = getEntity(glossaryGuid);
        assertEquals(DELETED, glossary.getStatus());

        // Verify children status
        terms = (List<Map<String, Object>>) glossary.getRelationshipAttribute("terms");
        categories = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_CATEGORIES);
        assertNotNull(terms);
        assertEquals(NUM_CHILDREN * 2, terms.size());
        assertNotNull(categories);
        assertEquals(NUM_CHILDREN, categories.size());

        for (String termGuid : termGuids) {
            AtlasEntity term = getEntity(termGuid);
            assertEquals(DELETED, term.getStatus());
            List<Map<String, Object>> termCategories = (List<Map<String, Object>>) term.getRelationshipAttribute(REL_CATEGORIES);
            assertTrue(termCategories.isEmpty());
        }
        for (String categoryGuid : categoryGuids) {
            AtlasEntity category = getEntity(categoryGuid);
            List<Map<String, Object>> categoryTerms = (List<Map<String, Object>>) category.getRelationshipAttribute("terms");
            assertNotNull(categoryTerms);
            assertEquals(NUM_CHILDREN, categoryTerms.size());
            assertEquals(NUM_CHILDREN, categoryTerms.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());
        }

        LOG.info("<< testDefaultDelete");
    }

    private void testSoftDelete() throws Exception {
        LOG.info(">> testSoftDelete");
        
        // Create a glossary (parent)
        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_soft_delete" + getRandomName());
        String glossaryGuid = createEntity(glossary).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create terms and categories (children)
        List<String> termGuids = createChildrenTerms(glossaryGuid, NUM_CHILDREN);
        List<String> categoryGuids = createChildrenCategories(glossaryGuid, NUM_CHILDREN);
        sleep(2);

        // Verify initial state
        glossary = getEntity(glossaryGuid);
        assertEquals(ACTIVE, glossary.getStatus());
        List<Map<String, Object>> terms = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_TERMS);
        List<Map<String, Object>> categories = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_CATEGORIES);
        assertNotNull(terms);
        assertEquals(NUM_CHILDREN * 2, terms.size()); // * 2 since terms linked to Categories are also linked to Glossary
        assertNotNull(categories);
        assertEquals(NUM_CHILDREN, categories.size());
        for (String categoryGuid : categoryGuids) {
            AtlasEntity category = getEntity(categoryGuid);
            List<Map<String, Object>> categoryTerms = (List<Map<String, Object>>) category.getRelationshipAttribute("terms");
            assertNotNull(categoryTerms);
            assertEquals(NUM_CHILDREN, categoryTerms.size());
            assertEquals(NUM_CHILDREN, categoryTerms.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());
        }

        // Delete Glossary with soft delete
        EntityMutationResponse response = deleteEntitySoft(glossaryGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(1 + (NUM_CHILDREN * 2), response.getDeletedEntities().stream().filter(x -> DELETE_HANDLER_SOFT.equals(x.getDeleteHandler().toUpperCase())).count());
        assertEquals(NUM_CHILDREN, response.getDeletedEntities().stream().filter(x -> DELETE_HANDLER_HARD.equals(x.getDeleteHandler().toUpperCase())).count());
        sleep(2);

        // Verify Glossary is deleted but still retrievable
        glossary = getEntity(glossaryGuid);
        assertEquals(DELETED, glossary.getStatus());

        // Verify children status
        terms = (List<Map<String, Object>>) glossary.getRelationshipAttribute("terms");
        categories = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_CATEGORIES);
        assertNotNull(terms);
        assertEquals(NUM_CHILDREN * 2, terms.size());
        assertNotNull(categories);
        assertEquals(NUM_CHILDREN, categories.size());

        for (String termGuid : termGuids) {
            AtlasEntity term = getEntity(termGuid);
            assertEquals(DELETED, term.getStatus());
            List<Map<String, Object>> termCategories = (List<Map<String, Object>>) term.getRelationshipAttribute(REL_CATEGORIES);
            assertTrue(termCategories.isEmpty());
        }
        for (String categoryGuid : categoryGuids) {
            AtlasEntity category = getEntity(categoryGuid);
            List<Map<String, Object>> categoryTerms = (List<Map<String, Object>>) category.getRelationshipAttribute("terms");
            assertNotNull(categoryTerms);
            assertEquals(NUM_CHILDREN, categoryTerms.size());
            assertEquals(NUM_CHILDREN, categoryTerms.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());
        }

        LOG.info("<< testSoftDelete");
    }

    private void testHardDelete() throws Exception {
        LOG.info(">> testHardDelete");
        
        // Create a glossary (parent)
        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_hard_delete" + getRandomName());
        String glossaryGuid = createEntity(glossary).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create terms and categories (children)
        List<String> termGuids = createChildrenTerms(glossaryGuid, NUM_CHILDREN);
        List<String> categoryGuids = createChildrenCategories(glossaryGuid, NUM_CHILDREN);
        sleep(2);

        // Verify initial state
        glossary = getEntity(glossaryGuid);
        assertEquals(ACTIVE, glossary.getStatus());
        List<Map<String, Object>> terms = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_TERMS);
        List<Map<String, Object>> categories = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_CATEGORIES);
        assertNotNull(terms);
        assertEquals(NUM_CHILDREN * 2, terms.size());
        assertNotNull(categories);
        assertEquals(NUM_CHILDREN, categories.size());

        // Delete Glossary with hard delete
        EntityMutationResponse response = deleteEntityHard(glossaryGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(response.getDeletedEntities().size(), response.getDeletedEntities().stream().filter(x -> DELETE_HANDLER_HARD.equals(x.getDeleteHandler().toUpperCase())).count());
        sleep(2);

        // Verify Glossary is not retrievable
        try {
            glossary = getEntity(glossaryGuid);
            fail("Glossary should not be retrievable after hard delete");
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage.contains("|")) {
                errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
            }
            assertEquals("404", errorMessage);
        }

        // Verify all children are not retrievable
        for (String termGuid : termGuids) {
            try {
                AtlasEntity term = getEntity(termGuid);
                fail("Term should not be retrievable after hard delete");
            } catch (Exception e) {
                String errorMessage = e.getMessage();
                if (errorMessage.contains("|")) {
                    errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
                }
                assertEquals("404", errorMessage);
            }
        }
        for (String categoryGuid : categoryGuids) {
            try {
                AtlasEntity category = getEntity(categoryGuid);
                fail("Category should not be retrievable after hard delete");
            } catch (Exception e) {
                String errorMessage = e.getMessage();
                if (errorMessage.contains("|")) {
                    errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
                }
                assertEquals("404", errorMessage);
            }
        }

        LOG.info("<< testHardDelete");
    }

    private void testPurgeDelete() throws Exception {
        LOG.info(">> testPurgeDelete");
        
        // Create a glossary (parent)
        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_purge_delete" + getRandomName());
        String glossaryGuid = createEntity(glossary).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create terms and categories (children)
        List<String> termGuids = createChildrenTerms(glossaryGuid, NUM_CHILDREN);
        List<String> categoryGuids = createChildrenCategories(glossaryGuid, NUM_CHILDREN);
        sleep(2);

        // Verify initial state
        glossary = getEntity(glossaryGuid);
        assertEquals(ACTIVE, glossary.getStatus());
        List<Map<String, Object>> terms = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_TERMS);
        List<Map<String, Object>> categories = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_CATEGORIES);
        assertNotNull(terms);
        assertEquals(NUM_CHILDREN * 2, terms.size());
        assertNotNull(categories);
        assertEquals(NUM_CHILDREN, categories.size());

        // Delete Glossary with purge delete
        EntityMutationResponse response = deleteEntityPurge(glossaryGuid);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        assertEquals(response.getDeletedEntities().size(), response.getDeletedEntities().stream().filter(x -> DELETE_HANDLER_PURGE.equals(x.getDeleteHandler().toUpperCase())).count());
        sleep(2);

        // Verify Glossary is not retrievable
        try {
            glossary = getEntity(glossaryGuid);
            fail("Glossary should not be retrievable after purge delete");
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage.contains("|")) {
                errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
            }
            assertEquals("404", errorMessage);
        }

        // Verify all children are not retrievable
        for (String termGuid : termGuids) {
            try {
                AtlasEntity term = getEntity(termGuid);
                fail("Term should not be retrievable after purge delete");
            } catch (Exception e) {
                String errorMessage = e.getMessage();
                if (errorMessage.contains("|")) {
                    errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
                }
                assertEquals("404", errorMessage);
            }
        }
        for (String categoryGuid : categoryGuids) {
            try {
                AtlasEntity category = getEntity(categoryGuid);
                fail("Category should not be retrievable after purge delete");
            } catch (Exception e) {
                String errorMessage = e.getMessage();
                if (errorMessage.contains("|")) {
                    errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
                }
                assertEquals("404", errorMessage);
            }
        }

        LOG.info("<< testPurgeDelete");
    }

    public static List<String> createChildrenTerms(String glossaryGuid, int numChildren) throws Exception {
        List<AtlasEntity> entitiesToCreate = new ArrayList<>();

        for (int i = 0; i < numChildren; i++) {
            AtlasEntity term = getAtlasEntity(TYPE_TERM, "test_term_" + i + "_" + getRandomName());
            term.setRelationshipAttribute(ANCHOR, getObjectId(glossaryGuid, TYPE_GLOSSARY));
            entitiesToCreate.add(term);
        }

        return createChildren(entitiesToCreate);
    }

    public static List<String> createChildrenCategories(String glossaryGuid, int numChildren) throws Exception {
        List<AtlasEntity> entitiesToCreate = new ArrayList<>();

        List<String> termGuids = createChildrenTerms(glossaryGuid, numChildren);

        for (int i = 0; i < numChildren; i++) {
            AtlasEntity category = getAtlasEntity(TYPE_CATEGORY, "test_category_" + i + "_" + getRandomName());
            category.setRelationshipAttribute(ANCHOR, getObjectId(glossaryGuid, TYPE_GLOSSARY));
            category.setRelationshipAttribute("terms", Collections.singletonList(getObjectId(termGuids.get(i), TYPE_TERM)));
            entitiesToCreate.add(category);
        }

        return createChildren(entitiesToCreate);
    }

    private static List<String> createChildren(List<AtlasEntity> entitiesToCreate) throws Exception {
        List<String> childrenGuids = new ArrayList<>();

        // Create all entities in one request
        EntityMutationResponse response = createEntitiesBulk(entitiesToCreate);
        for (AtlasEntityHeader createdEntity : response.getCreatedEntities()) {
            childrenGuids.add(createdEntity.getGuid());
        }
        sleep(2);

        return childrenGuids;
    }
} 