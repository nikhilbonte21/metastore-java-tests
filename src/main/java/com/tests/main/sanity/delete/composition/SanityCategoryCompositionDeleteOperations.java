package com.tests.main.sanity.delete.composition;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.tests.main.sanity.delete.composition.SanityCompositionDeleteOperations.createChildrenCategories;
import static com.tests.main.utils.TestUtil.ANCHOR;
import static com.tests.main.utils.TestUtil.REL_CATEGORIES;
import static com.tests.main.utils.TestUtil.REL_TERMS;
import static com.tests.main.utils.TestUtil.TYPE_CATEGORY;
import static com.tests.main.utils.TestUtil.TYPE_GLOSSARY;
import static com.tests.main.utils.TestUtil.TYPE_TERM;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.deleteEntity;
import static com.tests.main.utils.TestUtil.deleteEntityDefault;
import static com.tests.main.utils.TestUtil.deleteEntityHard;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getObjectId;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.sleep;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests different delete operations on categories and their impact on associated terms.
 * 
 * This class verifies the behavior of category deletion operations:
 * 1. Default Delete: Category is forcefully HARD deleted, terms move up to glossary level
 * 2. Soft Delete: Category is forcefully HARD deleted, terms move up to glossary level
 * 3. Hard Delete: Category is HARD deleted, terms move up to glossary level
 * 4. Purge Delete: Category is PURGE deleted, terms move up to glossary level
 * 
 * Key behaviors:
 * - Categories are always HARD deleted regardless of delete type
 * - Terms under deleted category move up to glossary level
 * - Terms maintain their relationships with other categories
 */
public class SanityCategoryCompositionDeleteOperations implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(SanityCategoryCompositionDeleteOperations.class);
    private static final String DELETE_HANDLER_DEFAULT = "DEFAULT";
    private static final String DELETE_HANDLER_SOFT = "SOFT";
    private static final String DELETE_HANDLER_HARD = "HARD";
    private static final String DELETE_HANDLER_PURGE = "PURGE";

    private static int NUM_CHILDREN = 2;

    public static void main(String[] args) throws Exception {
        try {
            new SanityCategoryCompositionDeleteOperations().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running category composition delete operations tests");

        testDelete(DELETE_HANDLER_DEFAULT);
        testDelete(DELETE_HANDLER_SOFT);
        testDelete(DELETE_HANDLER_HARD);
        testDelete(DELETE_HANDLER_PURGE);

    }

    private void testDelete(String deleteType) throws Exception {
        LOG.info(">> testDefaultDelete");
        
        // Create a glossary (parent)
        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_category_delete" + getRandomName());
        String glossaryGuid = createEntity(glossary).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create categories with terms
        List<String> categoryGuids = createChildrenCategories(glossaryGuid, NUM_CHILDREN);
        sleep(2);

        // Verify initial state
        glossary = getEntity(glossaryGuid);
        assertEquals(ACTIVE, glossary.getStatus());
        
        // Verify glossary has all categories
        List<Map<String, Object>> categories = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_CATEGORIES);
        assertNotNull(categories);
        assertEquals(NUM_CHILDREN, categories.size());

        List<String> nestedTermGuids = new ArrayList<>();
        
        // Verify each category has its terms
        for (String categoryGuid : categoryGuids) {
            AtlasEntity category = getEntity(categoryGuid);
            assertEquals(ACTIVE, category.getStatus());
            List<Map<String, Object>> categoryTerms = (List<Map<String, Object>>) category.getRelationshipAttribute(REL_TERMS);
            assertNotNull(categoryTerms);
            assertEquals(1, categoryTerms.size());
            assertEquals(1, categoryTerms.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());
            nestedTermGuids.add((String) categoryTerms.get(0).get("guid"));
        }

        // Delete first category with default delete
        String categoryToDelete = categoryGuids.get(0);
        EntityMutationResponse response = deleteEntity(categoryToDelete, deleteType);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);

        // Category should be HARD deleted even with DEFAULT delete
        assertEquals(1, response.getDeletedEntities().size());
        if (DELETE_HANDLER_PURGE.equals(deleteType)) {
            assertEquals(1, response.getDeletedEntities().stream().filter(x -> DELETE_HANDLER_PURGE.equals(x.getDeleteHandler().toUpperCase())).count());
        } else {
            assertEquals(1, response.getDeletedEntities().stream().filter(x -> DELETE_HANDLER_HARD.equals(x.getDeleteHandler().toUpperCase())).count());
        }
        sleep(2);

        // Verify category is not retrievable (HARD deleted)
        try {
            AtlasEntity category = getEntity(categoryToDelete);
            fail("Category should not be retrievable after default delete (force HARD delete)");
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage.contains("|")) {
                errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
            }
            assertEquals("404", errorMessage);
        }

        // Verify glossary still has remaining category
        glossary = getEntity(glossaryGuid);
        categories = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_CATEGORIES);
        assertNotNull(categories);
        assertEquals(NUM_CHILDREN - 1, categories.size());

        // Verify terms from deleted category moved up to glossary level
        List<Map<String, Object>> glossaryTerms = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_TERMS);
        assertNotNull(glossaryTerms);
        // Should have terms from both categories (original + moved up)
        assertEquals(NUM_CHILDREN, glossaryTerms.size());
        assertEquals(NUM_CHILDREN, glossaryTerms.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        for (int i = 0; i < nestedTermGuids.size(); i++) {
            AtlasEntity term = getEntity(nestedTermGuids.get(i));
            assertEquals(ACTIVE, term.getStatus());
            List<Map<String, Object>> termCategories = (List<Map<String, Object>>) term.getRelationshipAttribute(REL_CATEGORIES);
            if (i == 0) {
                assertTrue(termCategories.isEmpty());
            } else {
                assertEquals(1, termCategories.size());
                assertEquals("ACTIVE", termCategories.get(0).get("relationshipStatus"));
            }
        }

        // Verify remaining category is unaffected
        for (int i = 1; i < categoryGuids.size(); i++) {
            String remainingCategory = categoryGuids.get(i);
            AtlasEntity category = getEntity(remainingCategory);
            assertEquals(ACTIVE, category.getStatus());
            List<Map<String, Object>> categoryTerms = (List<Map<String, Object>>) category.getRelationshipAttribute(REL_TERMS);
            assertNotNull(categoryTerms);
            assertEquals(1, categoryTerms.size());
            assertEquals(1, categoryTerms.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());
        }


        LOG.info("<< testDefaultDelete");
    }

    private void testHARDDelete() throws Exception {
        LOG.info(">> testDefaultDelete");

        // Create a glossary (parent)
        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_category_delete" + getRandomName());
        String glossaryGuid = createEntity(glossary).getCreatedEntities().get(0).getGuid();
        sleep(2);

        // Create categories with terms
        List<String> categoryGuids = createChildrenCategories(glossaryGuid, NUM_CHILDREN);
        sleep(2);

        // Verify initial state
        glossary = getEntity(glossaryGuid);
        assertEquals(ACTIVE, glossary.getStatus());

        // Verify glossary has all categories
        List<Map<String, Object>> categories = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_CATEGORIES);
        assertNotNull(categories);
        assertEquals(NUM_CHILDREN, categories.size());

        List<String> nestedTermGuids = new ArrayList<>();

        // Verify each category has its terms
        for (String categoryGuid : categoryGuids) {
            AtlasEntity category = getEntity(categoryGuid);
            assertEquals(ACTIVE, category.getStatus());
            List<Map<String, Object>> categoryTerms = (List<Map<String, Object>>) category.getRelationshipAttribute(REL_TERMS);
            assertNotNull(categoryTerms);
            assertEquals(1, categoryTerms.size());
            assertEquals(1, categoryTerms.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());
            nestedTermGuids.add((String) categoryTerms.get(0).get("guid"));
        }

        // Delete first category with default delete
        String categoryToDelete = categoryGuids.get(0);
        EntityMutationResponse response = deleteEntityHard(categoryToDelete);
        assertNotNull(response);
        assertTrue(response.getDeletedEntities().size() > 0);
        // Category should be HARD deleted even with DEFAULT delete
        assertEquals(1, response.getDeletedEntities().size());
        assertEquals(1, response.getDeletedEntities().stream().filter(x -> DELETE_HANDLER_HARD.equals(x.getDeleteHandler().toUpperCase())).count());
        sleep(2);

        // Verify category is not retrievable (HARD deleted)
        try {
            AtlasEntity category = getEntity(categoryToDelete);
            fail("Category should not be retrievable after default delete (force HARD delete)");
        } catch (Exception e) {
            String errorMessage = e.getMessage();
            if (errorMessage.contains("|")) {
                errorMessage = errorMessage.substring(0, errorMessage.indexOf("|")).trim();
            }
            assertEquals("404", errorMessage);
        }

        // Verify glossary still has remaining category
        glossary = getEntity(glossaryGuid);
        categories = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_CATEGORIES);
        assertNotNull(categories);
        assertEquals(NUM_CHILDREN - 1, categories.size());

        // Verify terms from deleted category moved up to glossary level
        List<Map<String, Object>> glossaryTerms = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_TERMS);
        assertNotNull(glossaryTerms);
        // Should have terms from both categories (original + moved up)
        assertEquals(NUM_CHILDREN, glossaryTerms.size());
        assertEquals(NUM_CHILDREN, glossaryTerms.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        for (int i = 0; i < nestedTermGuids.size(); i++) {
            AtlasEntity term = getEntity(nestedTermGuids.get(i));
            assertEquals(ACTIVE, term.getStatus());
            List<Map<String, Object>> termCategories = (List<Map<String, Object>>) term.getRelationshipAttribute(REL_CATEGORIES);
            if (i == 0) {
                assertTrue(termCategories.isEmpty());
            } else {
                assertEquals(1, termCategories.size());
                assertEquals("ACTIVE", termCategories.get(0).get("relationshipStatus"));
            }
        }

        // Verify remaining category is unaffected
        for (int i = 1; i < categoryGuids.size(); i++) {
            String remainingCategory = categoryGuids.get(i);
            AtlasEntity category = getEntity(remainingCategory);
            assertEquals(ACTIVE, category.getStatus());
            List<Map<String, Object>> categoryTerms = (List<Map<String, Object>>) category.getRelationshipAttribute(REL_TERMS);
            assertNotNull(categoryTerms);
            assertEquals(1, categoryTerms.size());
            assertEquals(1, categoryTerms.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());
        }


        LOG.info("<< testDefaultDelete");
    }

    private List<String> createTermsForCategory(String glossaryGuid, int numTerms) throws Exception {
        List<AtlasEntity> entitiesToCreate = new ArrayList<>();
        List<String> termGuids = new ArrayList<>();

        for (int i = 0; i < numTerms; i++) {
            AtlasEntity term = getAtlasEntity(TYPE_TERM, "test_term_" + i + "_" + getRandomName());
            term.setRelationshipAttribute(ANCHOR, getObjectId(glossaryGuid, TYPE_GLOSSARY));
            entitiesToCreate.add(term);
        }

        EntityMutationResponse response = createEntitiesBulk(entitiesToCreate);
        for (AtlasEntityHeader createdEntity : response.getCreatedEntities()) {
            termGuids.add(createdEntity.getGuid());
        }
        sleep(2);

        return termGuids;
    }
} 