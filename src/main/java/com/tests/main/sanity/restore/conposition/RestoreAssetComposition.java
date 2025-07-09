package com.tests.main.sanity.restore.conposition;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

import static com.tests.main.utils.TestUtil.*;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RestoreAssetComposition implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(RestoreAssetComposition.class);

    private static long SLEEP = 1000;

    private static int NUM_CHILDREN = 2;

    public static void main(String[] args) throws Exception {
        try {
            new RestoreAssetComposition().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running column delete RestoreAssetComposition tests");

        testRestoreTerm();

        testRestoreGlossary();
    }

    private void testRestoreTerm() throws Exception {
        LOG.info(">> testRestoreTerm");

        // Create a Glossary
        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_delete" + getRandomName());
        String glossaryGuid = createEntity(glossary).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Create term
        List<String> termGuids = createChildrenTerms(glossaryGuid, 1);
        String termGuid = termGuids.get(0);
        sleep(SLEEP);

        // Delete Term
        deleteEntityDefault(termGuid);
        sleep(SLEEP);

        // Restore Term
        AtlasEntity term = getEntity(termGuid);
        term.setStatus(ACTIVE);
        updateEntity(term);
        sleep(SLEEP);

        // Verify Glossary is ACTIVE
        glossary = getEntity(glossaryGuid);
        assertEquals(ACTIVE, glossary.getStatus());
        verifyESAttributes(glossaryGuid, mapOf(ATTR_STATE, ACTIVE.name()));

        // Verify Term status
        term = getEntity(termGuid);
        assertEquals(ACTIVE, term.getStatus());
        verifyESAttributes(termGuid, mapOf(ATTR_STATE, ACTIVE.name()));
        Map<String, Object> anchor = (Map<String, Object>) term.getRelationshipAttribute(REL_ANCHOR);
        assertEquals(ACTIVE.name(), anchor.get("relationshipStatus"));


        LOG.info("<< testRestoreTerm");
    }

    private void testRestoreGlossary() throws Exception {
        LOG.info(">> testRestoreGlossary");

        // Create a Glossary
        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_delete" + getRandomName());
        String glossaryGuid = createEntity(glossary).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Create terms & categories under Glossary
        List<String> termGuids = createChildrenTerms(glossaryGuid, NUM_CHILDREN);
        List<String> categoryGuids = createChildrenCategories(glossaryGuid, NUM_CHILDREN);
        sleep(SLEEP);

        LOG.info("termGuids: {}", termGuids.toString());

        // Delete Glossary
        deleteEntityDefault(glossaryGuid);
        sleep(SLEEP);

        // Restore Glossary
        glossary = getEntity(glossaryGuid);
        glossary.setStatus(ACTIVE);
        glossary.removeAttribute("lexicographicalSortOrder"); // had to do this due unexpected duplicate lexo value issue
        EntityMutationResponse restoreResponse = updateEntity(glossary);
        assertNotNull(restoreResponse);
        assertTrue(restoreResponse.getUpdatedEntities().size() == 6);
        sleep(SLEEP);

        // Verify Glossary is ACTIVE
        glossary = getEntity(glossaryGuid);
        assertEquals(ACTIVE, glossary.getStatus());
        verifyESAttributes(glossaryGuid, mapOf(ATTR_STATE, ACTIVE.name()));


        // Verify children status
        List<Map<String, Object>> terms = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_TERMS);
        List<Map<String, Object>> categories = (List<Map<String, Object>>) glossary.getRelationshipAttribute(REL_CATEGORIES);
        assertNotNull(terms);
        assertEquals(NUM_CHILDREN * 2, terms.size());
        assertNotNull(categories);
        assertEquals(NUM_CHILDREN, categories.size());

        for (String termGuid : termGuids) {
            AtlasEntity term = getEntity(termGuid);
            assertEquals(ACTIVE, term.getStatus());
            verifyESAttributes(termGuid, mapOf(ATTR_STATE, ACTIVE.name()));
            List<Map<String, Object>> termCategories = (List<Map<String, Object>>) term.getRelationshipAttribute(REL_CATEGORIES);
            assertTrue(termCategories.isEmpty());

            Map<String, Object> anchor = (Map<String, Object>) term.getRelationshipAttribute(REL_ANCHOR);
            assertEquals(ACTIVE.name(), anchor.get("relationshipStatus"));
        }
        for (String categoryGuid : categoryGuids) {
            AtlasEntity category = getEntity(categoryGuid);
            assertEquals(ACTIVE, category.getStatus());
            verifyESAttributes(categoryGuid, mapOf(ATTR_STATE, ACTIVE.name()));
            List<Map<String, Object>> categoryTerms = (List<Map<String, Object>>) category.getRelationshipAttribute(REL_TERMS);
            assertNotNull(categoryTerms);
            assertEquals(1, categoryTerms.size());
            assertEquals(1, categoryTerms.stream().filter(x -> ACTIVE.name().equals(x.get("relationshipStatus"))).count());

            Map<String, Object> anchor = (Map<String, Object>) category.getRelationshipAttribute(REL_ANCHOR);
            assertEquals(ACTIVE.name(), anchor.get("relationshipStatus"));
        }

        LOG.info("<< testRestoreGlossary");
    }
}
