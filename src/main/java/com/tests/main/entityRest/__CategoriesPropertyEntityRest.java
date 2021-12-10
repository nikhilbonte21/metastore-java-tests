package com.tests.main.entityRest;

import com.tests.main.glossary.tests.TestsMain;
import com.tests.main.glossary.utils.ESUtils;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.tests.main.glossary.utils.TestUtils.*;
import static org.junit.Assert.*;

public class __CategoriesPropertyEntityRest implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(__CategoriesPropertyEntityRest.class);

    public static void main(String[] args) throws Exception {
        try {
            new __CategoriesPropertyEntityRest().run();
        } finally {
            cleanUpAll();
            ESUtils.close();
        }
    }

    @Override
    public void run() throws  Exception{
        LOG.info("Running GlossaryANDCategoriesProperties tests");

        long start = System.currentTimeMillis();
        try {
            //testCreateOrAddTerm();
            testCreateOrAddCategory();

            //TODO: testCreateOrUpdateMeaningsInCategory(); //Entity rest

        } catch (Exception e){
            throw e;
        } finally {
            LOG.info("Completed running GlossaryANDCategoriesProperties tests, took {} seconds", (System.currentTimeMillis() - start) / 1000 );
        }
    }

    private static void testCreateOrAddTerm() throws Exception {
        LOG.info(">> testCreateOrAddTerm");

        AtlasEntity glossary_0 = createGlossary("glossary_0");

        AtlasEntity term_0 = create("term_0", glossary_0.getGuid());

        AtlasEntity c_0 = createCategory("cat_0", glossary_0.getGuid());
        AtlasEntity c_1 = createCategory("cat_1", glossary_0.getGuid());
        AtlasEntity c_2 = createCategory("cat_2", glossary_0.getGuid());

        //create new Term with 3 categories;
        AtlasEntity term_1 = getAtlasEntity(TYPE_TERM, "term_1_c").getEntity();
        setAnchor(term_1, glossary_0.getGuid());
        setCategories(term_1, c_0.getGuid(), c_1.getGuid(), c_2.getGuid());
        term_1 = create(term_1);

        Thread.sleep(1500);
        term_1 = getEntity(term_1.getGuid());
        assertEquals(getCategoriesRelationshipAttribute(term_1).size(), 3);
        assertESHits(term_1.getGuid(), getQualifiedName(glossary_0), getQualifiedName(c_0), getQualifiedName(c_1), getQualifiedName(c_2));

        //update existing Term with 3 categories;
        term_0 = getEntity(term_0.getGuid());
        setCategories(term_0, c_0.getGuid(), c_1.getGuid(), c_2.getGuid());
        create(term_0);

        Thread.sleep(1500);
        term_0 = getEntity(term_0.getGuid());
        assertEquals(getCategoriesRelationshipAttribute(term_0).size(), 3);
        assertESHits(term_0.getGuid(), getQualifiedName(glossary_0), getQualifiedName(c_0), getQualifiedName(c_1), getQualifiedName(c_2));

        //update existing Term with add 2 more categories;
        AtlasEntity c_3 = createCategory("cat_3", glossary_0.getGuid());
        AtlasEntity c_4 = createCategory("cat_4", glossary_0.getGuid());

        Thread.sleep(1500);
        term_0 = getEntity(term_0.getGuid());
        setCategories(term_0, c_0.getGuid(), c_1.getGuid(), c_2.getGuid(), c_3.getGuid(), c_4.getGuid());
        create(term_0);

        Thread.sleep(1500);
        term_0 = getEntity(term_0.getGuid());
        assertEquals(getCategoriesRelationshipAttribute(term_0).size(), 5);
        assertESHits(term_0.getGuid(), getQualifiedName(glossary_0), getQualifiedName(c_0), getQualifiedName(c_1), getQualifiedName(c_2),
                                                                        getQualifiedName(c_3), getQualifiedName(c_4));

        //update existing Term with remove 3 categories;
        setCategories(term_0, c_3.getGuid(), c_4.getGuid());
        create(term_0);

        Thread.sleep(2000);
        term_0 = getEntity(term_0.getGuid());
        List<Map> cats = getCategoriesRelationshipAttribute(term_0);
        assertEquals(cats.size(), 5);
        int activeCount = cats.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size();
        assertEquals(2, activeCount);
        assertESHits(term_0.getGuid(), getQualifiedName(glossary_0), getQualifiedName(c_3), getQualifiedName(c_4));


        //remove all terms from cat
        term_0.setRelationshipAttribute("categories", new ArrayList<>());
        create(term_0);

        Thread.sleep(2000);
        term_0 = getEntity(term_0.getGuid());
        cats = getCategoriesRelationshipAttribute(term_0);
        assertEquals(cats.size(), 5);
        activeCount = cats.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size();
        assertEquals(0, activeCount);
        assertESHits(term_0.getGuid(), getQualifiedName(glossary_0));

        LOG.info("<< testCreateOrAddTerm");
    }

    private static void testCreateOrAddCategory() throws Exception {
        LOG.info(">> testCreateOrAddCategory");

        AtlasEntity glossary_0 = createGlossary("glossary_0");

        AtlasEntity t_0 = create("term_0", glossary_0.getGuid());
        AtlasEntity t_1 = create("term_1", glossary_0.getGuid());
        AtlasEntity t_2 = create("term_2", glossary_0.getGuid());


        //create new Category with 3 categories;
        AtlasEntity category_0_c = createCategory("cat_0", glossary_0.getGuid());
        setAnchor(category_0_c, glossary_0.getGuid());
        setTerms(category_0_c, t_0.getGuid(), t_1.getGuid(), t_2.getGuid());
        create(category_0_c);

        Thread.sleep(1500);
        category_0_c = getEntity(category_0_c.getGuid());
        assertEquals(getTermsRelationshipAttribute(category_0_c).size(), 3);
        assertESHits(getQualifiedName(glossary_0), Arrays.asList(t_0.getGuid(), t_1.getGuid(), t_2.getGuid()), getQualifiedName(category_0_c));


        //create new Category & add previous terms to it also
        AtlasEntity category_1 = createCategory("cat_1", glossary_0.getGuid());
        setAnchor(category_1, glossary_0.getGuid());
        setTerms(category_1, t_0.getGuid(), t_1.getGuid(), t_2.getGuid());
        create(category_1);

        Thread.sleep(2000);
        category_1 = getEntity(category_1.getGuid());
        assertEquals(getTermsRelationshipAttribute(category_1).size(), 3);
        assertESHits(getQualifiedName(glossary_0), Arrays.asList(t_0.getGuid(), t_1.getGuid(), t_2.getGuid()),
                getQualifiedName(category_1), getQualifiedName(category_0_c));



        //create new Category & add previous terms to it also
        AtlasEntity category_2_c = createCategory("cat_2", glossary_0.getGuid());
        setAnchor(category_2_c, glossary_0.getGuid());
        setTerms(category_2_c, t_0.getGuid(), t_1.getGuid(), t_2.getGuid());
        create(category_2_c);

        Thread.sleep(2000);
        category_2_c = getEntity(category_2_c.getGuid());
        assertEquals(getTermsRelationshipAttribute(category_2_c).size(), 3);
        assertESHits(getQualifiedName(glossary_0), Arrays.asList(t_0.getGuid(), t_1.getGuid(), t_2.getGuid()), getQualifiedName(category_1), getQualifiedName(category_0_c), getQualifiedName(category_2_c));


        AtlasEntity t_3 = create("term_3", glossary_0.getGuid());
        AtlasEntity t_4 = create("term_4", glossary_0.getGuid());


        //Update existing Category & add 2 new terms & remove existing terms
        setTerms(category_2_c, t_4.getGuid(), t_3.getGuid());
        create(category_2_c);

        Thread.sleep(2000);
        category_2_c = getEntity(category_2_c.getGuid());
        List<Map> terms = getTermsRelationshipAttribute(category_2_c);
        assertEquals(terms.size(), 5);
        int activeCount = terms.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size();
        assertEquals(2, activeCount);


        assertESHits(getQualifiedName(glossary_0), Arrays.asList(t_0.getGuid(), t_1.getGuid(), t_2.getGuid()), getQualifiedName(category_0_c), getQualifiedName(category_1));
        assertESHits(getQualifiedName(glossary_0), Arrays.asList(t_3.getGuid(), t_4.getGuid()), getQualifiedName(category_2_c));


        //Update existing Category & add remove existing 2 terms
        category_2_c.setRelationshipAttribute("terms", new ArrayList<>());
        create(category_2_c);

        Thread.sleep(2000);
        category_2_c = getEntity(category_2_c.getGuid());
        assertNotNull(getTermsRelationshipAttribute(category_2_c));
        terms = getTermsRelationshipAttribute(category_2_c);
        assertEquals(terms.size(), 5);
        activeCount = terms.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size();
        assertEquals(0, activeCount);

        assertESHits(getQualifiedName(glossary_0), Arrays.asList(t_0.getGuid(), t_1.getGuid(), t_2.getGuid()), getQualifiedName(category_0_c), getQualifiedName(category_1));

        assertESHits(getQualifiedName(glossary_0), Arrays.asList(t_3.getGuid(), t_4.getGuid()));

        LOG.info("<< testCreateOrAddCategory");
    }

    private static void assertTermCatProperty(String termGuid, String... expectedCatQNames) {
        assertTermCatProperty(termGuid, true, expectedCatQNames);
    }

    private static void assertTermCatProperty(String termGuid, boolean shouldPresent, String... expectedCatQNames) {
        SearchHit[] searchHit = ESUtils.searchWithGuid(termGuid).getHits().getHits();
        assertNotNull(searchHit);
        assertTrue(searchHit.length > 0);

        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            List<String> actualCategories = (List<String>) sourceAsMap.get("__categories");
            assertNotNull(actualCategories);

            if (shouldPresent) {
                assertEquals(actualCategories.size(), expectedCatQNames.length);
                Arrays.stream(expectedCatQNames).forEach(expected -> assertTrue(actualCategories.contains(expected)));

            } else {
                Arrays.stream(expectedCatQNames).forEach(expected -> assertFalse(actualCategories.contains(expected)));
            }
        }
    }

    private static void assertESHits(String termGuid, String glossaryQn, String... catQualifedNames) {
        SearchHit[] searchHit = ESUtils.searchWithGuid(termGuid).getHits().getHits();
        assertNotNull(searchHit);
        assertTrue(searchHit.length > 0);

        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            assertEquals(glossaryQn, sourceAsMap.get("__glossary"));

            List<String> actualCategories = (List<String>) sourceAsMap.get("__categories");

            if (catQualifedNames == null || catQualifedNames.length == 0){
                assertNotNull(actualCategories);
                assertEquals(0, actualCategories.size());
            } else {
                assertNotNull(actualCategories);
                assertEquals(catQualifedNames.length, actualCategories.size());

                Arrays.stream(catQualifedNames).forEach(ac -> assertTrue(actualCategories.contains(ac)));
            }

        }
    }

    private static void assertESHits(String glossaryQn, List<String> termGuids, String... expectedQNames) {
        for (String termGuid : termGuids) {
            SearchHit[] searchHit = ESUtils.searchWithGuid(termGuid).getHits().getHits();

            assertNotNull(searchHit);
            assertTrue(searchHit.length > 0);
            for (SearchHit hit : searchHit) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                assertEquals(sourceAsMap.get("__glossary"), glossaryQn);

                List<String> actualCategories = (List<String>) sourceAsMap.get("__categories");
                assertNotNull(actualCategories);
                assertEquals(expectedQNames.length, actualCategories.size());

                Arrays.stream(expectedQNames).forEach(ac -> assertTrue(actualCategories.contains(ac)));
            }
        }
    }

    private static void verifyGlossaryProperty(String guid, String gloQname){

        SearchHit[] searchHit = ESUtils.searchWithGuid(guid).getHits().getHits();
        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            assertEquals(  sourceAsMap.get("__glossary"), gloQname);
            assertNull(sourceAsMap.get("__categories"));
        }
    }

    private static AtlasEntity createGlossary(String name) throws Exception {
        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, name).getEntity();

        EntityMutationResponse reps = createEntity(glossary);
        return getEntity(reps.getCreatedEntities().get(0).getGuid());
    }

    private static AtlasEntity create(AtlasEntity entity) throws Exception {
        EntityMutationResponse reps = createEntity(entity);

        if (CollectionUtils.isNotEmpty(reps.getCreatedEntities())) {
            return getEntity(reps.getCreatedEntities().get(0).getGuid());
        } else {
            return getEntity(reps.getUpdatedEntities().get(0).getGuid());
        }
    }


    private static AtlasEntity create(String name, String gloGuid) throws Exception {
        AtlasEntity term = getAtlasEntity(TYPE_TERM, name).getEntity();
        term.setRelationshipAttribute("anchor", getObjectId(gloGuid, TYPE_GLOSSARY));

        EntityMutationResponse reps = createEntity(term);

        return getEntity(reps.getCreatedEntities().get(0).getGuid());
    }

    private static AtlasEntity createCategory(String name, String gloGuid) throws Exception {
        AtlasEntity term = getAtlasEntity(TYPE_CATEGORY, name).getEntity();
        term.setRelationshipAttribute("anchor", getObjectId(gloGuid, TYPE_GLOSSARY));

        EntityMutationResponse reps = createEntity(term);

        return getEntity(reps.getCreatedEntities().get(0).getGuid());
    }

    private static AtlasEntity setAnchor(AtlasEntity termEntity, String anchorGuid) {
        termEntity.setRelationshipAttribute("anchor", getObjectId(anchorGuid, TYPE_GLOSSARY));
        return termEntity;
    }

    private static AtlasEntity setCategories(AtlasEntity termEntity, String... categoryGuids) {
        termEntity.setRelationshipAttribute("categories", getObjectIdsAsList(TYPE_CATEGORY, categoryGuids));
        return termEntity;
    }


    private static AtlasEntity setTerms(AtlasEntity catEntity, String... termGuids) {
        catEntity.setRelationshipAttribute("terms", getObjectIdsAsList(TYPE_TERM, termGuids));
        return catEntity;
    }

}

