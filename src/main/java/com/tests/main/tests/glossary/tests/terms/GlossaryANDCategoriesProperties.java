package com.tests.main.tests.glossary.tests.terms;

import com.tests.main.tests.glossary.models.AtlasGlossaryTerm;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static com.tests.main.utils.TestUtil.*;

@Deprecated
public class GlossaryANDCategoriesProperties {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryANDCategoriesProperties.class);

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws  Exception{
        LOG.info("Running GlossaryANDCategoriesProperties tests");

        long start = System.currentTimeMillis();
        try {
            testCreateOrAddTerm(); //Glossary rest
            testCreateOrAddCategory(); //Glossary rest

            //testCreateOrAddCategoryEntityRest(); //Entity Rest
            //TODO: testCreateOrUpdateMeaningsInCategory(); //Entity rest
            //TODO: testCreateOrAddTermEntityRest(); //Entity Rest

        } catch (Exception e){
            throw e;
        } finally {
            Thread.sleep(2000);
            ESUtil.close();
            cleanUpAll();
            LOG.info("Completed running GlossaryANDCategoriesProperties tests, took {} seconds", (System.currentTimeMillis() - start) / 1000 );
        }
    }

    private static void testCreateOrAddTerm() throws Exception {
        LOG.info(">> testCreateOrAddTerm");

        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel());
        AtlasGlossaryTerm term_0 = createTerm(getTermModel(glossary_0.getGuid()));

        SearchHit[] searchHit = ESUtil.searchWithName(term_0.getName()).getHits().getHits();
        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            assertEquals(  sourceAsMap.get("__glossary"), glossary_0.getQualifiedName());
            assertNull(sourceAsMap.get("__categories"));
        }

        CatNCatTermHeader catHeaderMap = getCatHeader(3, glossary_0.getGuid());

        //create new Term with 3 categories;
        AtlasGlossaryTerm term_1_c = getTermModel(glossary_0.getGuid());
        term_1_c.setCategories(catHeaderMap.getHeaders());
        AtlasGlossaryTerm term_1 = createTerm(term_1_c);

        Thread.sleep(2000);
        assertEquals(term_1.getCategories().size(), 3);
        searchHit = ESUtil.searchWithName(term_1.getName()).getHits().getHits();
        assertESHits(searchHit, glossary_0.getQualifiedName(), catHeaderMap);

        //update existing Term with 3 categories;
        term_0.setCategories(catHeaderMap.getHeaders());
        AtlasGlossaryTerm updatedTerm = updateTerm(term_0.getGuid(), term_0);

        Thread.sleep(2000);
        assertEquals(updatedTerm.getCategories().size(), 3);
        searchHit = ESUtil.searchWithName(term_0.getName()).getHits().getHits();
        assertESHits(searchHit, glossary_0.getQualifiedName(), catHeaderMap);

        //update existing Term with add 2 more categories;
        CatNCatTermHeader catHeaderMap_1 = getCatHeader(2, glossary_0.getGuid());
        term_0.getCategories().addAll(catHeaderMap_1.getHeaders());
        updatedTerm = updateTerm(term_0.getGuid(), term_0);

        Thread.sleep(2000);
        assertEquals(updatedTerm.getCategories().size(), 5);
        searchHit = ESUtil.searchWithName(term_0.getName()).getHits().getHits();
        assertESHits(searchHit, glossary_0.getQualifiedName(), catHeaderMap.addAll(catHeaderMap_1));

        //update existing Term with remove 3 categories;
        term_0.setCategories(catHeaderMap_1.getHeaders());
        updatedTerm = updateTerm(term_0.getGuid(), term_0);

        Thread.sleep(2000);
        assertEquals(updatedTerm.getCategories().size(), 2);
        searchHit = ESUtil.searchWithName(term_0.getName()).getHits().getHits();
        assertESHits(searchHit, glossary_0.getQualifiedName(), catHeaderMap_1);


        //remove all terms from cat
        term_0.setCategories(new HashSet<>());
        updatedTerm = updateTerm(term_0.getGuid(), term_0);

        Thread.sleep(2000);
        assertNull(updatedTerm.getCategories());
        searchHit = ESUtil.searchWithName(term_0.getName()).getHits().getHits();
        assertESHits(searchHit, glossary_0.getQualifiedName(), null);

        LOG.info("<< testCreateOrAddTerm");
    }

    private static void testCreateOrUpdateMeaningsInTerm() throws Exception {
        LOG.info(">> testCreateOrUpdateMeaningsInTerm");



        LOG.info("<< testCreateOrUpdateMeaningsInTerm");
    }

    private static void testCreateOrAddCategory() throws Exception {
        LOG.info(">> testCreateOrAddCategory");

        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel());

        TermNTermHeader termHeaderMap = getTermHeader(3, glossary_0.getGuid());

        //create new Category with 3 categories;
        AtlasGlossaryCategory category_0_c = getCategoryModel(glossary_0.getGuid());
        category_0_c.setTerms(termHeaderMap.getHeaders());
        AtlasGlossaryCategory category_0 = createCategory(category_0_c);

        Thread.sleep(2000);
        assertEquals(category_0.getTerms().size(), 3);
        assertESHits(glossary_0.getQualifiedName(), termHeaderMap, category_0.getQualifiedName());


        //create new Category & add previous terms to it also
        AtlasGlossaryCategory category_1_c = getCategoryModel(glossary_0.getGuid());
        category_1_c.setTerms(termHeaderMap.getHeaders());
        AtlasGlossaryCategory category_1 = createCategory(category_1_c);

        Thread.sleep(2000);
        assertEquals(category_1.getTerms().size(), 3);
        assertESHits(glossary_0.getQualifiedName(), termHeaderMap, category_0.getQualifiedName(), category_1.getQualifiedName());



        //create new Category & add previous terms to it also
        AtlasGlossaryCategory category_2_c = getCategoryModel(glossary_0.getGuid());
        category_2_c.setTerms(termHeaderMap.getHeaders());
        AtlasGlossaryCategory category2 = createCategory(category_2_c);

        Thread.sleep(2000);
        assertEquals(category2.getTerms().size(), 3);
        assertESHits(glossary_0.getQualifiedName(), termHeaderMap, category_0.getQualifiedName(), category_1.getQualifiedName(), category2.getQualifiedName());


        TermNTermHeader termHeaderMap_1 = getTermHeader(2, glossary_0.getGuid());
        //Update existing Category & add 2 new terms & remove existing terms
        category2.setTerms(termHeaderMap_1.getHeaders());
        updateCategory(category2.getGuid(), category2);

        Thread.sleep(2000);
        assertEquals(category2.getTerms().size(), 2);
        assertESHits(glossary_0.getQualifiedName(), termHeaderMap, category_0.getQualifiedName(), category_1.getQualifiedName());
        assertESHits(glossary_0.getQualifiedName(), termHeaderMap_1, category2.getQualifiedName());


        //Update existing Category & add remove existing 2 terms
        category2.setTerms(null);
        updateCategory(category2.getGuid(), category2);

        Thread.sleep(2000);
        assertNull(category2.getTerms());
        assertESHits(glossary_0.getQualifiedName(), termHeaderMap, category_0.getQualifiedName(), category_1.getQualifiedName());
        assertESHits(glossary_0.getQualifiedName(), termHeaderMap_1);

        LOG.info("<< testCreateOrAddCategory");
    }

    private static void testCreateOrAddCategoryEntityRest() throws Exception {
        LOG.info(">> testCreateOrAddCategoryEntityRest");

        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel());
        String gloGUID = glossary_0.getGuid();

        AtlasGlossaryTerm term_0 = createTerm(getTermModel(gloGUID));
        AtlasGlossaryTerm term_1 = createTerm(getTermModel(gloGUID));
        AtlasGlossaryTerm term_2 = createTerm(getTermModel(gloGUID));

        AtlasEntity entity = new AtlasEntity(TYPE_CATEGORY);
        String name = getRandomName();
        entity.setAttribute(NAME, name);
        entity.setAttribute(QUALIFIED_NAME, name);

        entity.setRelationshipAttribute("anchor", getAnchorRelatedObject(gloGUID));

        List<AtlasRelatedObjectId> terms = new ArrayList<>();
        terms.add(getTermRelatedObject(term_0.getGuid()));
        terms.add(getTermRelatedObject(term_1.getGuid()));
        terms.add(getTermRelatedObject(term_2.getGuid()));

        entity.setRelationshipAttribute("terms", terms);
        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        entityWithExtInfo.setEntity(entity);
        EntityMutationResponse response = createEntity(entityWithExtInfo);
        assertNotNull(response.getCreatedEntities());
        assertTrue(response.getCreatedEntities().size() > 0);

        Thread.sleep(2000);
        AtlasEntity entity1 = getEntity(response.getCreatedEntities().get(0).getGuid());
        String categoryQName = entity1.getAttribute(QUALIFIED_NAME).toString();
        LOG.info("categoryQName {}", categoryQName);

        assertTermCatProperty(term_0, categoryQName);
        assertTermCatProperty(term_1, categoryQName);
        assertTermCatProperty(term_2, categoryQName);

        // add 2 more terms into category entity
        List<AtlasRelatedObjectId> terms_1 = new ArrayList<>();
        AtlasGlossaryTerm term_3 = createTerm(getTermModel(gloGUID));
        AtlasGlossaryTerm term_4 = createTerm(getTermModel(gloGUID));
        terms_1.add(getTermRelatedObject(term_3.getGuid()));
        terms_1.add(getTermRelatedObject(term_4.getGuid()));
        terms.addAll(terms_1);

        entity.setRelationshipAttribute("terms", terms);
        entityWithExtInfo.setEntity(entity);

        response = createEntity(entityWithExtInfo);

        Thread.sleep(2000);
        assertTermCatProperty(term_0, categoryQName);
        assertTermCatProperty(term_1, categoryQName);
        assertTermCatProperty(term_2, categoryQName);
        assertTermCatProperty(term_3, categoryQName);
        assertTermCatProperty(term_4, categoryQName);

        getCategory(entity1.getGuid()).getTerms().forEach(x -> LOG.info(x.getRelationGuid() + "  " + x.getDisplayText() + "  " + x.getStatus()) );

        // remove 3 more terms from category entity
        entity.setRelationshipAttribute("terms", terms_1);
        entityWithExtInfo.setEntity(entity);

        response = createEntity(entityWithExtInfo);

        Thread.sleep(2000);
        getCategory(entity1.getGuid()).getTerms().forEach(x -> LOG.info(x.getRelationGuid() + "  " + x.getDisplayText() + "  " + x.getStatus()));
        assertTermCatProperty(term_0, false, categoryQName);
        assertTermCatProperty(term_1, false, categoryQName);
        assertTermCatProperty(term_2, false, categoryQName);
        assertTermCatProperty(term_3, categoryQName);
        assertTermCatProperty(term_4, categoryQName);

 /*       // remove all terms from category entity
        entity.setRelationshipAttribute("terms", new HashSet<>());
        entityWithExtInfo.setEntity(entity);

        response = createEntity(entityWithExtInfo);

        Thread.sleep(2000);
        assertTermCatProperty(term_0, false, categoryQName);
        assertTermCatProperty(term_1, false, categoryQName);
        assertTermCatProperty(term_2, false, categoryQName);
        assertTermCatProperty(term_3, false, categoryQName);
        assertTermCatProperty(term_4, false, categoryQName);*/


        LOG.info("<< testCreateOrAddCategoryEntityRest");
    }

    private static void assertTermCatProperty(AtlasGlossaryTerm term, String... expectedCatQNames) {
        assertTermCatProperty(term, true, expectedCatQNames);
    }

    private static void assertTermCatProperty(AtlasGlossaryTerm term, boolean shouldPresent, String... expectedCatQNames) {
        SearchHit[] searchHit = ESUtil.searchWithName(term.getName()).getHits().getHits();
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

    private static void assertESHits(SearchHit[] searchHit, String glossaryQn, CatNCatTermHeader catHeaderMap) {
        assertNotNull(searchHit);
        assertTrue(searchHit.length > 0);
        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            assertEquals(glossaryQn, sourceAsMap.get("__glossary"));

            List<String> actualCategories = (List<String>) sourceAsMap.get("__categories");

            if (catHeaderMap == null){
                assertNotNull(actualCategories);
                assertEquals(0, actualCategories.size());
            } else {
                assertNotNull(actualCategories);
                assertEquals(catHeaderMap.getCategories().size(), actualCategories.size());

                Set<String> expectedQNames = catHeaderMap.getCategories().stream().map(c -> c.getQualifiedName()).collect(Collectors.toSet());
                expectedQNames.stream().forEach(ac -> assertTrue(actualCategories.contains(ac)));
            }

        }
    }

    private static void assertESHits(String glossaryQn, TermNTermHeader termHeaderMap, String... expectedQNames) {
        for (AtlasGlossaryTerm term : termHeaderMap.getTerms()) {
            SearchHit[] searchHit = ESUtil.searchWithName(term.getName()).getHits().getHits();

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

}
