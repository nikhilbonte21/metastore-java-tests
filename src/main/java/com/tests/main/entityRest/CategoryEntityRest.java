package com.tests.main.entityRest;

import com.tests.main.glossary.tests.TestsMain;
import com.tests.main.glossary.utils.ESUtils;
;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.*;
import java.util.stream.Collectors;

import static com.tests.main.glossary.utils.TestUtils.*;
import static org.junit.Assert.*;

public class CategoryEntityRest implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(CategoryEntityRest.class);

    public static void main(String[] args) throws Exception {
        try {
            new CategoryEntityRest().run();
        } finally {
            cleanUpAll();
            ESUtils.close();
        }
    }

    @Override
    public void run() throws Exception{
        LOG.info("Running CategoryEntityRest tests");

        long start = System.currentTimeMillis();
        try {
            testCreateCategory();
            changeAnotherAnchorNotAllowed();
            changeParentInAnotherAnchorNotAllowed();
            testUpdateParent();
            testCreateAllBulk();
            testCreateCategoryWithParentBulk();
            testUpdateParentWithChildren();
            testUpdateChildren();
            testChildrenRelationWithSetToSetRelationshipDef();
            testDeleteCategory();

            //TODO: add term tests

        } catch (Exception e){
            throw e;
        } finally {
            LOG.info("Completed running CategoryEntityRest tests, took {} seconds", (System.currentTimeMillis() - start) / 1000 );
        }
    }

    private static void testCreateCategory() throws Exception {
        LOG.info(">> testCreateCategory");

        AtlasEntity glossary_0 = createGlossary();
        String gloGUID = glossary_0.getGuid();
        String gloQname = (String) glossary_0.getAttribute(QUALIFIED_NAME);

        String guid = createCategory(gloGUID, null).getGuid();

        Thread.sleep(2000);
        AtlasEntity createdEntity = getEntity(guid);

        assertNotNull(createdEntity);
        Map<String, String> anchor = getAnchorRelationshipAttribute(createdEntity);
        assertNotNull(anchor);
        assertEquals(anchor.get("guid"), gloGUID);
        verifyESGlossary(createdEntity.getGuid(), gloQname);

        LOG.info("<< testCreateCategory");
    }

    private static void changeAnotherAnchorNotAllowed() throws Exception {
        LOG.info(">> changeAnotherAnchorNotAllowed");

        AtlasEntity glossary_0 = createGlossary();
        AtlasEntity glossary_1 = createGlossary();

        AtlasEntity category_0 = getEntity(createCategory(glossary_0.getGuid(), null).getGuid());
        AtlasEntity category_1 = getEntity(createCategory(glossary_0.getGuid(), category_0.getGuid()).getGuid());

        AtlasEntity category_2 = getEntity(createCategory(glossary_1.getGuid(), null).getGuid());
        AtlasEntity category_3 = getEntity(createCategory(glossary_1.getGuid(), category_2.getGuid()).getGuid());

        category_3.setRelationshipAttribute("anchor", getAnchorObjectId(glossary_0.getGuid()));

        boolean failed = false;
        try {
            createEntity(new AtlasEntity.AtlasEntityWithExtInfo(category_3));
        } catch (AtlasServiceException exception) {
            LOG.info("This test have failed as expected");
            assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-400-00-0010"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }
        LOG.info(">> changeAnotherAnchorNotAllowed");
    }

    private static void testUpdateParent() throws Exception {
        LOG.info(">> testUpdateParent");

        AtlasEntity glossary_0 = createGlossary();
        String gloGUID = glossary_0.getGuid();
        String gloQname = (String) glossary_0.getAttribute(QUALIFIED_NAME);

        AtlasEntity cat_0 = createCategory(gloGUID, "cat_0", null, null);
        AtlasEntity cat_1 = createCategory(gloGUID, "cat_1", null, null);
        AtlasEntity cat_2 = createCategory(gloGUID, "cat_2", null, null);

        String cat_0_nanoId = getNanoid(getQualifiedName(cat_0));
        String cat_1_nanoId = getNanoid(getQualifiedName(cat_1));
        String cat_2_nanoId = getNanoid(getQualifiedName(cat_2));


        assertEquals(getQualifiedName(cat_0), concat(cat_0_nanoId, gloQname));
        assertEquals(getQualifiedName(cat_1), concat(cat_1_nanoId, gloQname));
        assertEquals(getQualifiedName(cat_2), concat(cat_2_nanoId, gloQname));

        verifyESGlossary(cat_0.getGuid(), gloQname);
        verifyESGlossary(cat_1.getGuid(), gloQname);
        verifyESGlossary(cat_2.getGuid(), gloQname);


        //update cat_2 add parent as cat_1
        cat_2.setRelationshipAttribute("parentCategory", getParentCategoryObjectId(cat_1.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(cat_2));

        Thread.sleep(2000);
        assertEquals(getQualifiedName(getEntity(cat_1.getGuid())), concat(cat_1_nanoId, gloQname));
        AtlasEntity cat_2_updated = getEntity(cat_2.getGuid());
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQname));
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1), gloQname);

        //update cat_2 add parent as cat_0
        cat_2.setRelationshipAttribute("parentCategory", getParentCategoryObjectId(cat_0.getGuid()));
        createEntity(cat_2);

        Thread.sleep(2000);
        assertEquals(getQualifiedName(getEntity(cat_1.getGuid())), concat(cat_1_nanoId, gloQname));
        cat_2_updated = getEntity(cat_2.getGuid());
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQname));
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_0), gloQname);

        //update cat_2 remove parent
        cat_2.setRelationshipAttribute("parentCategory", null);
        createEntity(cat_2);

        Thread.sleep(2000);
        cat_2_updated = getEntity(cat_2.getGuid());
        Map<String, String> parent = getParentRelationshipAttribute(cat_2_updated);
        assertNotNull(parent);
        assertEquals("DELETED", parent.get("relationshipStatus"));
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQname));
        verifyESGlossary(cat_2.getGuid(), gloQname);

        LOG.info("<< testUpdateParent");
    }

    private static void changeParentInAnotherAnchorNotAllowed() throws Exception {
        LOG.info(">> changeParentInAnotherAnchorNotAllowed");

        AtlasEntity glossary_0 = createGlossary("glossary_0");
        AtlasEntity glossary_1 = createGlossary("glossary_1");

        AtlasEntity category_0 = createCategory(glossary_0.getGuid(), "cat_0", null, null);
        AtlasEntity category_1 = createCategory(glossary_0.getGuid(), "cat_1", category_0.getGuid(), null);

        AtlasEntity category_2 = createCategory(glossary_1.getGuid(), "cat_2", null, null);
        AtlasEntity category_3 = createCategory(glossary_1.getGuid(), "cat_3", category_2.getGuid(), null);

        Thread.sleep(2000);
        category_3 = getEntity(category_3.getGuid());
        category_3.setRelationshipAttribute(PARENT_CATEGORY, getParentCategoryObjectId(category_1.getGuid()));

        boolean failed = false;
        try {
            createEntity(category_3);
        } catch (AtlasServiceException exception) {
            LOG.info("This test failed as expected");
            assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-400-00-0015"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        Thread.sleep(2000);
        category_2 = getEntity(category_2.getGuid());
        category_2.setRelationshipAttribute(CHILDREN_CATEGORY, getCategoryObjectIds(category_1.getGuid()));

        failed = false;
        try {
            createEntity(category_2);
        } catch (AtlasServiceException exception) {
            LOG.info("This test failed as expected");
            assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-400-00-0015"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        failed = false;
        try {
            createCategory(glossary_1.getGuid(), "cat_4", category_0.getGuid(), null);
        } catch (AtlasServiceException exception) {
            LOG.info("This test failed as expected");
            assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-400-00-0015"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        failed = false;
        try {
            AtlasEntity entity = getAtlasEntity(TYPE_CATEGORY, "cat_4").getEntity();
            entity.setRelationshipAttribute(ANCHOR, getObjectId(glossary_1.getGuid(), TYPE_GLOSSARY));
            entity.setRelationshipAttribute(CHILDREN_CATEGORY, getCategoryObjectIds(category_1.getGuid()));

            createEntity(entity);
        } catch (AtlasServiceException exception) {
            LOG.info("This test failed as expected");
            assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-400-00-0015"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        LOG.info(">> changeParentInAnotherAnchorNotAllowed");
    }


    private static void testUpdateParentWithChildren() throws Exception {
        LOG.info(">> testUpdateParentWithChildren");

        LOG.info(getAdminStatus());

        AtlasEntity glossary_0, cat_0_updated, cat_1_updated, cat_2_updated, cat_3_updated, cat_4_updated, cat_5_updated, cat_6_updated;
        Map parentCat = new HashMap();
        List<Map> childrenCat = new ArrayList<>();

        glossary_0 = createGlossary();
        String gloGUID = glossary_0.getGuid();
        String gloQName = getQualifiedName(glossary_0);

        /*
        1
        cat_0  -->  cat_1  -->  cat_2  --> cat_3
                                       --> cat_4
        cat_5
        cat_6
        */

        AtlasEntity cat_0 = getEntity(createCategory(gloGUID, "cat_0", null,  null).getGuid());
        AtlasEntity cat_5 = getEntity(createCategory(gloGUID,  "cat_5",null,null).getGuid());
        AtlasEntity cat_6 = getEntity(createCategory(gloGUID,  "cat_6",null,null).getGuid());

        AtlasEntity cat_1 = createCategory(gloGUID, "cat_1", cat_0.getGuid(), null);
        AtlasEntity cat_2 = createCategory(gloGUID, "cat_2", cat_1.getGuid(), null);
        AtlasEntity cat_3 = createCategory(gloGUID, "cat_3", cat_2.getGuid(), null);
        AtlasEntity cat_4 = createCategory(gloGUID, "cat_4", cat_2.getGuid(), null);

        cat_1 = getEntity(cat_1.getGuid());
        cat_2 = getEntity(cat_2.getGuid());
        cat_3 = getEntity(cat_3.getGuid());
        cat_4 = getEntity(cat_4.getGuid());

        String cat_0_nanoId = getNanoid(getQualifiedName(cat_0));
        String cat_1_nanoId = getNanoid(getQualifiedName(cat_1));
        String cat_2_nanoId = getNanoid(getQualifiedName(cat_2));
        String cat_3_nanoId = getNanoid(getQualifiedName(cat_3));
        String cat_4_nanoId = getNanoid(getQualifiedName(cat_4));
        String cat_5_nanoId = getNanoid(getQualifiedName(cat_5));
        String cat_6_nanoId = getNanoid(getQualifiedName(cat_6));


        assertEquals(getQualifiedName(cat_0), concat(cat_0_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_5), concat(cat_5_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_6), concat(cat_6_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_1), concat(cat_1_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_2), concat(cat_2_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_3), concat(cat_3_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_4), concat(cat_4_nanoId, gloQName));

        Thread.sleep(2000);
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());
        cat_2_updated = getEntity(cat_2.getGuid());
        cat_3_updated = getEntity(cat_3.getGuid());
        cat_4_updated = getEntity(cat_4.getGuid());
        cat_5_updated = getEntity(cat_5.getGuid());
        cat_6_updated = getEntity(cat_6.getGuid());

        parentCat = getParentRelationshipAttribute(cat_1_updated);
        assertNotNull(parentCat);
        assertEquals(cat_0.getGuid(), parentCat.get("guid"));

        parentCat = getParentRelationshipAttribute(cat_2_updated);
        assertNotNull(parentCat);
        assertEquals(cat_1.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_2_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_3.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_4.getGuid());
        childrenCat.clear();

        parentCat = (HashMap) cat_3_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_4_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        Thread.sleep(2000);
        verifyESGlossary(cat_5.getGuid(), gloQName);
        verifyESGlossary(cat_6.getGuid(), gloQName);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_0), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1), gloQName);
        verifyESParentCatAndGlossary(cat_4.getGuid(), getQualifiedName(cat_2), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_2), gloQName);

        //------------------------------------------------------------------------------------------------------------------------

        /*
        2
        cat_0  -->  cat_1  -->  cat_2  --> cat_3
                                       --> cat_4
        cat_5
        cat_6
        */
        //update cat_1 add parent as cat_5
        cat_1.setRelationshipAttribute("parentCategory", getParentCategoryObjectId(cat_5.getGuid()));
        EntityMutationResponse response = createEntity(new AtlasEntity.AtlasEntityWithExtInfo(cat_1));

        //assertEquals(4, response.getUpdatedEntities().size());

        Thread.sleep(2000);
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());
        cat_2_updated = getEntity(cat_2.getGuid());
        cat_3_updated = getEntity(cat_3.getGuid());
        cat_4_updated = getEntity(cat_4.getGuid());
        cat_5_updated = getEntity(cat_5.getGuid());
        cat_6_updated = getEntity(cat_6.getGuid());


        assertEquals(getQualifiedName(cat_0_updated), concat(cat_0_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_6_updated), concat(cat_6_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_5_updated), concat(cat_5_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_1_updated), concat(cat_1_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_3_updated), concat(cat_3_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_4_updated), concat(cat_4_nanoId, gloQName));

        parentCat = (HashMap) cat_1_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_5.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_2_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_1.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_2_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_3.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_4.getGuid());
        childrenCat.clear();

        parentCat = (HashMap) cat_3_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_4_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_0_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));

        childrenCat = (List) cat_5_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_1.getGuid());
        childrenCat.clear();


        Thread.sleep(2000);
        verifyESGlossary(cat_0.getGuid(), gloQName);
        verifyESGlossary(cat_6.getGuid(), gloQName);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_5), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1_updated), gloQName);
        verifyESParentCatAndGlossary(cat_4.getGuid(), getQualifiedName(cat_2_updated), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_2_updated), gloQName);

        //------------------------------------------------------------------------------------------------------------------------
        /*
        3
        cat_0
        cat_5   -->  cat_1  -->  cat_2  --> cat_3
                                        --> cat_4
        cat_6
        */
        //update cat_1 add parent as cat_6
        cat_1.setRelationshipAttribute("parentCategory", getParentCategoryObjectId(cat_6.getGuid()));
        response = createEntity(new AtlasEntity.AtlasEntityWithExtInfo(cat_1));

        assertEquals(3, response.getUpdatedEntities().size());

        Thread.sleep(2000);
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());
        cat_2_updated = getEntity(cat_2.getGuid());
        cat_3_updated = getEntity(cat_3.getGuid());
        cat_4_updated = getEntity(cat_4.getGuid());
        cat_5_updated = getEntity(cat_5.getGuid());
        cat_6_updated = getEntity(cat_6.getGuid());


        assertEquals(getQualifiedName(cat_0_updated), concat(cat_0_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_5_updated), concat(cat_5_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_6_updated), concat(cat_6_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_1_updated), concat(cat_1_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_3_updated), concat(cat_3_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_4_updated), concat(cat_4_nanoId, gloQName));

        parentCat = (HashMap) cat_1_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_6.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_2_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_1.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_5_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));


        childrenCat = (List) cat_6_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_1.getGuid());
        childrenCat.clear();

        childrenCat = (List) cat_2_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_3.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_4.getGuid());
        childrenCat.clear();

        parentCat = (HashMap) cat_3_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_4_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));


        childrenCat = (List) cat_0_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));

        Thread.sleep(2000);
        verifyESGlossary(cat_0.getGuid(), gloQName);
        verifyESGlossary(cat_5.getGuid(), gloQName);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_6), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1_updated), gloQName);
        verifyESParentCatAndGlossary(cat_4.getGuid(), getQualifiedName(cat_2_updated), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_2_updated), gloQName);


        //------------------------------------------------------------------------------------------------------------------------
        //update cat_1 add parent as cat_0
        cat_1.setRelationshipAttribute("parentCategory", getParentCategoryObjectId(cat_0.getGuid()));
        response = createEntity(new AtlasEntity.AtlasEntityWithExtInfo(cat_1));

        assertEquals(3, response.getUpdatedEntities().size());

        Thread.sleep(2000);
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());
        cat_2_updated = getEntity(cat_2.getGuid());
        cat_3_updated = getEntity(cat_3.getGuid());
        cat_4_updated = getEntity(cat_4.getGuid());
        cat_5_updated = getEntity(cat_5.getGuid());
        cat_6_updated = getEntity(cat_6.getGuid());


        assertEquals(getQualifiedName(cat_0_updated), concat(cat_0_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_5_updated), concat(cat_5_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_6_updated), concat(cat_6_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_1_updated), concat(cat_1_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_3_updated), concat(cat_3_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_4_updated), concat(cat_4_nanoId, gloQName));

        parentCat = (HashMap) cat_1_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_0.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_2_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_1.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_2_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_3.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_4.getGuid());
        childrenCat.clear();

        parentCat = (HashMap) cat_3_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_4_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_5_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));

        childrenCat = (List) cat_0_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        assertEquals(1, childrenCat.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());
        assertEquals(1, childrenCat.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());


        childrenCat = (List) cat_6_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));

        Thread.sleep(2000);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_0), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1_updated), gloQName);
        verifyESParentCatAndGlossary(cat_4.getGuid(), getQualifiedName(cat_2_updated), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_2_updated), gloQName);

        //------------------------------------------------------------------------------------------------------------------------
        /*
            cat_0  -->  cat_1  -->  cat_2  --> cat_3
                                           --> cat_4
            cat_5
            cat_6
        */

        //update cat_2 add parent as cat_6
        cat_2.setRelationshipAttribute("parentCategory", getParentCategoryObjectId(cat_6.getGuid()));
        response = createEntity(new AtlasEntity.AtlasEntityWithExtInfo(cat_2));

        //assertEquals(6, response.getUpdatedEntities().size());

        Thread.sleep(2000);
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());
        cat_2_updated = getEntity(cat_2.getGuid());
        cat_3_updated = getEntity(cat_3.getGuid());
        cat_4_updated = getEntity(cat_4.getGuid());
        cat_5_updated = getEntity(cat_5.getGuid());
        cat_6_updated = getEntity(cat_6.getGuid());


        assertEquals(getQualifiedName(cat_0_updated), concat(cat_0_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_5_updated), concat(cat_5_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_6_updated), concat(cat_6_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_1_updated), concat(cat_1_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_3_updated), concat(cat_3_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_4_updated), concat(cat_4_nanoId, gloQName));

        parentCat = (HashMap) cat_1_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_0.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_1_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_2.getGuid());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));

        parentCat = (HashMap) cat_2_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_6.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_2_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_3.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_4.getGuid());
        childrenCat.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));

        parentCat = (HashMap) cat_3_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_4_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_6_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_2.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_1.getGuid());
        assertEquals(1, childrenCat.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());
        assertEquals(1, childrenCat.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());

        Thread.sleep(2000);
        verifyESGlossary(cat_5.getGuid(), gloQName);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_0), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_6_updated), gloQName);
        verifyESParentCatAndGlossary(cat_4.getGuid(), getQualifiedName(cat_2_updated), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_2_updated), gloQName);

        //------------------------------------------------------------------------------------------------------------------------
        /*
            cat_0  -->  cat_1  -->  cat_2  --> cat_3
                                           --> cat_4
            cat_5
            cat_6
        */

        //update cat_2 add parent as cat_1
        cat_2.setRelationshipAttribute("parentCategory", getParentCategoryObjectId(cat_1.getGuid()));
        response = createEntity(new AtlasEntity.AtlasEntityWithExtInfo(cat_2));

        //assertEquals(4, response.getUpdatedEntities().size());

        Thread.sleep(2000);
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());
        cat_2_updated = getEntity(cat_2.getGuid());
        cat_3_updated = getEntity(cat_3.getGuid());
        cat_4_updated = getEntity(cat_4.getGuid());
        cat_5_updated = getEntity(cat_5.getGuid());


        assertEquals(getQualifiedName(cat_0_updated), concat(cat_0_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_5_updated), concat(cat_5_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_6_updated), concat(cat_6_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_1_updated), concat(cat_1_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_3_updated), concat(cat_3_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_4_updated), concat(cat_4_nanoId, gloQName));

        HashMap parent_cat = (HashMap) cat_2_updated.getRelationshipAttribute("parentCategory");
        assertNotNull(parent_cat);
        assertEquals(cat_1.getGuid(), parent_cat.get("guid"));


        Thread.sleep(2000);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_0), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1_updated), gloQName);
        verifyESParentCatAndGlossary(cat_4.getGuid(), getQualifiedName(cat_2_updated), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_2_updated), gloQName);

        //------------------------------------------------------------------------------------------------------------------------
        /*
        Last
            cat_5
            cat_6  -->  cat_0   -->  cat_1  -->  cat_2  --> cat_3
                                                        --> cat_4
        */
        //update cat_0 add parent as cat_6
        cat_0.setRelationshipAttribute("parentCategory", getParentCategoryObjectId(cat_6.getGuid()));
        response = createEntity(new AtlasEntity.AtlasEntityWithExtInfo(cat_0));

        assertEquals(3, response.getUpdatedEntities().size());

        Thread.sleep(2000);
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());
        cat_2_updated = getEntity(cat_2.getGuid());
        cat_3_updated = getEntity(cat_3.getGuid());
        cat_4_updated = getEntity(cat_4.getGuid());
        cat_5_updated = getEntity(cat_5.getGuid());
        cat_6_updated = getEntity(cat_6.getGuid());


        assertEquals(getQualifiedName(cat_5_updated), concat(cat_5_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_6_updated), concat(cat_6_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_0_updated), concat(cat_0_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_1_updated), concat(cat_1_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_3_updated), concat(cat_3_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_4_updated), concat(cat_4_nanoId, gloQName));

        parentCat = getParentRelationshipAttribute(cat_1_updated);
        assertNotNull(parentCat);
        assertEquals(cat_0.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_1_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_2.getGuid());
        assertEquals(1, childrenCat.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());
        assertEquals(1, childrenCat.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());

        parentCat = (HashMap) cat_2_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_1.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_2_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_3.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_4.getGuid());
        childrenCat.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));

        parentCat = (HashMap) cat_3_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_4_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_6_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertEquals(3, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_2.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_1.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_0.getGuid());
        assertEquals(1, childrenCat.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());
        assertEquals(2, childrenCat.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());

        Thread.sleep(2000);
        verifyESParentCatAndGlossary(cat_0.getGuid(), getQualifiedName(cat_6), gloQName);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_0_updated), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1_updated), gloQName);
        verifyESParentCatAndGlossary(cat_4.getGuid(), getQualifiedName(cat_2_updated), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_2_updated), gloQName);

        LOG.info("<< testUpdateParentWithChildren");
    }

    private static void testUpdateChildren() throws Exception {
        LOG.info(">> testUpdateChildren");

        LOG.info(getAdminStatus());

        AtlasEntity glossary_0, cat_0_updated, cat_1_updated, cat_2_updated, cat_3_updated, cat_4_updated, cat_5_updated, cat_6_updated;
        Map parentCat = new HashMap();
        List<Map> childrenCat = new ArrayList<>();

        glossary_0 = createGlossary();
        String gloGUID = glossary_0.getGuid();
        String gloQName = getQualifiedName(glossary_0);

        /*
        1
        cat_0  -->  cat_1  -->  cat_2  --> cat_3
                                       --> cat_4
        cat_5
        cat_6
        */

        AtlasEntity cat_0 = getEntity(createCategory(gloGUID, "cat_0", null,  null).getGuid());
        AtlasEntity cat_5 = getEntity(createCategory(gloGUID,  "cat_5",null,null).getGuid());
        AtlasEntity cat_6 = getEntity(createCategory(gloGUID,  "cat_6",null,null).getGuid());

        AtlasEntity cat_1 = createCategory(gloGUID, "cat_1", null, null);
        AtlasEntity cat_2 = createCategory(gloGUID, "cat_2", null, null);
        AtlasEntity cat_3 = createCategory(gloGUID, "cat_3", null, null);
        AtlasEntity cat_4 = createCategory(gloGUID, "cat_4", null, null);

        cat_0.setRelationshipAttribute(CHILDREN_CATEGORY, getCategoryObjectIds(cat_1.getGuid()));
        createEntity(cat_0);
        cat_1 = getEntity(cat_1.getGuid());
        cat_1.setRelationshipAttribute(CHILDREN_CATEGORY, getCategoryObjectIds(cat_2.getGuid()));
        createEntity(cat_1);
        cat_2_updated = getEntity(cat_2.getGuid());

        cat_2_updated.setRelationshipAttribute(CHILDREN_CATEGORY, getCategoryObjectIds(cat_3.getGuid(), cat_4.getGuid()));
        createEntity(cat_2_updated);

        Thread.sleep(3000);
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());
        cat_2_updated = getEntity(cat_2.getGuid());
        cat_3_updated = getEntity(cat_3.getGuid());
        cat_4_updated = getEntity(cat_4.getGuid());
        cat_5_updated = getEntity(cat_5.getGuid());
        cat_6_updated = getEntity(cat_6.getGuid());

        String cat_0_nanoId = getNanoid(getQualifiedName(cat_0));
        String cat_1_nanoId = getNanoid(getQualifiedName(cat_1));
        String cat_2_nanoId = getNanoid(getQualifiedName(cat_2));
        String cat_3_nanoId = getNanoid(getQualifiedName(cat_3));
        String cat_4_nanoId = getNanoid(getQualifiedName(cat_4));
        String cat_5_nanoId = getNanoid(getQualifiedName(cat_5));
        String cat_6_nanoId = getNanoid(getQualifiedName(cat_6));


        assertEquals(getQualifiedName(cat_0), concat(cat_0_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_5), concat(cat_5_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_6), concat(cat_6_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_1), concat(cat_1_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_2), concat(cat_2_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_3), concat(cat_3_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_4), concat(cat_4_nanoId, gloQName));

        parentCat = getParentRelationshipAttribute(cat_1_updated);
        assertNotNull(parentCat);
        assertEquals(cat_0.getGuid(), parentCat.get("guid"));

        parentCat = getParentRelationshipAttribute(cat_2_updated);
        assertNotNull(parentCat);
        assertEquals(cat_1.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_2_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_3.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_4.getGuid());
        childrenCat.clear();

        parentCat = (HashMap) cat_3_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_4_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));


        Thread.sleep(2000);
        verifyESGlossary(cat_0.getGuid(), gloQName);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_0), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1), gloQName);
        verifyESParentCatAndGlossary(cat_4.getGuid(), getQualifiedName(cat_2), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_2), gloQName);

        //------------------------------------------------------------------------------------------------------------------------
        /*
        2
            cat_0  -->  cat_1  -->  cat_2  --> cat_3
                                           --> cat_4
            cat_5
            cat_6
        **/
        //update cat_5 add children as cat_1
        cat_5_updated.setRelationshipAttribute(CHILDREN_CATEGORY, getCategoryObjectIds(cat_1.getGuid()));
        EntityMutationResponse response = createEntity(new AtlasEntity.AtlasEntityWithExtInfo(cat_5_updated));

        assertEquals(2, response.getUpdatedEntities().size());

        Thread.sleep(2000);
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());
        cat_2_updated = getEntity(cat_2.getGuid());
        cat_3_updated = getEntity(cat_3.getGuid());
        cat_4_updated = getEntity(cat_4.getGuid());
        cat_5_updated = getEntity(cat_5.getGuid());
        cat_6_updated = getEntity(cat_6.getGuid());


        parentCat = (HashMap) cat_1_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_5.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_2_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_1.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_2_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_3.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_4.getGuid());
        childrenCat.clear();

        parentCat = (HashMap) cat_3_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_4_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_0_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));

        childrenCat = (List) cat_5_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_1.getGuid());
        childrenCat.clear();

        assertEquals(getQualifiedName(cat_0_updated), concat(cat_0_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_6_updated), concat(cat_6_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_5_updated), concat(cat_5_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_1_updated), concat(cat_1_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_3_updated), concat(cat_3_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_4_updated), concat(cat_4_nanoId, gloQName));


        Thread.sleep(2000);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_5), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1_updated), gloQName);
        verifyESParentCatAndGlossary(cat_4.getGuid(), getQualifiedName(cat_2_updated), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_2_updated), gloQName);

        //------------------------------------------------------------------------------------------------------------------------
        /*
        3
            cat_0
            cat_5   -->  cat_1  -->  cat_2  --> cat_3
                                            --> cat_4
            cat_6
        **/
        //update cat_6 add children as cat_1
        cat_6_updated.setRelationshipAttribute(CHILDREN_CATEGORY, getCategoryObjectIds(cat_1.getGuid()));
        response = createEntity(new AtlasEntity.AtlasEntityWithExtInfo(cat_6_updated));

        assertEquals(2, response.getUpdatedEntities().size());

        Thread.sleep(5000);
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());
        cat_2_updated = getEntity(cat_2.getGuid());
        cat_3_updated = getEntity(cat_3.getGuid());
        cat_4_updated = getEntity(cat_4.getGuid());
        cat_5_updated = getEntity(cat_5.getGuid());
        cat_6_updated = getEntity(cat_6.getGuid());


        assertEquals(getQualifiedName(cat_0_updated), concat(cat_0_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_5_updated), concat(cat_5_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_6_updated), concat(cat_6_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_1_updated), concat(cat_1_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_3_updated), concat(cat_3_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_4_updated), concat(cat_4_nanoId, gloQName));


        childrenCat = (List) cat_5_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));


        childrenCat = (List) cat_6_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_1.getGuid());
        childrenCat.clear();

        parentCat = (HashMap) cat_1_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_6.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_2_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_1.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_2_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_3.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_4.getGuid());
        childrenCat.clear();

        parentCat = (HashMap) cat_3_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_4_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));


        childrenCat = (List) cat_0_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));


        Thread.sleep(2000);
        verifyESGlossary(cat_0.getGuid(), gloQName);
        verifyESGlossary(cat_5.getGuid(), gloQName);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_6), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1_updated), gloQName);
        verifyESParentCatAndGlossary(cat_4.getGuid(), getQualifiedName(cat_2_updated), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_2_updated), gloQName);


        //------------------------------------------------------------------------------------------------------------------------
        /*
        4
            cat_0
            cat_5
            cat_6   -->  cat_1  -->  cat_2  --> cat_3
                                            --> cat_4
        **/

        //update cat_0 add child as cat_1
        cat_0_updated.setRelationshipAttribute(CHILDREN_CATEGORY, getCategoryObjectIds(cat_1.getGuid()));
        response = createEntity(new AtlasEntity.AtlasEntityWithExtInfo(cat_0_updated));

        assertEquals(2, response.getUpdatedEntities().size());

        Thread.sleep(2000);
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());
        cat_2_updated = getEntity(cat_2.getGuid());
        cat_3_updated = getEntity(cat_3.getGuid());
        cat_4_updated = getEntity(cat_4.getGuid());
        cat_5_updated = getEntity(cat_5.getGuid());
        cat_6_updated = getEntity(cat_6.getGuid());


        assertEquals(getQualifiedName(cat_0_updated), concat(cat_0_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_5_updated), concat(cat_5_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_6_updated), concat(cat_6_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_1_updated), concat(cat_1_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_3_updated), concat(cat_3_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_4_updated), concat(cat_4_nanoId, gloQName));

        LOG.info("cat_0_updated {}", cat_0_updated.getGuid());
        LOG.info("cat_5_updated {}", cat_5_updated.getGuid());
        LOG.info("cat_6_updated {}", cat_6_updated.getGuid());

        parentCat = (HashMap) cat_1_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_0.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_2_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_1.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_2_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_3.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_4.getGuid());
        childrenCat.clear();

        parentCat = (HashMap) cat_3_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_4_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_2.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_5_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));

        childrenCat = (List) cat_0_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        assertEquals(1, childrenCat.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());
        assertEquals(1, childrenCat.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());


        childrenCat = (List) cat_6_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));


        Thread.sleep(2000);
        verifyESGlossary(cat_5.getGuid(), gloQName);
        verifyESGlossary(cat_6.getGuid(), gloQName);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_0), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1_updated), gloQName);
        verifyESParentCatAndGlossary(cat_4.getGuid(), getQualifiedName(cat_2_updated), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_2_updated), gloQName);

        //------------------------------------------------------------------------------------------------------------------------
        /*
        5
            cat_0   -->  cat_1  -->  cat_2  --> cat_3
                                            --> cat_4
            cat_5
            cat_6
        **/

        //update cat_1 add child as cat_3 & cat_4
        cat_1_updated.setRelationshipAttribute(CHILDREN_CATEGORY, getCategoryObjectIds(cat_2.getGuid(), cat_3.getGuid(), cat_4.getGuid()));
        response = createEntity(new AtlasEntity.AtlasEntityWithExtInfo(cat_1_updated));

        //assertEquals(5, response.getUpdatedEntities().size());

        Thread.sleep(2000);
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());
        cat_2_updated = getEntity(cat_2.getGuid());
        cat_3_updated = getEntity(cat_3.getGuid());
        cat_4_updated = getEntity(cat_4.getGuid());
        cat_5_updated = getEntity(cat_5.getGuid());
        cat_6_updated = getEntity(cat_6.getGuid());

        assertEquals(getQualifiedName(cat_0_updated), concat(cat_0_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_5_updated), concat(cat_5_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_6_updated), concat(cat_6_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_1_updated), concat(cat_1_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_2_updated), concat(cat_2_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_3_updated), concat(cat_3_nanoId, gloQName));
        assertEquals(getQualifiedName(cat_4_updated), concat(cat_4_nanoId, gloQName));


        parentCat = (HashMap) cat_2_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_1.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_2_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_3.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_4.getGuid());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));
        childrenCat.clear();

        parentCat = (HashMap) cat_3_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_1.getGuid(), parentCat.get("guid"));

        parentCat = (HashMap) cat_4_updated.getRelationshipAttribute(PARENT_CATEGORY);
        assertNotNull(parentCat);
        assertEquals(cat_1.getGuid(), parentCat.get("guid"));

        childrenCat = (List) cat_1_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(3, childrenCat.size());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_3.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_4.getGuid());
        childrenCat.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(cat_2.getGuid());
        childrenCat.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        childrenCat.clear();

        childrenCat = (List) cat_5_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));

        childrenCat = (List) cat_0_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(2, childrenCat.size());
        assertEquals(1, childrenCat.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());
        assertEquals(1, childrenCat.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());

        childrenCat = (List) cat_6_updated.getRelationshipAttribute(CHILDREN_CATEGORY);
        assertNotNull(childrenCat);
        assertEquals(1, childrenCat.size());
        childrenCat.stream().forEach(x -> assertTrue("DELETED".equals(x.get("relationshipStatus"))));

        Thread.sleep(2000);
        verifyESGlossary(cat_5.getGuid(), gloQName);
        verifyESGlossary(cat_6.getGuid(), gloQName);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_0), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1_updated), gloQName);
        verifyESParentCatAndGlossary(cat_4.getGuid(), getQualifiedName(cat_1_updated), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_1_updated), gloQName);

        LOG.info("<< testUpdateChildren");
    }

    private static void testCreateAllBulk() throws Exception {
        LOG.info(">> testCreateAllBulk");
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();

        AtlasEntity entity = new AtlasEntity(TYPE_GLOSSARY);
        String name = getRandomName();
        entity.setAttribute(NAME, name);
        entity.setGuid("-1");
        entity.setAttribute(QUALIFIED_NAME, name);
        entitiesWithExtInfo.addEntity(entity);


        entity = new AtlasEntity(TYPE_CATEGORY);
        name = "cat_2";
        entity.setAttribute(NAME, name);
        entity.setGuid("-2");
        entity.setAttribute(QUALIFIED_NAME, name);
        entity.setRelationshipAttribute("anchor", getAnchorObjectId("-1"));
        entitiesWithExtInfo.addEntity(entity);

        entity = new AtlasEntity(TYPE_CATEGORY);
        name = "cat_22";
        entity.setAttribute(NAME, name);
        entity.setGuid("-22");
        entity.setAttribute(QUALIFIED_NAME, name);
        entity.setRelationshipAttribute("anchor", getAnchorObjectId("-1"));
        entity.setRelationshipAttribute("parentCategory", getParentCategoryObjectId("-2"));
        entitiesWithExtInfo.addEntity(entity);

        entity = new AtlasEntity(TYPE_CATEGORY);
        name = "cat_222";
        entity.setAttribute(NAME, name);
        entity.setGuid("-222");
        entity.setAttribute(QUALIFIED_NAME, name);
        entity.setRelationshipAttribute("anchor", getAnchorObjectId("-1"));
        entity.setRelationshipAttribute("parentCategory", getParentCategoryObjectId("-22"));
        entitiesWithExtInfo.addEntity(entity);



        entity = new AtlasEntity(TYPE_TERM);
        name = "term_3";
        entity.setAttribute(NAME, name);
        entity.setGuid("-3");
        entity.setAttribute(QUALIFIED_NAME, name);
        entity.setRelationshipAttribute("anchor", getAnchorObjectId("-1"));
        entity.setRelationshipAttribute("categories", getCategoryObjectIds("-2", "-22"));
        entitiesWithExtInfo.addEntity(entity);


        entity = new AtlasEntity(TYPE_TERM);
        name = "term_33";
        entity.setAttribute(NAME, name);
        entity.setGuid("-33");
        entity.setAttribute(QUALIFIED_NAME, name);
        entity.setRelationshipAttribute("anchor", getAnchorObjectId("-1"));
        entity.setRelationshipAttribute("categories", getCategoryObjectIds("-2", "-222"));
        entitiesWithExtInfo.addEntity(entity);

        EntityMutationResponse response = createEntitiesBulk(entitiesWithExtInfo);

        AtlasEntity glo = getEntity(response.getGuidAssignments().get("-1"));
        AtlasEntity cat_2 = getEntity(response.getGuidAssignments().get("-2"));
        AtlasEntity cat_22 = getEntity(response.getGuidAssignments().get("-22"));
        AtlasEntity cat_222 = getEntity(response.getGuidAssignments().get("-222"));
        AtlasEntity term_3 = getEntity(response.getGuidAssignments().get("-3"));
        AtlasEntity term_33 = getEntity(response.getGuidAssignments().get("-33"));


        String glo_nanoId = getNanoid(getQualifiedName(glo));
        String cat_2_nanoId = getNanoid(getQualifiedName(cat_2));
        String cat_22_nanoId = getNanoid(getQualifiedName(cat_22));
        String cat_222_nanoId = getNanoid(getQualifiedName(cat_222));
        String term_0_nanoId = getNanoid(getQualifiedName(term_3));
        String term_1_nanoId = getNanoid(getQualifiedName(term_33));

        assertEquals(getQualifiedName(cat_2), concat(cat_2_nanoId, glo_nanoId));
        assertEquals(getQualifiedName(cat_22), concat(cat_22_nanoId, glo_nanoId));
        assertEquals(getQualifiedName(cat_222), concat(cat_222_nanoId, glo_nanoId));
        assertEquals(getQualifiedName(term_3), concat(term_0_nanoId, glo_nanoId));
        assertEquals(getQualifiedName(term_33), concat(term_1_nanoId, glo_nanoId));

        Thread.sleep(3000);
        String gloQName = getQualifiedName(glo);
        verifyESGlossary(cat_2.getGuid(), gloQName);
        verifyESParentCatAndGlossary(cat_22.getGuid(), getQualifiedName(cat_2), gloQName);
        verifyESParentCatAndGlossary(cat_222.getGuid(), getQualifiedName(cat_22), gloQName);
        verifyESCategoriesAndGlossary(term_3.getGuid(), gloQName, getQualifiedName(cat_2), getQualifiedName(cat_22));
        verifyESCategoriesAndGlossary(term_33.getGuid(), gloQName, getQualifiedName(cat_2), getQualifiedName(cat_222));

        assertNotNull(cat_22.getRelationshipAttribute("parentCategory"));
        Map<String, String> parent = (Map<String, String>) cat_22.getRelationshipAttribute("parentCategory");
        assertEquals(parent.get("guid"), cat_2.getGuid());

        assertNotNull(term_3.getRelationshipAttribute("categories"));
        List<Map<String, String>> categories = (List<Map<String, String>>) term_3.getRelationshipAttribute("categories");
        assertEquals(categories.size(), 2);

        assertNotNull(term_33.getRelationshipAttribute("categories"));
        categories = (List<Map<String, String>>) term_33.getRelationshipAttribute("categories");
        assertEquals(categories.size(), 2);

        LOG.info("<< testCreateAllBulk");
    }

    private static void testCreateCategoryWithParentBulk() throws Exception {
        LOG.info(">> testCreateCategoryWithParentBulk");

        AtlasEntity glossary = createGlossary();
        String gloGuid = glossary.getGuid();
        String gloQNAme = getQualifiedName(glossary);

        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();

        AtlasEntity entity = new AtlasEntity(TYPE_CATEGORY);
        String name = "cat_1";
        entity.setAttribute(NAME, name);
        entity.setGuid("-1");
        entity.setAttribute(QUALIFIED_NAME, name);

        entity.setRelationshipAttribute("anchor", getAnchorObjectId(gloGuid));
        entitiesWithExtInfo.addEntity(entity);

        AtlasEntity entity_0 = new AtlasEntity(TYPE_CATEGORY);
        name = "cat_2";
        entity_0.setAttribute(NAME, name);
        entity_0.setGuid("-2");
        entity_0.setAttribute(QUALIFIED_NAME, name);

        entity_0.setRelationshipAttribute("anchor", getAnchorObjectId(gloGuid));
        entity_0.setRelationshipAttribute("parentCategory", getParentCategoryObjectId("-1"));

        entitiesWithExtInfo.addEntity(entity_0);

        EntityMutationResponse response = createEntitiesBulk(entitiesWithExtInfo);


        AtlasEntity cat_0 = getEntity(response.getGuidAssignments().get("-1"));
        AtlasEntity cat_1 = getEntity(response.getGuidAssignments().get("-2"));


        String cat_0_nanoId = getNanoid(getQualifiedName(cat_0));
        String cat_1_nanoId = getNanoid(getQualifiedName(cat_1));

        assertEquals(getQualifiedName(cat_0), concat(cat_0_nanoId, gloQNAme));
        assertEquals(getQualifiedName(cat_1), concat(cat_1_nanoId, gloQNAme));

        LOG.info("<< testCreateCategoryWithParentBulk");
    }

    private static void testChildrenRelationWithSetToSetRelationshipDef() throws Exception {
        LOG.info(">> testChildrenRelationWithSetToSetRelationshipDef");

        EntityMutationResponse response;
        AtlasEntity table_0, table_1, process_0, process_1;

        table_0 = createCustomEntity(TYPE_TABLE, "table_0");
        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        List<HashMap> children = new ArrayList<>();

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_0");
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid(), table_1.getGuid()));

        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_0 = getEntity(response.getCreatedEntities().get(0).getGuid());
        table_0 = getEntity(table_0.getGuid());
        table_1 = getEntity(table_1.getGuid());

        children = (List) table_0.getRelationshipAttribute(OUTPUTS_FROM_P);
        assertNotNull(children);
        assertEquals(1, children.size());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(process_0.getGuid());

        children = (List) table_1.getRelationshipAttribute(OUTPUTS_FROM_P);
        assertNotNull(children);
        assertEquals(1, children.size());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(process_0.getGuid());

        children = (List) process_0.getRelationshipAttribute(OUTPUTS);
        assertNotNull(children);
        assertEquals(2, children.size());
        children.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(table_0.getGuid());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(table_1.getGuid());


        /*
        2.
        p0  -->  t0, t1

        p1  -->  t0, t1
        * */
        extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid(), table_1.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());
        process_0 = getEntity(process_0.getGuid());
        table_0 = getEntity(table_0.getGuid());
        table_1 = getEntity(table_1.getGuid());

        children = (List) table_0.getRelationshipAttribute(OUTPUTS_FROM_P);
        assertNotNull(children);
        assertEquals(2, children.size());
        children.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(process_0.getGuid());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(process_1.getGuid());

        children = (List) table_1.getRelationshipAttribute(OUTPUTS_FROM_P);
        assertNotNull(children);
        assertEquals(2, children.size());
        children.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(process_0.getGuid());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(process_1.getGuid());

        children = (List) process_0.getRelationshipAttribute(OUTPUTS);
        assertNotNull(children);
        assertEquals(2, children.size());
        children.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(table_0.getGuid());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(table_1.getGuid());

        children = (List) process_1.getRelationshipAttribute(OUTPUTS);
        assertNotNull(children);
        assertEquals(2, children.size());
        children.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(table_0.getGuid());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(table_1.getGuid());



        //----------------------

        AtlasEntity table_2, table_3, process_2, process_3;

        table_2 = createCustomEntity(TYPE_TABLE, "table_2");
        table_3 = createCustomEntity(TYPE_TABLE, "table_3");

        extInfo = getAtlasEntity(TYPE_PROCESS, "process_2");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid(), table_3.getGuid()));

        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_2 = getEntity(response.getCreatedEntities().get(0).getGuid());
        table_2 = getEntity(table_2.getGuid());
        table_3 = getEntity(table_3.getGuid());

        children = (List) table_2.getRelationshipAttribute(INPUTS_TO_P);
        assertNotNull(children);
        assertEquals(1, children.size());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(process_2.getGuid());

        children = (List) table_3.getRelationshipAttribute(INPUTS_TO_P);
        assertNotNull(children);
        assertEquals(1, children.size());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(process_2.getGuid());

        children = (List) process_2.getRelationshipAttribute(INPUTS);
        assertNotNull(children);
        assertEquals(2, children.size());
        children.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(table_2.getGuid());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(table_1.getGuid());


        /*
        2.
        p0  -->  t0, t1

        p1  -->  t0, t1
        * */
        extInfo = getAtlasEntity(TYPE_PROCESS, "process_3");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid(), table_3.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_3 = getEntity(response.getCreatedEntities().get(0).getGuid());
        process_2 = getEntity(process_2.getGuid());
        table_2 = getEntity(table_2.getGuid());
        table_3 = getEntity(table_3.getGuid());

        children = (List) table_2.getRelationshipAttribute(INPUTS_TO_P);
        assertNotNull(children);
        assertEquals(2, children.size());
        children.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(process_2.getGuid());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(process_3.getGuid());

        children = (List) table_2.getRelationshipAttribute(INPUTS_TO_P);
        assertNotNull(children);
        assertEquals(2, children.size());
        children.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(process_2.getGuid());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(process_3.getGuid());

        children = (List) process_2.getRelationshipAttribute(INPUTS);
        assertNotNull(children);
        assertEquals(2, children.size());
        children.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(table_2.getGuid());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(table_3.getGuid());

        children = (List) process_3.getRelationshipAttribute(INPUTS);
        assertNotNull(children);
        assertEquals(2, children.size());
        children.stream().forEach(x -> assertTrue("ACTIVE".equals(x.get("relationshipStatus"))));
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(table_3.getGuid());
        children.stream().map(x -> x.get("guid")).collect(Collectors.toSet()).contains(table_3.getGuid());


        LOG.info("<< testChildrenRelationWithSetToSetRelationshipDef");
    }

    private static void testDeleteCategory() throws Exception {
        LOG.info(">> testDeleteCategory");

        AtlasEntity glossary_0, cat_00_updated, cat_0_updated, cat_1_updated, cat_2_updated;
        Map parentCat;


        glossary_0 = createGlossary();
        String gloGUID = glossary_0.getGuid();
        String gloQName = getQualifiedName(glossary_0);

        /*
        * cat_00  -->  cat_0  -->  cat_1  -->  cat_2
        *                                 -->  cat_2
        * */
        AtlasEntity cat_00 = createCategory(gloGUID, "cat_00", null,  null);
        AtlasEntity cat_0 = createCategory(gloGUID, "cat_0", cat_00.getGuid(),  null);
        AtlasEntity cat_1 = createCategory(gloGUID, "cat_1", cat_0.getGuid(), null);
        AtlasEntity cat_2 = createCategory(gloGUID, "cat_2", cat_1.getGuid(), null);
        AtlasEntity cat_3 = createCategory(gloGUID, "cat_3", cat_1.getGuid(), null);

        Thread.sleep(2000);
        cat_00_updated = getEntity(cat_00.getGuid());
        cat_0_updated = getEntity(cat_0.getGuid());
        cat_1_updated = getEntity(cat_1.getGuid());

        verifyESGlossary(cat_00.getGuid(), gloQName);
        verifyESParentCatAndGlossary(cat_0.getGuid(), getQualifiedName(cat_00_updated), gloQName);
        verifyESParentCatAndGlossary(cat_1.getGuid(), getQualifiedName(cat_0_updated), gloQName);
        verifyESParentCatAndGlossary(cat_2.getGuid(), getQualifiedName(cat_1_updated), gloQName);
        verifyESParentCatAndGlossary(cat_3.getGuid(), getQualifiedName(cat_1_updated), gloQName);

        //delete cat_1
        deleteEntities(Collections.singletonList(cat_1_updated.getGuid()));

        boolean failed = false;
        try {
            getEntity(cat_1_updated.getGuid());
        } catch (AtlasServiceException exception) {
            assertEquals(exception.getStatus().getStatusCode(),404);
            assertTrue(exception.getMessage().contains("ATLAS-404-00-005"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        Thread.sleep(2000);
        cat_2_updated = getEntity(cat_2.getGuid());

        parentCat = getParentRelationshipAttribute(cat_2_updated);
        assertNull(parentCat);

        verifyESGlossary(cat_00.getGuid(), gloQName);
        verifyESParentCatAndGlossary(cat_0.getGuid(), getQualifiedName(cat_00_updated), gloQName);
        verifyESGlossary(cat_2.getGuid(), gloQName);
        verifyESGlossary(cat_3.getGuid(), gloQName);

        LOG.info("<< testDeleteCategory");
    }

    private static void verifyESParentCatAndGlossary(String catGuid, String expectedParentCatQNAme, String expectedGloQName) {
        SearchHit[] searchHit = ESUtils.searchWithGuid(catGuid).getHits().getHits();
        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            assertNotNull(sourceAsMap.get("__glossary"));
            String qName = (String) sourceAsMap.get("__glossary");
            assertEquals(expectedGloQName, qName);

            assertNotNull(sourceAsMap.get("__parentCategory"));
            qName = (String) sourceAsMap.get("__parentCategory");
            assertEquals(expectedParentCatQNAme, qName);
        }
    }

    private static void verifyESCategoriesAndGlossary(String termGuid, String expectedGloQName, String... expectedCatQNAmes) {
        SearchHit[] searchHit = ESUtils.searchWithGuid(termGuid).getHits().getHits();
        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            assertNotNull(sourceAsMap.get("__glossary"));
            String qName = (String) sourceAsMap.get("__glossary");
            assertEquals(expectedGloQName, qName);

            assertNotNull(sourceAsMap.get("__categories"));
            List<String> actualCategories = (List<String>) sourceAsMap.get("__categories");
            LOG.info("expected {}", expectedCatQNAmes);
            LOG.info("actual   {}", actualCategories);
            assertEquals(expectedCatQNAmes.length, actualCategories.size());
            for (String q : expectedCatQNAmes) {
                assertTrue(actualCategories.contains(q));
            }
        }
    }

}
