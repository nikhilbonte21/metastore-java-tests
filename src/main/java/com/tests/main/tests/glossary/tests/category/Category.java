package com.tests.main.tests.glossary.tests.category;


import org.apache.atlas.AtlasErrorCode;

import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static com.tests.main.utils.TestUtil.*;
import static org.junit.Assert.*;

@Deprecated
public class Category {
    private static final Logger LOG = LoggerFactory.getLogger(Category.class);

    private static final int ALREADY_EXIST_STATUS_CODE = AtlasErrorCode.GLOSSARY_CATEGORY_ALREADY_EXISTS.getHttpCode().getStatusCode();
    private static final String ALREADY_EXIST_ERROR_CODE = AtlasErrorCode.GLOSSARY_CATEGORY_ALREADY_EXISTS.getErrorCode();

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        LOG.info("Running Category tests");

        long start = System.currentTimeMillis();
        try {
            /*testUpdateCategory();
            testUpdateParent();
            testCreateDupCat();
            testUpdateDupCat();
            changeAnotherAnchorNotAllowed();
            changeParentInAnotherAnchorNotAllowed();*/

            //TODO
            //changeCatParent();
        } catch (Exception e){
            throw e;
        } finally {
            cleanUpAll();
            LOG.info("Completed running Category tests, took {} seconds", (System.currentTimeMillis() - start) / 1000 );
        }
    }

    /*private static void testUpdateCategory() throws Exception {
        LOG.info(">> testUpdateCategory");
        AtlasGlossary glossary = createGlossary(getGlossaryModel(getRandomName()));

        AtlasGlossaryCategory category_0 = createCategory(getCategoryModel(glossary.getGuid()));
        AtlasGlossaryCategory category_1 = createCategory(getCategoryModelWithParent(glossary.getGuid(), getCategoryParentModel(category_0)));

        AtlasGlossaryCategory toUpdate = getCategory(category_1.getGuid());
        assertNull(toUpdate.getAdditionalAttributes());
        toUpdate.setAdditionalAttributes(Collections.singletonMap("key_0", "value_0"));
        updateCategory(category_1.getGuid(), toUpdate);

        AtlasGlossaryCategory updatedCat = getCategory(category_1.getGuid());
        assertEquals(updatedCat.getQualifiedName(), getNanoid(category_0.getQualifiedName()) + "." + getNanoid(category_1.getQualifiedName()) + "@" + glossary.getQualifiedName());;
        assertNotNull(updatedCat.getAdditionalAttributes());
        assertEquals(updatedCat.getAdditionalAttributes().size(), 1);

        LOG.info("<< testUpdateCategory");
    }

    private static void testUpdateParent() throws Exception {
        LOG.info(">> testUpdateParent");
        AtlasGlossary glossary = createGlossary(getGlossaryModel(getRandomName()));

        AtlasGlossaryCategory category_0 = createCategory(getCategoryModel(glossary.getGuid()));
        AtlasGlossaryCategory category_1 = createCategory(getCategoryModelWithParent(glossary.getGuid(), getCategoryParentModel(category_0)));
        AtlasGlossaryCategory category_2 = createCategory(getCategoryModelWithParent(glossary.getGuid(), getCategoryParentModel(category_1)));
        AtlasGlossaryCategory category_3 = createCategory(getCategoryModelWithParent(glossary.getGuid(), getCategoryParentModel(category_2)));
        AtlasGlossaryCategory category_4 = createCategory(getCategoryModelWithParent(glossary.getGuid(), getCategoryParentModel(category_3)));

        String category_0_nid = getNanoid(category_0.getQualifiedName());
        String category_1_nid = getNanoid(category_1.getQualifiedName());
        String category_2_nid = getNanoid(category_2.getQualifiedName());
        String category_3_nid = getNanoid(category_3.getQualifiedName());
        String category_4_nid = getNanoid(category_4.getQualifiedName());

        assertEquals(category_0.getQualifiedName(), category_0_nid + "@" + glossary.getQualifiedName());
        assertEquals(category_1.getQualifiedName(), category_0_nid + "." + category_1_nid + "@" + glossary.getQualifiedName());
        assertEquals(category_2.getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_2_nid + "@" + glossary.getQualifiedName());
        assertEquals(category_3.getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_2_nid + "." + category_3_nid + "@" + glossary.getQualifiedName());
        assertEquals(category_4.getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_2_nid + "." + category_3_nid + "." + category_4_nid + "@" + glossary.getQualifiedName());

        assertEquals(category_1.getParentCategory().getCategoryGuid(), category_0.getGuid());
        assertEquals(category_2.getParentCategory().getCategoryGuid(), category_1.getGuid());
        assertEquals(category_3.getParentCategory().getCategoryGuid(), category_2.getGuid());
        assertEquals(category_4.getParentCategory().getCategoryGuid(), category_3.getGuid());

        //changing category_3's parent from category_2 to category_0
        AtlasGlossaryCategory category = getCategory(category_3.getGuid());
        category.setParentCategory(getCategoryParentModel(category_0));
        AtlasGlossaryCategory updatedCategory = updateCategory(category.getGuid(), category);
        assertEquals(updatedCategory.getQualifiedName(), category_0_nid + "." + category_3_nid + "@" + glossary.getQualifiedName());
        assertEquals(getCategory(category_4.getGuid()).getQualifiedName(), category_0_nid + "." + category_3_nid + "." + category_4_nid + "@" + glossary.getQualifiedName());

        //changing category_3's parent from category_0 to category_1
        category = getCategory(category_3.getGuid());
        category.setParentCategory(getCategoryParentModel(category_1));
        updatedCategory = updateCategory(category.getGuid(), category);
        assertEquals(updatedCategory.getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_3_nid + "@" + glossary.getQualifiedName());
        assertEquals(getCategory(category_4.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_3_nid + "." + category_4_nid + "@" + glossary.getQualifiedName());

        //change category_2's parent from category_1 to category_0
        category = getCategory(category_2.getGuid());
        category.setParentCategory(getCategoryParentModel(category_0));
        updatedCategory = updateCategory(category.getGuid(), category);
        assertEquals(updatedCategory.getQualifiedName(), category_0_nid + "." + category_2_nid + "@" + glossary.getQualifiedName());

        LOG.info("<< testUpdateParent");
    }

    private static void changeAnotherAnchorNotAllowed() throws Exception {
        LOG.info(">> changeAnotherAnchorNotAllowed");
        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel(getRandomName()));
        AtlasGlossary glossary_1 = createGlossary(getGlossaryModel(getRandomName()));

        AtlasGlossaryCategory category_0 = createCategory(getCategoryModel(glossary_0.getGuid()));
        AtlasGlossaryCategory category_1 = createCategory(getCategoryModelWithParent(glossary_0.getGuid(), getCategoryParentModel(category_0)));

        AtlasGlossaryCategory category_2 = createCategory(getCategoryModel(glossary_1.getGuid()));
        AtlasGlossaryCategory category_3 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_2)));

        AtlasGlossaryCategory category = getCategory(category_3.getGuid());
        category.setAnchor(getGlossaryHeader(glossary_0.getGuid()));
        category.setParentCategory(getCategoryParentModel(category_1));

        boolean failed = false;
        try {
            updateCategory(category.getGuid(), category);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-400-00-0010"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }
        LOG.info(">> changeAnotherAnchorNotAllowed");
    }

    private static void testCreateDupCat() throws Exception {
        LOG.info(">> testCreateDupCat");

        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel(getRandomName()));

        String catName = getRandomName();
        AtlasGlossaryCategory category_0 = createCategory(getCategoryModel(catName, glossary_0.getGuid()));
        AtlasGlossaryCategory category_1 = createCategory(getCategoryModelWithParent(glossary_0.getGuid(), getCategoryParentModel(category_0)));

        boolean failed = false;
        try {
            Thread.sleep(2000);
            //do not allow creating a category with same name at same level
            createCategory(getCategoryModel(catName, glossary_0.getGuid()));
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), ALREADY_EXIST_STATUS_CODE);
            assertTrue(exception.getMessage().contains(ALREADY_EXIST_ERROR_CODE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        AtlasGlossaryCategory category_2 = createCategory(getCategoryModelWithParent(glossary_0.getGuid(), getCategoryParentModel(category_1)));

        AtlasGlossaryCategory category_3 = createCategory(getCategoryModel(category_1.getName(), glossary_0.getGuid()));
        AtlasGlossaryCategory category_4 = createCategory(getCategoryModelWithParent(category_2.getName(), glossary_0.getGuid(), getCategoryParentModel(category_3)));
        AtlasGlossaryCategory category_5 = createCategory(getCategoryModelWithParent(category_0.getName(), glossary_0.getGuid(), getCategoryParentModel(category_4)));

        LOG.info("<< testCreateDupCat");
    }

    private static void testUpdateDupCat() throws Exception {
        LOG.info(">> testUpdateDupCat");

        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel(getRandomName()));

        String catName = getRandomName();
        AtlasGlossaryCategory category_a_0 = createCategory(getCategoryModel(catName, glossary_0.getGuid()));
        AtlasGlossaryCategory category_a_1 = createCategory(getCategoryModelWithParent(glossary_0.getGuid(), getCategoryParentModel(category_a_0)));

        boolean failed = false;
        try {
            Thread.sleep(2000);

            //do not allow updating a category with same name at same level
            AtlasGlossaryCategory toUpdate = getCategory(category_a_1.getGuid());
            toUpdate.setName(catName);

            //removing anchor will try to move this cat to 1st level & it should be allowed as we are passing same name to update
            toUpdate.setParentCategory(null);
            updateCategory(category_a_1.getGuid(), toUpdate);

        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), ALREADY_EXIST_STATUS_CODE);
            assertTrue(exception.getMessage().contains(ALREADY_EXIST_ERROR_CODE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        //chaning parent to upper level
        catName = getRandomName();
        AtlasGlossaryCategory category_a_2 = createCategory(getCategoryModelWithParent(catName, glossary_0.getGuid(), getCategoryParentModel(category_a_1)));
        AtlasGlossaryCategory category_a_3 = createCategory(getCategoryModelWithParent(glossary_0.getGuid(), getCategoryParentModel(category_a_2)));

        AtlasGlossaryCategory toUpdate = getCategory(category_a_3.getGuid());
        toUpdate.setName(category_a_1.getName());
        toUpdate.setParentCategory(getCategoryParentModel(category_a_0));

        failed = false;
        try {
            updateCategory(category_a_3.getGuid(), toUpdate);
        } catch (Exception exception) {
           //assertEquals(exception.getStatus().getStatusCode(), ALREADY_EXIST_STATUS_CODE);
            assertTrue(exception.getMessage().contains(ALREADY_EXIST_ERROR_CODE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }
        LOG.info("<< testUpdateDupCat");
    }

    private static void changeParentInAnotherAnchorNotAllowed() throws Exception {
        LOG.info(">> changeParentInAnotherAnchorNotAllowed");
        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel(getRandomName()));
        AtlasGlossary glossary_1 = createGlossary(getGlossaryModel(getRandomName()));

        AtlasGlossaryCategory category_0 = createCategory(getCategoryModel(glossary_0.getGuid()));
        AtlasGlossaryCategory category_1 = createCategory(getCategoryModelWithParent(glossary_0.getGuid(), getCategoryParentModel(category_0)));

        AtlasGlossaryCategory category_2 = createCategory(getCategoryModel(glossary_1.getGuid()));
        AtlasGlossaryCategory category_3 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_2)));

        AtlasGlossaryCategory category = getCategory(category_3.getGuid());
        category.setAnchor(getGlossaryHeader(glossary_0.getGuid()));
        category.setParentCategory(getCategoryParentModel(category_1));

        boolean failed = false;
        try {
            updateCategory(category.getGuid(), category);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-400-00-0010"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }
        LOG.info(">> changeParentInAnotherAnchorNotAllowed");
    }

    private static void changeCatParent() throws Exception {
        LOG.info(">> changeCatParent");
        AtlasGlossary glossary = createGlossary(getGlossaryModel(getRandomName()));

        AtlasGlossaryCategory category_0 = createCategory(getCategoryModel(glossary.getGuid()));
        AtlasGlossaryCategory category_1 = createCategory(getCategoryModelWithParent(glossary.getGuid(), getCategoryParentModel(category_0)));
        AtlasGlossaryCategory category_2 = createCategory(getCategoryModelWithParent(glossary.getGuid(), getCategoryParentModel(category_1)));
        AtlasGlossaryCategory category_3 = createCategory(getCategoryModelWithParent(glossary.getGuid(), getCategoryParentModel(category_2)));
        AtlasGlossaryCategory category_4 = createCategory(getCategoryModelWithParent(glossary.getGuid(), getCategoryParentModel(category_3)));

        String category_0_nid = getNanoid(category_0.getQualifiedName());
        String category_1_nid = getNanoid(category_1.getQualifiedName());
        String category_2_nid = getNanoid(category_2.getQualifiedName());
        String category_3_nid = getNanoid(category_3.getQualifiedName());
        String category_4_nid = getNanoid(category_4.getQualifiedName());

        assertEquals(category_0.getQualifiedName(), category_0_nid + "@" + glossary.getQualifiedName());
        assertEquals(category_1.getQualifiedName(), category_0_nid + "." + category_1_nid + "@" + glossary.getQualifiedName());
        assertEquals(category_2.getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_2_nid + "@" + glossary.getQualifiedName());
        assertEquals(category_3.getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_2_nid + "." + category_3_nid + "@" + glossary.getQualifiedName());
        assertEquals(category_4.getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_2_nid + "." + category_3_nid + "." + category_4_nid + "@" + glossary.getQualifiedName());

        assertEquals(category_1.getParentCategory().getCategoryGuid(), category_0.getGuid());
        assertEquals(category_2.getParentCategory().getCategoryGuid(), category_1.getGuid());
        assertEquals(category_3.getParentCategory().getCategoryGuid(), category_2.getGuid());
        assertEquals(category_4.getParentCategory().getCategoryGuid(), category_3.getGuid());

        //changing category_3's parent from category_2 to category_0
        AtlasGlossaryCategory category = getCategory(category_3.getGuid());
        category.setParentCategory(getCategoryParentModel(category_0));
        AtlasGlossaryCategory updatedCategory = updateCategory(category.getGuid(), category);
        assertEquals(updatedCategory.getQualifiedName(), category_0_nid + "." + category_3_nid + "@" + glossary.getQualifiedName());
        assertEquals(getCategory(category_4.getGuid()).getQualifiedName(), category_0_nid + "." + category_3_nid + "." + category_4_nid + "@" + glossary.getQualifiedName());

        //changing category_3's parent from category_0 to category_1
        category = getCategory(category_3.getGuid());
        category.setParentCategory(getCategoryParentModel(category_1));
        updatedCategory = updateCategory(category.getGuid(), category);
        assertEquals(updatedCategory.getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_3_nid + "@" + glossary.getQualifiedName());
        assertEquals(getCategory(category_4.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_3_nid + "." + category_4_nid + "@" + glossary.getQualifiedName());

        //change category_2's parent from category_1 to category_0
        category = getCategory(category_2.getGuid());
        category.setParentCategory(getCategoryParentModel(category_0));
        updatedCategory = updateCategory(category.getGuid(), category);
        assertEquals(updatedCategory.getQualifiedName(), category_0_nid + "." + category_2_nid + "@" + glossary.getQualifiedName());

        LOG.info("<< changeCatParent");
    }

    *//*NOTE

    This will fail as we are not supporting changing of glossary for Terms & Categories
    But, https://linear.app/atlanproduct/issue/META-2694/use-uuid-in-qualifiedname-instead-of-name
    Feature has required changes for changing of acnhor & reevaluating qualifiedName for both Terms & Categories
    If chaging anchor is allowed, in that case this test must poss

    *//*
    private void changeAnchor() throws Exception {
        LOG.info(">> changeAnchor");
        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel(getRandomName()));
        AtlasGlossary glossary_1 = createGlossary(getGlossaryModel(getRandomName()));

        AtlasGlossaryCategory category_0 = createCategory(getCategoryModel(glossary_0.getGuid()));
        AtlasGlossaryCategory category_1 = createCategory(getCategoryModelWithParent(glossary_0.getGuid(), getCategoryParentModel(category_0)));

        AtlasGlossaryCategory category_2 = createCategory(getCategoryModel(glossary_1.getGuid()));
        AtlasGlossaryCategory category_3 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_2)));
        AtlasGlossaryCategory category_4 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_3)));

        String category_0_nid = getNanoid(category_0.getQualifiedName());
        String category_1_nid = getNanoid(category_1.getQualifiedName());
        String category_2_nid = getNanoid(category_2.getQualifiedName());
        String category_3_nid = getNanoid(category_3.getQualifiedName());
        String category_4_nid = getNanoid(category_4.getQualifiedName());

        assertEquals(category_3.getQualifiedName(), category_2_nid + "." + category_3_nid + "@" + glossary_1.getQualifiedName());
        assertEquals(category_4.getQualifiedName(), category_2_nid + "." + category_3_nid + "." + category_4_nid +  "@" + glossary_1.getQualifiedName());

        //change category_3's anchor from glossary_1 to glossary_0
        AtlasGlossaryCategory category = getCategory(category_3.getGuid());
        category.setParentCategory(null);
        category.setAnchor(getGlossaryHeader(glossary_0.getGuid()));
        updateCategory(category.getGuid(), category);

        assertEquals(getCategory(category_3.getGuid()).getQualifiedName(), category_3_nid + "@" + glossary_0.getQualifiedName());
        assertEquals(getCategory(category_4.getGuid()).getQualifiedName(), category_3_nid + "." + category_4_nid +  "@" + glossary_0.getQualifiedName());

        assertEquals(getCategory(category_0.getGuid()).getQualifiedName(), category_0_nid + "@" + glossary_0.getQualifiedName());
        assertEquals(getCategory(category_1.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid +  "@" + glossary_0.getQualifiedName());


        LOG.info(">> changeAnchor");
    }

    *//*NOTE

    This will fail as we are not supporting changing of glossary for Terms & Categories
    But, https://linear.app/atlanproduct/issue/META-2694/use-uuid-in-qualifiedname-instead-of-name
    Feature has required changes for changing of acnhor & reevaluating qualifiedName for both Terms & Categories
    If changing anchor is allowed, in that case this test must pass

    As of now This case fails:
    As category_4, category_5 & category_6 's qualifiedName is not getting properly evaluated due the category_3's glossary change
    If changing anchor is allowed, in that case this needs to be fixed
     *//*
    private void changeParentInAnotherAnchor() throws Exception {
        LOG.info(">> changeParentInAnotherAnchor");
        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel(getRandomName()));
        AtlasGlossary glossary_1 = createGlossary(getGlossaryModel(getRandomName()));

        AtlasGlossaryCategory category_0 = createCategory(getCategoryModel(glossary_0.getGuid()));
        AtlasGlossaryCategory category_1 = createCategory(getCategoryModelWithParent(glossary_0.getGuid(), getCategoryParentModel(category_0)));
        AtlasGlossaryCategory category_1_1 = createCategory(getCategoryModelWithParent(glossary_0.getGuid(), getCategoryParentModel(category_1)));

        AtlasGlossaryCategory category_2 = createCategory(getCategoryModel(glossary_1.getGuid()));
        AtlasGlossaryCategory category_3 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_2)));
        AtlasGlossaryCategory category_4 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_3)));
        AtlasGlossaryCategory category_5 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_4)));
        AtlasGlossaryCategory category_6 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_5)));

        String category_0_nid = getNanoid(category_0.getQualifiedName());
        String category_1_nid = getNanoid(category_1.getQualifiedName());
        String category_2_nid = getNanoid(category_2.getQualifiedName());
        String category_3_nid = getNanoid(category_3.getQualifiedName());
        String category_4_nid = getNanoid(category_4.getQualifiedName());
        String category_5_nid = getNanoid(category_5.getQualifiedName());
        String category_6_nid = getNanoid(category_6.getQualifiedName());
        String category_1_1_nid = getNanoid(category_1_1.getQualifiedName());

        assertEquals(category_1_1.getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_1_1_nid + "@" + glossary_0.getQualifiedName());
        assertEquals(category_3.getQualifiedName(), category_2_nid + "." + category_3_nid + "@" + glossary_1.getQualifiedName());
        assertEquals(category_4.getQualifiedName(), category_2_nid + "." + category_3_nid + "."+ category_4_nid + "@" + glossary_1.getQualifiedName());
        assertEquals(category_5.getQualifiedName(), category_2_nid + "." + category_3_nid + "."+ category_4_nid + "." + category_5_nid + "@" + glossary_1.getQualifiedName());
        assertEquals(category_6.getQualifiedName(), category_2_nid + "." + category_3_nid + "."+ category_4_nid + "." + category_5_nid + "." + category_6_nid + "@" + glossary_1.getQualifiedName());

*//*        LOG.info("glossary_0: nanoId {}, guid {}, name {}", getNanoid(glossary_0.getQualifiedName()), glossary_0.getGuid(), glossary_0.getName());
        LOG.info("glossary_1: nanoId {}, guid {}, name {}", getNanoid(glossary_1.getQualifiedName()), glossary_1.getGuid(), glossary_1.getName());

        LOG.info("category_0: nanoId {}, guid {}, name {}", category_0_nid, category_0.getGuid(), category_0.getName());
        LOG.info("category_1: nanoId {}, guid {}, name {}", category_1_nid, category_1.getGuid(), category_1.getName());
        LOG.info("category_1_1: nanoId {}, guid {}, name {}", category_1_1_nid, category_1_1.getGuid(), category_1_1.getName());
        LOG.info("category_3: nanoId {}, guid {}, name {}", category_3_nid, category_3.getGuid(), category_3.getName());
        LOG.info("category_4: nanoId {}, guid {}, name {}", category_4_nid, category_4.getGuid(), category_4.getName());
        LOG.info("category_5: nanoId {}, guid {}, name {}", category_5_nid, category_5.getGuid(), category_5.getName());
        LOG.info("category_6: nanoId {}, guid {}, name {}", category_6_nid, category_6.getGuid(), category_6.getName());*//*

        //change category_3's parent from glossary_1.category_2 to glossary_0.category_1
        *//*  Before:
            glossary_0: category_0 -> category_1 -> category_1_1
            glossary_1: category_2 -> category_3 -> category_4 -> category_5 -> category_6

            After:
            glossary_0: category_0 -> category_1 -> category_3 -> category_4 -> category_5 -> category_6
            glossary_1: category_2
         *//*
        AtlasGlossaryCategory category = getCategory(category_3.getGuid());
        category.setAnchor(getGlossaryHeader(glossary_0.getGuid()));
        category.setParentCategory(getCategoryParentModel(category_1));
        updateCategory(category.getGuid(), category);

        assertEquals(getGlossary(glossary_0.getGuid()).getCategories().size(), 7);
        assertEquals(getGlossary(glossary_1.getGuid()).getCategories().size(), 1);

        assertEquals(getCategory(category_1_1.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_1_1_nid + "@" + glossary_0.getQualifiedName());
        assertEquals(getCategory(category_0.getGuid()).getQualifiedName(), category_0_nid + "@" + glossary_0.getQualifiedName());
        assertEquals(getCategory(category_1.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid +  "@" + glossary_0.getQualifiedName());


        assertEquals(getCategory(category_2.getGuid()).getQualifiedName(), category_2_nid + "@" + glossary_1.getQualifiedName());

        assertEquals(getCategory(category_3.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_3_nid + "@" + glossary_0.getQualifiedName());

        assertEquals(getCategory(category_4.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_3_nid + "." + category_4_nid +  "@" + glossary_0.getQualifiedName());
        assertEquals(getCategory(category_5.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_3_nid + "." + category_4_nid + "." + category_5_nid + "@" + glossary_0.getQualifiedName());
        assertEquals(getCategory(category_6.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_3_nid + "." + category_4_nid + "." + category_5_nid + "." + category_6_nid + "@" + glossary_0.getQualifiedName());


        LOG.info("<< changeParentInAnotherAnchor");
    }*/


    private static String getNanoid(String qualifiedName){
        String[] at = qualifiedName.split("@");
        String[] dot = at[0].split("\\.");

        return dot[dot.length-1];
    }
}