package com.tests.main.tests.glossary.tests.category;


import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static com.tests.main.utils.TestUtil.ES_CATEGORIES;
import static com.tests.main.utils.TestUtil.ES_GLOSSARY;
import static com.tests.main.utils.TestUtil.ES_PARENT_CAT;
import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.REL_ANCHOR;
import static com.tests.main.utils.TestUtil.REL_CATEGORIES;
import static com.tests.main.utils.TestUtil.REL_TERMS;
import static com.tests.main.utils.TestUtil.REL_CHILDREN_CATS;
import static com.tests.main.utils.TestUtil.REL_PARENT_CAT;
import static com.tests.main.utils.TestUtil.TYPE_CATEGORY;
import static com.tests.main.utils.TestUtil.TYPE_GLOSSARY;
import static com.tests.main.utils.TestUtil.TYPE_TERM;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createAndGetEntity;
import static com.tests.main.utils.TestUtil.createGlossary;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getNanoid;
import static com.tests.main.utils.TestUtil.mapOf;
import static com.tests.main.utils.TestUtil.updateAndGetEntity;
import static com.tests.main.utils.TestUtil.verifyESInLoop;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class MoveCategoriesAcrossGlossaries implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(MoveCategoriesAcrossGlossaries.class);

    private static final String DUPLICATE_CATEGORY_ERROR_CODE = "ATLAS-409-00-00A";
    private static final String DUPLICATE_CATEGORY_ERROR_MESSAGE = "Glossary category with name %s already exists on this level";

    private static final String PARENT_CAT_FROM_OTHER_GLOSSARY_ERROR_CODE = "ATLAS-400-00-0015";
    private static final String PARENT_CAT_FROM_OTHER_GLOSSARY_ERROR_MESSAGE = "Parent category from another Anchor(glossary) not supported";

    public static void main(String[] args) throws Exception {
        try {
            new MoveCategoriesAcrossGlossaries().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running MoveCategoriesAcrossGlossaries tests");

        long start = System.currentTimeMillis();
        try {
            createCategoryDuplicateName();
            updateCategoryDuplicateName();
            createCategoryWithParentCategoryFromDifferentGlossary();
            moveFromRootToRootLevel();
            moveWithDuplicateNameInDifferentLevel();
            moveWithDuplicateNameAtSameLevel();
            moveHeavyCategory();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running MoveCategoriesAcrossGlossaries tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void createCategoryDuplicateName() throws Exception {
        LOG.info(">> createCategoryDuplicateName");

        AtlasEntity glossary_0 = createGlossary();
        AtlasEntity glossary_1 = createGlossary();

        AtlasEntity cat_0 = getAtlasEntity(TYPE_CATEGORY, "cat_0");
        cat_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_0 = createAndGetEntity(cat_0);

        AtlasEntity cat_1 = getAtlasEntity(TYPE_CATEGORY, "cat_1");
        cat_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_1.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null));
        cat_1 = createAndGetEntity(cat_1);

        AtlasEntity cat_2 = getAtlasEntity(TYPE_CATEGORY, "cat_2");
        cat_2.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_2.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null));
        cat_2 = createAndGetEntity(cat_2);

        String glossary_0_Qname = (String) glossary_0.getAttribute(QUALIFIED_NAME);
        String glossary_1_Qname = (String) glossary_1.getAttribute(QUALIFIED_NAME);
        String cat_0_Qname = (String) cat_0.getAttribute(QUALIFIED_NAME);
        String cat_1_Qname = (String) cat_1.getAttribute(QUALIFIED_NAME);
        String cat_2_Qname = (String) cat_2.getAttribute(QUALIFIED_NAME);

        assertEquals(cat_0_Qname, getNanoid(cat_0_Qname) + "@" + glossary_0_Qname);
        verifyESInLoop(cat_0.getGuid(), mapOf(ES_GLOSSARY, glossary_0_Qname));
        assertEquals(cat_1_Qname, getNanoid(cat_1_Qname) + "@" + glossary_0_Qname);
        verifyESInLoop(cat_1.getGuid(), mapOf(ES_GLOSSARY, glossary_0_Qname, ES_PARENT_CAT, cat_0_Qname));
        assertEquals(cat_2_Qname, getNanoid(cat_2_Qname) + "@" + glossary_0_Qname);
        verifyESInLoop(cat_2.getGuid(), mapOf(ES_GLOSSARY, glossary_0_Qname, ES_PARENT_CAT, cat_1_Qname));

        LOG.info("glossary_0, glossary_1 => {}, {}", glossary_0_Qname, glossary_1_Qname);
        LOG.info("catQname : {}", cat_0);

        String cat_0_name = (String) cat_0.getAttribute(NAME);
        String cat_1_name = (String) cat_1.getAttribute(NAME);
        String cat_2_name = (String) cat_2.getAttribute(NAME);

        /*
        *
        * glossary_0 -> cat_0 -> cat_1 -> cat_2
        * glossary_1
        *
        * create new category with name cat_0 in glossary_0
        * should fail
        * */
        AtlasEntity cat_duplicate = getAtlasEntity(TYPE_CATEGORY, cat_0_name);
        cat_duplicate.setAttribute(NAME, cat_0_name);
        cat_duplicate.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));

        boolean failed = false;
        try {
            cat_duplicate = createAndGetEntity(cat_duplicate);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains(DUPLICATE_CATEGORY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(String.format(DUPLICATE_CATEGORY_ERROR_MESSAGE, cat_0_name)));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        /*
         *
         * glossary_0 -> cat_0 -> cat_1 -> cat_2
         * glossary_1
         *
         * create new category with name cat_1 in glossary_0.cat_0
         * should fail
         * */

        cat_duplicate.setAttribute(NAME, cat_1_name);
        cat_duplicate.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_duplicate.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_GLOSSARY, cat_0.getGuid(), null));

        failed = false;
        try {
            cat_duplicate = createAndGetEntity(cat_duplicate);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains(DUPLICATE_CATEGORY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(String.format(DUPLICATE_CATEGORY_ERROR_CODE, cat_1_name)));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        /*
         *
         * glossary_0 -> cat_0 -> cat_1 -> cat_2
         * glossary_1
         *
         * create new category with name cat_2 in glossary_0.cat_1
         * should fail
         * */

        cat_duplicate.setAttribute(NAME, cat_2_name);
        cat_duplicate.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_duplicate.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_GLOSSARY, cat_1.getGuid(), null));

        failed = false;
        try {
            cat_duplicate = createAndGetEntity(cat_duplicate);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains(DUPLICATE_CATEGORY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(String.format(DUPLICATE_CATEGORY_ERROR_CODE, cat_2_name)));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        /*
         *
         * glossary_0 -> cat_0 -> cat_1 -> cat_2
         * glossary_1
         *
         * create new category with name cat_0 in glossary_0.cat_1
         *
         * */

        AtlasEntity cat_3 = getAtlasEntity(TYPE_CATEGORY, cat_0_name);
        cat_3.setAttribute(NAME, cat_0_name);
        cat_3.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_3.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null));
        cat_3 = createAndGetEntity(cat_3);


        AtlasEntity cat_4 = getAtlasEntity(TYPE_CATEGORY, cat_0_name);
        cat_4.setAttribute(NAME, cat_0_name);
        cat_4.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_4.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_2.getGuid(), null));
        cat_4 = createAndGetEntity(cat_4);

        /////

        LOG.info(">> createCategoryDuplicateName");
    }

    private static void updateCategoryDuplicateName() throws Exception {
        LOG.info(">> updateCategoryDuplicateName");


        AtlasEntity glossary_0 = createGlossary();

        AtlasEntity cat_0 = getAtlasEntity(TYPE_CATEGORY, "cat_0");
        cat_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_0 = createAndGetEntity(cat_0);

        AtlasEntity cat_1 = getAtlasEntity(TYPE_CATEGORY, "cat_1");
        cat_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_1.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null));
        cat_1 = createAndGetEntity(cat_1);

        AtlasEntity cat_2 = getAtlasEntity(TYPE_CATEGORY, "cat_2");
        cat_2.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_2.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null));
        cat_2 = createAndGetEntity(cat_2);

        String glossary_0_Qname = (String) glossary_0.getAttribute(QUALIFIED_NAME);
        String cat_0_Qname = (String) cat_0.getAttribute(QUALIFIED_NAME);
        String cat_1_Qname = (String) cat_1.getAttribute(QUALIFIED_NAME);
        String cat_2_Qname = (String) cat_2.getAttribute(QUALIFIED_NAME);

        assertEquals(cat_0_Qname, getNanoid(cat_0_Qname) + "@" + glossary_0_Qname);
        verifyESInLoop(cat_0.getGuid(), mapOf(ES_GLOSSARY, glossary_0_Qname));
        assertEquals(cat_1_Qname, getNanoid(cat_1_Qname) + "@" + glossary_0_Qname);
        verifyESInLoop(cat_1.getGuid(), mapOf(ES_GLOSSARY, glossary_0_Qname, ES_PARENT_CAT, cat_0_Qname));
        assertEquals(cat_2_Qname, getNanoid(cat_2_Qname) + "@" + glossary_0_Qname);
        verifyESInLoop(cat_2.getGuid(), mapOf(ES_GLOSSARY, glossary_0_Qname, ES_PARENT_CAT, cat_0_Qname));

        LOG.info("glossary_0 => {}", glossary_0_Qname);

        String cat_0_name = (String) cat_0.getAttribute(NAME);
        String cat_1_name = (String) cat_1.getAttribute(NAME);
        String cat_2_name = (String) cat_2.getAttribute(NAME);


        // glossary_0 -> cat_0 -> cat_1, cat_2
        // update cat_1 to rename to cat_2, should fail

        cat_1.setAttribute(NAME, cat_2_name);
        boolean failed = false;
        try {
            Thread.sleep(3000);
            cat_1 = updateAndGetEntity(cat_1);
        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains(DUPLICATE_CATEGORY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(String.format(DUPLICATE_CATEGORY_ERROR_MESSAGE, cat_2_name)));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        // glossary_0 -> cat_0 -> cat_1, cat_2
        // update cat_1 to rename to cat_0, should pass

        cat_1.setAttribute(NAME, cat_0.getAttribute(NAME));
        cat_1 = updateAndGetEntity(cat_1);

        assertEquals( (String) cat_1.getAttribute(NAME), (String) cat_0.getAttribute(NAME));
        assertEquals(cat_1_Qname, getNanoid(cat_1_Qname) + "@" + glossary_0_Qname);
        verifyESInLoop(cat_1.getGuid(), mapOf(ES_GLOSSARY, glossary_0_Qname, ES_PARENT_CAT, cat_0_Qname));


        LOG.info(">> updateCategoryDuplicateName");
    }
    
    private static void createCategoryWithParentCategoryFromDifferentGlossary() throws Exception {
        LOG.info(">> createCategoryWithParentCategoryFromDifferentGlossary");

        AtlasEntity glossary_0 = createGlossary();
        AtlasEntity glossary_1 = createGlossary();

        AtlasEntity cat_0 = getAtlasEntity(TYPE_CATEGORY, "cat_0");
        cat_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_0 = createAndGetEntity(cat_0);

        AtlasEntity cat_1 = getAtlasEntity(TYPE_CATEGORY, "cat_1");
        cat_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_1.getGuid(), null));
        cat_1 = createAndGetEntity(cat_1);


        String glossary_0_Qname = (String) glossary_0.getAttribute(QUALIFIED_NAME);
        String glossary_1_Qname = (String) glossary_1.getAttribute(QUALIFIED_NAME);
        String cat_0_Qname = (String) cat_0.getAttribute(QUALIFIED_NAME);
        String cat_1_Qname = (String) cat_1.getAttribute(QUALIFIED_NAME);

        assertEquals(cat_0_Qname, getNanoid(cat_0_Qname) + "@" + glossary_0_Qname);
        verifyESInLoop(cat_0.getGuid(), mapOf(ES_GLOSSARY, glossary_0_Qname));
        assertEquals(cat_1_Qname, getNanoid(cat_1_Qname) + "@" + glossary_1_Qname);
        verifyESInLoop(cat_1.getGuid(), mapOf(ES_GLOSSARY, glossary_1_Qname));

        AtlasEntity cat_to_create = getAtlasEntity(TYPE_CATEGORY, "cat_1");

        /*
        * glossary_0 -> cat_0
        * glossary_1 -> cat_1
        *
        *
        * create cat_to_create with glossary_1 & cat_0
        *
        * */
        cat_to_create.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_1.getGuid(), null));
        cat_to_create.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null));

        boolean failed = false;
        try {
            cat_to_create = createAndGetEntity(cat_to_create);
        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains(PARENT_CAT_FROM_OTHER_GLOSSARY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(PARENT_CAT_FROM_OTHER_GLOSSARY_ERROR_MESSAGE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        /*
         * glossary_0 -> cat_0
         * glossary_1 -> cat_1
         *
         *
         * create cat_to_create with glossary_0 & cat_1
         *
         * */
        cat_to_create.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_to_create.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null));

        failed = false;
        try {
            cat_to_create = createAndGetEntity(cat_to_create);
        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains(PARENT_CAT_FROM_OTHER_GLOSSARY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(PARENT_CAT_FROM_OTHER_GLOSSARY_ERROR_MESSAGE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        /*
         * glossary_0 -> cat_0
         * glossary_1 -> cat_1
         *
         *
         * create cat_to_create with glossary_0 & cat_0
         *
         * */
        cat_to_create.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_to_create.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null));

        cat_to_create = createAndGetEntity(cat_to_create);

        String cat_to_move_Qname = (String) cat_to_create.getAttribute(QUALIFIED_NAME);

        assertEquals(cat_to_move_Qname, getNanoid(cat_to_move_Qname) + "@" + glossary_0_Qname);
        verifyESInLoop(cat_to_create.getGuid(), mapOf(ES_GLOSSARY, glossary_0_Qname));

        /*
         * glossary_0 -> cat_0 -> cat_to_create
         * glossary_1 -> cat_1
         *
         *
         * update cat_to_create with glossary_1 & cat_1
         *
         * */
        Thread.sleep(3000);
        flushCategoryRedundantRelations(cat_to_create);
        cat_to_create.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_1.getGuid(), null));
        cat_to_create.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null));

        cat_to_create = updateAndGetEntity(cat_to_create);

        cat_to_move_Qname = (String) cat_to_create.getAttribute(QUALIFIED_NAME);

        assertEquals(cat_to_move_Qname, getNanoid(cat_to_move_Qname) + "@" + glossary_1_Qname);
        verifyESInLoop(cat_to_create.getGuid(), mapOf(ES_GLOSSARY, glossary_1_Qname));


        LOG.info(">> createCategoryWithParentCategoryFromDifferentGlossary");
    }

    private static void moveFromRootToRootLevel() throws Exception {
        LOG.info(">> moveFromRootToRootLevel");

        AtlasEntity glossarySource = createGlossary();
        AtlasEntity glossaryTarget = createGlossary();

        AtlasEntity cat_0 = getAtlasEntity(TYPE_CATEGORY, "cat_0");
        cat_0.setRelationshipAttribute("anchor", new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));

        cat_0 = createAndGetEntity(cat_0);

        String glossarySourceQname = (String) glossarySource.getAttribute(QUALIFIED_NAME);
        String glossaryTargetQname = (String) glossaryTarget.getAttribute(QUALIFIED_NAME);
        String cat_0_Qname = (String) cat_0.getAttribute(QUALIFIED_NAME);

        LOG.info("glossarySourceQname, glossaryTargetQname => {}, {}", glossarySourceQname, glossaryTargetQname);
        LOG.info("catQname : {}", cat_0);

        assertEquals(cat_0_Qname, getNanoid(cat_0_Qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), "__parentCategory");


        /*
        *
        * glossary_0 -> cat_0
        * glossary_1
        *
        * Move cat_0 to glossary_1
        * */
        cat_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
        flushCategoryRedundantRelations(cat_0);

        cat_0 = updateAndGetEntity(cat_0);
        cat_0_Qname = (String) cat_0.getAttribute(QUALIFIED_NAME);

        assertEquals(cat_0_Qname, getNanoid(cat_0_Qname) + "@" + glossaryTargetQname);
        verifyESInLoop(cat_0.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname), ES_PARENT_CAT);

        /*
         *
         * glossary_0
         * glossary_1 -> cat_0
         *
         * Move cat_0 back to glossary_0
         * */
        flushCategoryRedundantRelations(cat_0);
        cat_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));

        cat_0 = updateAndGetEntity(cat_0);
        cat_0_Qname = (String) cat_0.getAttribute(QUALIFIED_NAME);

        assertEquals(cat_0_Qname, getNanoid(cat_0_Qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_PARENT_CAT);

        LOG.info(">> moveFromRootToRootLevel");
    }

    /*
    * Source -> cat_0.cat_1.term_0
    * Target -> cat_1
    *
    * Move Source.cat_1 to Target.cat_1
    * This should be allowed
    * ----------
    *
    * Source -> cat_0
    * Target -> cat_1.cat_1.term_0
    *
    * Move back Target.cat_1.cat_1 to Source.cat_0
    * -----
    *
    * Source -> cat_0.cat_1.term_0
    * Target -> cat_1
    *
    *
    * */
    private static void moveWithDuplicateNameInDifferentLevel() throws Exception {
        LOG.info(">> moveWithDuplicateNameInDifferentLevel");

        AtlasEntity glossarySource = createGlossary();
        AtlasEntity glossaryTarget = createGlossary();

        AtlasEntity cat_0 = getAtlasEntity(TYPE_CATEGORY, "cat_0");
        cat_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_0 = createAndGetEntity(cat_0);

        AtlasEntity cat_1 = getAtlasEntity(TYPE_CATEGORY, "tag_1");
        cat_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_1.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null));
        cat_1 = createAndGetEntity(cat_1);

        AtlasEntity term_0 = getAtlasEntity(TYPE_TERM, "term_0");
        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singleton(new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null)));
        term_0 = createAndGetEntity(term_0);

        AtlasEntity cat_1_target = getAtlasEntity(TYPE_CATEGORY, (String) cat_0.getAttribute(NAME));
        cat_1_target.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
        cat_1_target = createAndGetEntity(cat_1_target);

        String glossarySourceQname = (String) glossarySource.getAttribute(QUALIFIED_NAME);
        String glossaryTargetQname = (String) glossaryTarget.getAttribute(QUALIFIED_NAME);
        String cat_0_Qname = (String) cat_0.getAttribute(QUALIFIED_NAME);
        String cat_1_Qname = (String) cat_1.getAttribute(QUALIFIED_NAME);
        String term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);
        String cat_1_target_Qname = (String) cat_1_target.getAttribute(QUALIFIED_NAME);

        LOG.info("glossarySourceQname, glossaryTargetQname => {}, {}", glossarySourceQname, glossaryTargetQname);
        LOG.info("catQname : {}", cat_0);

        assertEquals(cat_0_Qname, getNanoid(cat_0_Qname) + "@" + glossarySourceQname);
        assertEquals(cat_1_Qname, getNanoid(cat_1_Qname) + "@" + glossarySourceQname);
        assertEquals(cat_1_target_Qname, getNanoid(cat_1_target_Qname) + "@" + glossaryTargetQname);

        verifyESInLoop(cat_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_PARENT_CAT);
        verifyESInLoop(cat_1.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_0_Qname));
        verifyESInLoop(cat_1_target.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname), ES_PARENT_CAT);


        /*
        *
        * glossarySource -> cat_0 -> cat_1 -> term_0
        * glossaryTarget -> cat_1_target
        *
        * update cat_1 glossaryTarget, cat_1_target
        *
        * */
        cat_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
        cat_1.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_1_target.getGuid(), null));
        flushCategoryRedundantRelations(cat_1);

        cat_1 = updateAndGetEntity(cat_1);
        term_0 = getEntity(term_0.getGuid());
        cat_1_target = getEntity(cat_1_target.getGuid());

        cat_1_Qname = (String) cat_1.getAttribute(QUALIFIED_NAME);
        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);
        cat_1_target_Qname = (String) cat_1_target.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossaryTargetQname);
        assertEquals(cat_1_Qname, getNanoid(cat_1_Qname) + "@" + glossaryTargetQname);
        assertEquals(cat_1_target_Qname, getNanoid(cat_1_target_Qname) + "@" + glossaryTargetQname);

        verifyESInLoop(cat_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_PARENT_CAT);
        verifyESInLoop(term_0.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname, ES_CATEGORIES, cat_1_Qname), ES_PARENT_CAT);
        verifyESInLoop(cat_1.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname, ES_PARENT_CAT, cat_1_target_Qname));
        verifyESInLoop(cat_1_target.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname), ES_PARENT_CAT);


        /*
         *
         * glossarySource -> cat_0 ->
         * glossaryTarget -> cat_1_target -> cat_1 -> term_0
         *
         * update back cat_1 glossarySource, cat_0
         *
         * */

        //Move back Target.cat_1.cat_1 to Source.cat_0
        flushCategoryRedundantRelations(cat_1);
        cat_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_1.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null));


        cat_1 = updateAndGetEntity(cat_1);
        term_0 = getEntity(term_0.getGuid());
        cat_1_target = getEntity(cat_1_target.getGuid());

        cat_1_Qname = (String) cat_1.getAttribute(QUALIFIED_NAME);
        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);
        cat_1_target_Qname = (String) cat_1_target.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossarySourceQname);
        assertEquals(cat_1_Qname, getNanoid(cat_1_Qname) + "@" + glossarySourceQname);
        assertEquals(cat_1_target_Qname, getNanoid(cat_1_target_Qname) + "@" + glossaryTargetQname);

        verifyESInLoop(cat_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_PARENT_CAT);
        verifyESInLoop(term_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_CATEGORIES, cat_1_Qname), ES_PARENT_CAT);
        verifyESInLoop(cat_1.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_0_Qname));
        verifyESInLoop(cat_1_target.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname), ES_PARENT_CAT);

        LOG.info(">> moveWithDuplicateNameInDifferentLevel");
    }

    /*
    * Source -> cat_0.cat_1.term_0
    * Target -> cat_1
    *
    * Move Source.cat_1 to Target root with Source.cat_0 as parent
    * This should FAIL
    *
    * ----------------
    *
    * Remove cat_1 parent category from payload
    * Move Source.cat_1 to Target root
    * This should FAIL
     */
    private static void moveWithDuplicateNameAtSameLevel() throws Exception {
        LOG.info("<< moveWithDuplicateNameAtSameLevel");

        AtlasEntity glossarySource = createGlossary();
        AtlasEntity glossaryTarget = createGlossary();

        AtlasEntity cat_0 = getAtlasEntity(TYPE_CATEGORY, "cat_0");
        cat_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_0 = createAndGetEntity(cat_0);

        AtlasEntity cat_1_source = getAtlasEntity(TYPE_CATEGORY, "cat_1_source");
        cat_1_source.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_1_source.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null));
        cat_1_source = createAndGetEntity(cat_1_source);

        AtlasEntity term_0 = getAtlasEntity(TYPE_TERM, "term_0");
        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singleton(new AtlasEntityHeader(TYPE_CATEGORY, cat_1_source.getGuid(), null)));
        term_0 = createAndGetEntity(term_0);

        AtlasEntity cat_1_target = getAtlasEntity(TYPE_CATEGORY, "cat_1_target");
        cat_1_target.setAttribute(NAME, cat_1_source.getAttribute(NAME));
        cat_1_target.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
        cat_1_target = createAndGetEntity(cat_1_target);

        String glossarySourceQname = (String) glossarySource.getAttribute(QUALIFIED_NAME);
        String glossaryTargetQname = (String) glossaryTarget.getAttribute(QUALIFIED_NAME);
        String cat_0_Qname = (String) cat_0.getAttribute(QUALIFIED_NAME);
        String cat_1_source_Qname = (String) cat_1_source.getAttribute(QUALIFIED_NAME);
        String term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);
        String cat_1_target_Qname = (String) cat_1_target.getAttribute(QUALIFIED_NAME);

        LOG.info("glossarySourceQname, glossaryTargetQname => {}, {}", glossarySourceQname, glossaryTargetQname);
        LOG.info("catQname : {}", cat_0);

        assertEquals(cat_0_Qname, getNanoid(cat_0_Qname) + "@" + glossarySourceQname);
        assertEquals(cat_1_source_Qname, getNanoid(cat_1_source_Qname) + "@" + glossarySourceQname);
        assertEquals(cat_1_target_Qname, getNanoid(cat_1_target_Qname) + "@" + glossaryTargetQname);

        verifyESInLoop(cat_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_PARENT_CAT);
        verifyESInLoop(cat_1_source.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_0_Qname));
        verifyESInLoop(cat_1_target.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname), ES_PARENT_CAT);


        /* Source -> cat_0.cat_1_source.term_0
         * Target -> cat_1
         *
         * Move Source.cat_1_source to Target root with Source.cat_0 as parent
         * This should FAIL
         *
        * */

        flushCategoryRedundantRelations(cat_1_source);
        cat_1_source.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));

        boolean failed = false;
        try {
            cat_1_source = updateAndGetEntity(cat_1_source);

        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains(PARENT_CAT_FROM_OTHER_GLOSSARY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(PARENT_CAT_FROM_OTHER_GLOSSARY_ERROR_MESSAGE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        /*
        * Source -> cat_0.cat_1_source.term_0
        * Target -> cat_1
        *
        * Remove cat_1_source parent category from payload
        * Move Source.cat_1 to Target root
        * This should FAIL
        *
        * */

        cat_1_source.getRelationshipAttributes().remove(REL_PARENT_CAT);
        cat_1_source.setAttribute(NAME, "random");
        cat_1_source.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));

        failed = false;
        try {
            cat_1_source = updateAndGetEntity(cat_1_source);
        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains(PARENT_CAT_FROM_OTHER_GLOSSARY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(PARENT_CAT_FROM_OTHER_GLOSSARY_ERROR_MESSAGE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }


        /*
         * Source -> cat_0.cat_1_source.term_0
         * Target -> cat_1
         *
         * Set cat_1_source parent category to null
         * Move Source.cat_1_source to Target root
         * This should FAIL
         *
         * */

        Thread.sleep(3000);
        cat_1_source.setAttribute(NAME, cat_1_target.getAttribute(NAME));
        cat_1_source.setRelationshipAttribute(REL_PARENT_CAT, null);
        cat_1_source.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));

        failed = false;
        try {
            cat_1_source = updateAndGetEntity(cat_1_source);
        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains("ATLAS-409-00-00A"));
            assertTrue(exception.getMessage().contains("Glossary category with name " + cat_1_target.getAttribute(NAME) + " already exists on this level"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }


        /////

        cat_1_source.setAttribute(NAME, "random name");
        cat_1_source.setRelationshipAttribute(REL_PARENT_CAT, null);
        cat_1_source.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));

        cat_1_source = updateAndGetEntity(cat_1_source);

        cat_1_source_Qname = (String) cat_1_source.getAttribute(QUALIFIED_NAME);


        assertEquals(cat_1_source_Qname, getNanoid(cat_1_source_Qname) + "@" + glossaryTargetQname);
        assertEquals(cat_1_target_Qname, getNanoid(cat_1_target_Qname) + "@" + glossaryTargetQname);

        verifyESInLoop(cat_1_source.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname), ES_PARENT_CAT);
        verifyESInLoop(cat_1_target.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname), ES_PARENT_CAT);


        LOG.info(">> moveWithDuplicateNameAtSameLevel");
    }

    /*
    *
    * glossarySource -> cat_0 -> cat_0_0  -> cat_0_1   -> cat_0_2 -> term_0_3
    *                                     -> term_0_1
    *                         -> term_0_0

    *                -> cat_1 -> cat_1_0, cat_1_1, cat_1_1
    *                -> cat_2 -> term_2_0, term_2_1, term_2_1
    *
    * */
    private static void moveHeavyCategory() throws Exception {
        LOG.info(">> moveHeavyCategory");


        AtlasEntity glossarySource = createGlossary();
        AtlasEntity glossaryTarget = createGlossary();

        AtlasEntity cat_0 = getAtlasEntity(TYPE_CATEGORY, "cat_0");
        cat_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_0 = createAndGetEntity(cat_0);

        AtlasEntity cat_0_0 = getAtlasEntity(TYPE_CATEGORY, "cat_0_0");
        cat_0_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_0_0.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null));
        cat_0_0 = createAndGetEntity(cat_0_0);

        AtlasEntity cat_0_0_an = getAtlasEntity(TYPE_CATEGORY, "cat_0_0_an");
        cat_0_0_an.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_0_0_an.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null));
        cat_0_0_an = createAndGetEntity(cat_0_0_an);

        AtlasEntity term_0_0 = getAtlasEntity(TYPE_TERM, "term_0_0");
        term_0_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        term_0_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singleton(new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null)));
        term_0_0 = createAndGetEntity(term_0_0);

        AtlasEntity cat_0_1 = getAtlasEntity(TYPE_CATEGORY, "cat_0_1");
        cat_0_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_0_1.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0_0.getGuid(), null));
        cat_0_1 = createAndGetEntity(cat_0_1);

        AtlasEntity term_0_1 = getAtlasEntity(TYPE_TERM, "term_0_1");
        term_0_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        term_0_1.setRelationshipAttribute(REL_CATEGORIES, Collections.singleton(new AtlasEntityHeader(TYPE_CATEGORY, cat_0_0.getGuid(), null)));
        term_0_1 = createAndGetEntity(term_0_1);

        AtlasEntity cat_0_2 = getAtlasEntity(TYPE_CATEGORY, "cat_0_2");
        cat_0_2.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_0_2.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0_1.getGuid(), null));
        cat_0_2 = createAndGetEntity(cat_0_2);

        AtlasEntity term_0_3 = getAtlasEntity(TYPE_TERM, "term_0_3");
        term_0_3.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        term_0_3.setRelationshipAttribute(REL_CATEGORIES, Collections.singleton(new AtlasEntityHeader(TYPE_CATEGORY, cat_0_2.getGuid(), null)));
        term_0_3 = createAndGetEntity(term_0_3);

        //----
        AtlasEntity cat_1 = getAtlasEntity(TYPE_CATEGORY, "cat_1");
        cat_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_1 = createAndGetEntity(cat_1);

        AtlasEntity cat_1_0 = getAtlasEntity(TYPE_CATEGORY, "cat_1_0");
        cat_1_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_1_0.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null));
        cat_1_0 = createAndGetEntity(cat_1_0);

        AtlasEntity cat_1_1 = getAtlasEntity(TYPE_CATEGORY, "cat_1_1");
        cat_1_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_1_1.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null));
        cat_1_1 = createAndGetEntity(cat_1_1);

        AtlasEntity cat_1_2 = getAtlasEntity(TYPE_CATEGORY, "cat_1_2");
        cat_1_2.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_1_2.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null));
        cat_1_2 = createAndGetEntity(cat_1_2);

        //----------
        AtlasEntity cat_2 = getAtlasEntity(TYPE_CATEGORY, "cat_2");
        cat_2.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_2 = createAndGetEntity(cat_2);

        AtlasEntity term_2_0 = getAtlasEntity(TYPE_TERM, "term_2_0");
        term_2_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        term_2_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singleton(new AtlasEntityHeader(TYPE_CATEGORY, cat_2.getGuid(), null)));
        term_2_0 = createAndGetEntity(term_2_0);

        AtlasEntity term_2_1 = getAtlasEntity(TYPE_TERM, "term_2_1");
        term_2_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        term_2_1.setRelationshipAttribute(REL_CATEGORIES, Collections.singleton(new AtlasEntityHeader(TYPE_CATEGORY, cat_2.getGuid(), null)));
        term_2_1 = createAndGetEntity(term_2_1);

        AtlasEntity term_2_2 = getAtlasEntity(TYPE_TERM, "term_2_2");
        term_2_2.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        term_2_2.setRelationshipAttribute(REL_CATEGORIES, Collections.singleton(new AtlasEntityHeader(TYPE_CATEGORY, cat_2.getGuid(), null)));
        term_2_2 = createAndGetEntity(term_2_2);

        String glossarySourceQname = (String) glossarySource.getAttribute(QUALIFIED_NAME);
        String glossaryTargetQname = (String) glossaryTarget.getAttribute(QUALIFIED_NAME);

        String cat_0_qname = (String) cat_0.getAttribute(QUALIFIED_NAME);
        String cat_1_qname = (String) cat_1.getAttribute(QUALIFIED_NAME);
        String cat_2_qname = (String) cat_2.getAttribute(QUALIFIED_NAME);
        String cat_0_0_qname = (String) cat_0_0.getAttribute(QUALIFIED_NAME);
        String cat_0_0_an_qname = (String) cat_0_0_an.getAttribute(QUALIFIED_NAME);
        String cat_0_1_qname = (String) cat_0_1.getAttribute(QUALIFIED_NAME);
        String cat_0_2_qname = (String) cat_0_2.getAttribute(QUALIFIED_NAME);
        String cat_1_0_qname = (String) cat_1_0.getAttribute(QUALIFIED_NAME);
        String cat_1_1_qname = (String) cat_1_1.getAttribute(QUALIFIED_NAME);
        String cat_1_2_qname = (String) cat_1_2.getAttribute(QUALIFIED_NAME);

        String term_0_0_qname = (String) term_0_0.getAttribute(QUALIFIED_NAME);
        String term_0_1_qname = (String) term_0_1.getAttribute(QUALIFIED_NAME);
        String term_0_3_qname = (String) term_0_3.getAttribute(QUALIFIED_NAME);
        String term_2_0_qname = (String) term_2_0.getAttribute(QUALIFIED_NAME);
        String term_2_1_qname = (String) term_2_1.getAttribute(QUALIFIED_NAME);
        String term_2_2_qname = (String) term_2_2.getAttribute(QUALIFIED_NAME);

        assertEquals(cat_0_qname, getNanoid(cat_0_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_PARENT_CAT);

        assertEquals(cat_1_qname, getNanoid(cat_1_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_1.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_PARENT_CAT);

        assertEquals(cat_2_qname, getNanoid(cat_2_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_2.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_PARENT_CAT);

        assertEquals(cat_0_0_qname, getNanoid(cat_0_0_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_0_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_0_qname));

        assertEquals(cat_0_0_an_qname, getNanoid(cat_0_0_an_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_0_0_an.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_0_qname));

        assertEquals(term_0_0_qname, getNanoid(term_0_0_qname) + "@" + glossarySourceQname);
        verifyESInLoop(term_0_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_CATEGORIES, cat_0_qname));

        assertEquals(cat_0_1_qname, getNanoid(cat_0_1_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_0_1.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_0_0_qname));

        assertEquals(term_0_1_qname, getNanoid(term_0_1_qname) + "@" + glossarySourceQname);
        verifyESInLoop(term_0_1.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_CATEGORIES, cat_0_0_qname));

        assertEquals(cat_0_2_qname, getNanoid(cat_0_2_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_0_2.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_0_1_qname));

        assertEquals(term_0_3_qname, getNanoid(term_0_3_qname) + "@" + glossarySourceQname);
        verifyESInLoop(term_0_3.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_CATEGORIES, cat_0_2_qname));

        assertEquals(cat_1_0_qname, getNanoid(cat_1_0_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_1_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_1_qname));

        assertEquals(cat_1_1_qname, getNanoid(cat_1_1_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_1_1.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_1_qname));

        assertEquals(cat_1_2_qname, getNanoid(cat_1_2_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_1_2.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_1_qname));

        assertEquals(term_2_0_qname, getNanoid(term_2_0_qname) + "@" + glossarySourceQname);
        verifyESInLoop(term_2_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_CATEGORIES, cat_2_qname));

        assertEquals(term_2_1_qname, getNanoid(term_2_1_qname) + "@" + glossarySourceQname);
        verifyESInLoop(term_2_1.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_CATEGORIES, cat_2_qname));

        assertEquals(term_2_2_qname, getNanoid(term_2_2_qname) + "@" + glossarySourceQname);
        verifyESInLoop(term_2_2.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_CATEGORIES, cat_2_qname));

        // Move cat_0 to glossaryTarget

        flushCategoryRedundantRelations(cat_0);
        cat_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));

        cat_0 = updateAndGetEntity(cat_0);
        Thread.sleep(10000);

        cat_1 = getEntity(cat_1.getGuid());
        cat_2 = getEntity(cat_2.getGuid());

        cat_0_0 = getEntity(cat_0_0.getGuid());
        cat_0_0_an = getEntity(cat_0_0_an.getGuid());
        cat_0_1 = getEntity(cat_0_1.getGuid());
        cat_0_2 = getEntity(cat_0_2.getGuid());
        cat_1_0 = getEntity(cat_1_0.getGuid());
        cat_1_1 = getEntity(cat_1_1.getGuid());
        cat_1_2 = getEntity(cat_1_2.getGuid());

        term_0_0 = getEntity(term_0_0.getGuid());
        term_0_1 = getEntity(term_0_1.getGuid());
        term_0_3 = getEntity(term_0_3.getGuid());
        term_2_0 = getEntity(term_2_0.getGuid());
        term_2_1 = getEntity(term_2_1.getGuid());
        term_2_2 = getEntity(term_2_2.getGuid());

        cat_0_qname = (String) cat_0.getAttribute(QUALIFIED_NAME);
        cat_1_qname = (String) cat_1.getAttribute(QUALIFIED_NAME);
        cat_2_qname = (String) cat_2.getAttribute(QUALIFIED_NAME);
        cat_0_0_qname = (String) cat_0_0.getAttribute(QUALIFIED_NAME);
        cat_0_0_an_qname = (String) cat_0_0_an.getAttribute(QUALIFIED_NAME);
        cat_0_1_qname = (String) cat_0_1.getAttribute(QUALIFIED_NAME);
        cat_0_2_qname = (String) cat_0_2.getAttribute(QUALIFIED_NAME);
        cat_1_0_qname = (String) cat_1_0.getAttribute(QUALIFIED_NAME);
        cat_1_1_qname = (String) cat_1_1.getAttribute(QUALIFIED_NAME);
        cat_1_2_qname = (String) cat_1_2.getAttribute(QUALIFIED_NAME);

        term_0_0_qname = (String) term_0_0.getAttribute(QUALIFIED_NAME);
        term_0_1_qname = (String) term_0_1.getAttribute(QUALIFIED_NAME);
        term_0_3_qname = (String) term_0_3.getAttribute(QUALIFIED_NAME);
        term_2_0_qname = (String) term_2_0.getAttribute(QUALIFIED_NAME);
        term_2_1_qname = (String) term_2_1.getAttribute(QUALIFIED_NAME);
        term_2_2_qname = (String) term_2_2.getAttribute(QUALIFIED_NAME);

        assertEquals(cat_0_qname, getNanoid(cat_0_qname) + "@" + glossaryTargetQname);
        verifyESInLoop(cat_0.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname), ES_PARENT_CAT);

        assertEquals(cat_1_qname, getNanoid(cat_1_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_1.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_PARENT_CAT);

        assertEquals(cat_2_qname, getNanoid(cat_2_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_2.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_PARENT_CAT);

        assertEquals(cat_0_0_qname, getNanoid(cat_0_0_qname) + "@" + glossaryTargetQname);
        verifyESInLoop(cat_0_0.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname, ES_PARENT_CAT, cat_0_qname));

        assertEquals(cat_0_0_an_qname, getNanoid(cat_0_0_an_qname) + "@" + glossaryTargetQname);
        verifyESInLoop(cat_0_0_an.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname, ES_PARENT_CAT, cat_0_qname));

        assertEquals(term_0_0_qname, getNanoid(term_0_0_qname) + "@" + glossaryTargetQname);
        verifyESInLoop(term_0_0.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname, ES_CATEGORIES, cat_0_qname));

        assertEquals(cat_0_1_qname, getNanoid(cat_0_1_qname) + "@" + glossaryTargetQname);
        verifyESInLoop(cat_0_1.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname, ES_PARENT_CAT, cat_0_0_qname));

        assertEquals(term_0_1_qname, getNanoid(term_0_1_qname) + "@" + glossaryTargetQname);
        verifyESInLoop(term_0_1.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname, ES_CATEGORIES, cat_0_0_qname));

        assertEquals(cat_0_2_qname, getNanoid(cat_0_2_qname) + "@" + glossaryTargetQname);
        verifyESInLoop(cat_0_2.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname, ES_PARENT_CAT, cat_0_1_qname));

        assertEquals(term_0_3_qname, getNanoid(term_0_3_qname) + "@" + glossaryTargetQname);
        verifyESInLoop(term_0_3.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname, ES_CATEGORIES, cat_0_2_qname));

        assertEquals(cat_1_0_qname, getNanoid(cat_1_0_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_1_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_1_qname));

        assertEquals(cat_1_1_qname, getNanoid(cat_1_1_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_1_1.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_1_qname));

        assertEquals(cat_1_2_qname, getNanoid(cat_1_2_qname) + "@" + glossarySourceQname);
        verifyESInLoop(cat_1_2.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_PARENT_CAT, cat_1_qname));

        assertEquals(term_2_0_qname, getNanoid(term_2_0_qname) + "@" + glossarySourceQname);
        verifyESInLoop(term_2_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_CATEGORIES, cat_2_qname));

        assertEquals(term_2_1_qname, getNanoid(term_2_1_qname) + "@" + glossarySourceQname);
        verifyESInLoop(term_2_1.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_CATEGORIES, cat_2_qname));

        assertEquals(term_2_2_qname, getNanoid(term_2_2_qname) + "@" + glossarySourceQname);
        verifyESInLoop(term_2_2.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_CATEGORIES, cat_2_qname));

        LOG.info(">> moveHeavyCategory");
    }

    private static void flushCategoryRedundantRelations(AtlasEntity category) {
        category.getRelationshipAttributes().remove(REL_CHILDREN_CATS);
        category.getRelationshipAttributes().remove(REL_TERMS);
    }

}