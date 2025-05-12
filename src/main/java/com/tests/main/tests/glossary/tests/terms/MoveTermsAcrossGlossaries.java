package com.tests.main.tests.glossary.tests.terms;


import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.TestUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.TYPE_GLOSSARY;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createAndGetEntity;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.createGlossary;
import static com.tests.main.utils.TestUtil.getGlossaryModel;
import static com.tests.main.utils.TestUtil.getTermModel;
import static org.junit.Assert.*;

import static com.tests.main.utils.TestUtil.*;


public class MoveTermsAcrossGlossaries implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(MoveTermsAcrossGlossaries.class);

    private static final String CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_CODE = "ATLAS-400-00-029";
    private static final String CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_MESSAGE = "Passed category doesn't belongs to Passed Glossary";

    private static final String DUPLICATE_TERM_ERROR_CODE = "ATLAS-409-00-009";
    private static final String DUPLICATE_TERM_ERROR_MESSAGE = "Glossary term with name %s already exists";

    private static final String CONN_QN = "default/redshift/1688473816/";

    public static void main(String[] args) throws Exception {
        try {
            new MoveTermsAcrossGlossaries().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running MoveTermsAcrossGlossaries tests");

        long start = System.currentTimeMillis();
        try {

            createTermWithCategoryFromDifferentGlossary();
            moveFromRootToRootLevel();
            moveWithDuplicateName();
            moveNestedTermToRoot();
            moveRootTermToNestedCategory();
            moveNestedToNested();
            test__meaningsAttribute();

        } catch (Exception e) {
            throw e;
        } finally {
            cleanUpAll();
            LOG.info("Completed running MoveTermsAcrossGlossaries tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void createTermWithCategoryFromDifferentGlossary() throws Exception {
        LOG.info(">> createTermWithCategoryFromDifferentGlossary");

        AtlasEntity glossary_0 = createGlossary();
        AtlasEntity glossary_1 = createGlossary();

        AtlasEntity cat_0 = getAtlasEntity(TYPE_CATEGORY, "cat_0");
        cat_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        cat_0 = createAndGetEntity(cat_0);
        String cat_0_qname = (String) cat_0.getAttribute(QUALIFIED_NAME);

        /*
        *
        * glossary_0 -> cat_0
        * glossary_1
        *
        *
        * create term_0 with glossary_1 & cat_0
        * */

        AtlasEntity term_0 = getEntity(TYPE_TERM, "term_0");
        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_1.getGuid(), null));
        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singletonList(new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null)));

        boolean failed = false;
        try {
            term_0 = createAndGetEntity(term_0);
        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains(CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_MESSAGE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        /*
         *
         * glossary_0 -> cat_0
         * glossary_1
         *
         *
         * create term_0 with glossary_0 & cat_0
         * */

        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        term_0 = createAndGetEntity(term_0);

        String glossary_0_Qname = (String) glossary_0.getAttribute(QUALIFIED_NAME);
        String glossary_1_Qname = (String) glossary_1.getAttribute(QUALIFIED_NAME);
        String term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossary_0_Qname);
        verifyESInLoop(term_0.getGuid(), mapOf(ES_GLOSSARY, glossary_0_Qname, ES_CATEGORIES, cat_0_qname));


        //-------------
        /*
        * glossary_0 -> cat_0 -> term_0
        * glossary_1
        *
        * create cat_1 in glossary_1
        *
        * */

        AtlasEntity cat_1 = getAtlasEntity(TYPE_CATEGORY, "cat_1");
        cat_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_1.getGuid(), null));
        cat_1 = createAndGetEntity(cat_1);

        String cat_1_Qname = (String) cat_1.getAttribute(QUALIFIED_NAME);


        //-------------
        /*
         * glossary_0 -> cat_0 -> term_0
         * glossary_1 -> cat_1
         *
         * update term_0 glossary_0 & cat_1
         * should fail
         *
         * */

        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singletonList(new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null)));

        failed = false;
        try {
            term_0 = updateAndGetEntity(term_0);
        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains(CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_MESSAGE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        //-------------
        /*
         * glossary_0 -> cat_0 -> term_0
         * glossary_1 -> cat_1
         *
         * update term_0 category to cat_1 & glossary glossary_1
         * should Pass
         *
         * */

        String cat_1_qname = (String) cat_1.getAttribute(QUALIFIED_NAME);

        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_1.getGuid(), null));
        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singletonList(new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null)));

        term_0 = updateAndGetEntity(term_0);
        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossary_1_Qname);
        verifyESInLoop(term_0.getGuid(), mapOf(ES_GLOSSARY, glossary_1_Qname, ES_CATEGORIES, cat_1_qname));


        //-------------
        /*
         * glossary_0 -> cat_0
         * glossary_1 -> cat_1 -> term_0
         *
         * update term_0 category to cat_0
         * should fail
         *
         * */

        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singletonList(new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null)));

        failed = false;
        try {
            term_0 = updateAndGetEntity(term_0);
        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains(CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_MESSAGE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        //-------------
        /*
         * glossary_0 -> cat_0
         * glossary_1 -> cat_1 -> term_0
         *
         * remove category relationship & update term_0
         *
         * */

        term_0.getRelationshipAttributes().remove(REL_CATEGORIES);

        createEntity(term_0);
        term_0 = TestUtil.getEntity(term_0.getGuid());
        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossary_1_Qname);
        verifyESInLoop(term_0.getGuid(), mapOf(ES_GLOSSARY, glossary_1_Qname, ES_CATEGORIES, cat_1_qname));


        //-------------
        /*
         * glossary_0 -> cat_0
         * glossary_1 -> cat_1 -> term_0
         *
         * update term_0 to glossary_0 & cat_1 explicitly
         * Should fail
         *
         * */

        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singletonList(new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null)));

        failed = false;
        try {
            term_0 = updateAndGetEntity(term_0);
        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains(CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_MESSAGE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        /*
         * glossary_0 -> cat_0
         * glossary_1 -> cat_1 -> term_0
         *
         * update term_0 with glossary_0 & cat_0
         *
         * */


        //failing due to Glossary term with name term_0 already exists
        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossary_0.getGuid(), null));
        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singletonList(new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null)));

        term_0 = updateAndGetEntity(term_0);
        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossary_0_Qname);
        verifyESInLoop(term_0.getGuid(), mapOf(ES_GLOSSARY, glossary_0_Qname));


        LOG.info(">> createTermWithCategoryFromDifferentGlossary");
    }
    
    //move term_0 to glossaryTarget
    private static void moveFromRootToRootLevel() throws Exception {
        LOG.info(">> moveFromRootToRootLevel");

        AtlasEntity glossarySource = createGlossary();
        AtlasEntity glossaryTarget = createGlossary();

        AtlasEntity term_0 = getEntity(TYPE_TERM, "term_0");
        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));

        term_0 = createAndGetEntity(term_0);

        String glossarySourceQname = (String) glossarySource.getAttribute(QUALIFIED_NAME);
        String glossaryTargetQname = (String) glossaryTarget.getAttribute(QUALIFIED_NAME);
        String term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        LOG.info("glossarySourceQname, glossaryTargetQname => {}, {}", glossarySourceQname, glossaryTargetQname);
        LOG.info("termQname : {}", term_0_Qname);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossarySourceQname);
        verifyES(term_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname));

        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));

        term_0 = updateAndGetEntity(term_0);
        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossaryTargetQname);
        Thread.sleep(2000);
        verifyES(term_0.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname));


        // move back term_0 to source glossary
        //failing due to Glossary term with name term_0 already exists
        term_0.setRelationshipAttribute("anchor", new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));

        term_0 = updateAndGetEntity(term_0);
        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossarySourceQname);
        Thread.sleep(2000);
        verifyES(term_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname));


        LOG.info(">> moveFromRootToRootLevel");
    }

    private static void moveWithDuplicateName() throws Exception {
        LOG.info(">> moveWithDuplicateName");

        AtlasEntity glossarySource, glossaryTarget, term_0;

        glossarySource = createGlossary();
        glossaryTarget = createGlossary();

        term_0 = getEntity(TYPE_TERM, "term_0");
        term_0.setRelationshipAttribute("anchor", new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));

        term_0 = createAndGetEntity(term_0);

        String glossarySourceQname = (String) glossarySource.getAttribute(QUALIFIED_NAME);
        String glossaryTargetQname = (String) glossaryTarget.getAttribute(QUALIFIED_NAME);
        String term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        //LOG.info("glossarySourceQname, glossaryTargetQname => {}, {}", glossarySourceQname, glossaryTargetQname);
        //LOG.info("termQname : {}", term_0_Qname);

        /*
        * glossarySource -> term_0
        * glossaryTarget ->
        *
        *
        * Create term_1 in glossaryTarget
        * */

        String term_0_name = (String) term_0.getAttribute(NAME);

        AtlasEntity term_1 = new AtlasEntity(TYPE_TERM);
        term_1.setAttribute(NAME, term_0_name);
        term_1.setAttribute(QUALIFIED_NAME, term_0_name);
        term_1.setRelationshipAttribute("anchor", new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));

        term_1 = createAndGetEntity(term_1);
        String term_1_Qname = (String) term_1.getAttribute(QUALIFIED_NAME);

        assertEquals(term_1_Qname, getNanoid(term_1_Qname) + "@" + glossaryTargetQname);
        verifyES(term_1.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname, "name", term_0_name), ES_CATEGORIES);

        /*
         * glossarySource -> term_0
         * glossaryTarget -> term_1(name:term_0)
         *
         *
         * Move term_0 to glossaryTarget
         * This should fail
         * */

        Thread.sleep(2000);
        term_0.setRelationshipAttribute("anchor", new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));

        boolean failed = false;
        try {
            term_0 = updateAndGetEntity(term_0);
        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains(DUPLICATE_TERM_ERROR_CODE));
            assertTrue(exception.getMessage().contains(String.format(DUPLICATE_TERM_ERROR_MESSAGE, term_0_name)));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        /*
         * glossarySource -> term_0
         * glossaryTarget -> term_1(name:term_0)
         *
         *
         * create new category_0 to glossaryTarget
         * */


        AtlasEntity category_0 = getEntity(TYPE_CATEGORY, "cat_0");
        category_0.setRelationshipAttribute("anchor", new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));

        category_0 = createAndGetEntity(category_0);
        String category_0_Qname = (String) category_0.getAttribute(QUALIFIED_NAME);

        assertEquals(category_0_Qname, getNanoid(category_0_Qname) + "@" + glossaryTargetQname);
        verifyES(category_0.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname));

        /*
         * glossarySource -> term_0
         * glossaryTarget -> category_0, term_1(name:term_0)
         *
         *
         * move term_0 to glossaryTarget under category_0
         * */

        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singletonList(new AtlasEntityHeader(TYPE_CATEGORY, category_0.getGuid(), null)));

        failed = false;
        try {
            term_0 = updateAndGetEntity(term_0);
        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains(DUPLICATE_TERM_ERROR_CODE));
            assertTrue(exception.getMessage().contains(String.format(DUPLICATE_TERM_ERROR_MESSAGE, term_0.getAttribute(NAME))));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        LOG.info(">> moveWithDuplicateName");
    }

    //Move from cat_0.cat_1.term_0 to root
    private static void moveNestedTermToRoot() throws Exception {
        LOG.info(">> moveNestedTermToRoot");

        AtlasEntity glossarySource, glossaryTarget, cat_0, cat_1, term_0;

        glossarySource = createGlossary();
        glossaryTarget = createGlossary();

        cat_0 = getEntity(TYPE_CATEGORY, "cat_0");
        cat_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_0 = createAndGetEntity(cat_0);

        cat_1 = getEntity(TYPE_CATEGORY, "cat_1");
        cat_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_1.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null));
        cat_1 = createAndGetEntity(cat_1);


        term_0 = getEntity(TYPE_TERM, "term_0");
        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singletonList(new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null)));

        term_0 = createAndGetEntity(term_0);

        String glossarySourceQname = (String) glossarySource.getAttribute(QUALIFIED_NAME);
        String glossaryTargetQname = (String) glossaryTarget.getAttribute(QUALIFIED_NAME);

        String term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);
        String cat_0_Qname = (String) cat_0.getAttribute(QUALIFIED_NAME);
        String cat_1_Qname = (String) cat_1.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossarySourceQname);
        verifyES(term_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname));
        verifyES(term_0.getGuid(), mapOf(ES_CATEGORIES, getNanoid(cat_1_Qname) + "@" + glossarySourceQname));

        //LOG.info("glossarySourceQname, glossaryTargetQname => {}, {}", glossarySourceQname, glossaryTargetQname);
        //LOG.info("termQname : {}", term_0_Qname);

        /*
        * glossarySource -> cat_0 -> cat_1 -> term_0
        * glossaryTarget ->
        *
        * */

        //Move from source.cat_0.cat_1.term_0 to target.root
        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
        term_0.setRelationshipAttribute(REL_CATEGORIES, null);
        term_0 = updateAndGetEntity(term_0);
        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossaryTargetQname);
        Thread.sleep(2000);
        verifyES(term_0.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname), ES_CATEGORIES);

        LOG.info(">> moveNestedTermToRoot");
    }

    //Move from source.root to target.cat_0.cat_1.term_0
    private static void moveRootTermToNestedCategory() throws Exception {
        LOG.info(">> moveRootTermToNestedCategory");

        AtlasEntity glossarySource, glossaryTarget, cat_0, cat_1, term_0;

        glossarySource = createGlossary();
        glossaryTarget = createGlossary();

        cat_0 = getEntity(TYPE_CATEGORY, "cat_0");
        cat_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
        cat_0 = createAndGetEntity(cat_0);

        cat_1 = getEntity(TYPE_CATEGORY, "cat_1");
        cat_1.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
        cat_1.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0.getGuid(), null));
        cat_1 = createAndGetEntity(cat_1);


        term_0 = getEntity(TYPE_TERM, "term_0");
        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));

        term_0 = createAndGetEntity(term_0);

        String glossarySourceQname = (String) glossarySource.getAttribute(QUALIFIED_NAME);
        String glossaryTargetQname = (String) glossaryTarget.getAttribute(QUALIFIED_NAME);

        String term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);
        String cat_0_Qname = (String) cat_0.getAttribute(QUALIFIED_NAME);
        String cat_1_Qname = (String) cat_1.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossarySourceQname);
        verifyES(term_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_CATEGORIES);

        //LOG.info("glossarySourceQname, glossaryTargetQname => {}, {}", glossarySourceQname, glossaryTargetQname);
        //LOG.info("termQname : {}", term_0_Qname);

        /*
        * glossarySource -> term_0
        * glossaryTarget -> cat_0 -> cat_1
        *
        * Move from term_0 to target.cat_1
        *
        * */

        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singletonList(new AtlasEntityHeader(TYPE_CATEGORY, cat_1.getGuid(), null)));
        term_0 = updateAndGetEntity(term_0);
        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossaryTargetQname);
        Thread.sleep(2000);
        verifyES(term_0.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname, ES_CATEGORIES, cat_1_Qname));


        //moving back from target.cat_0.cat_1.term_0 to source.root
        /*
         * glossarySource ->
         * glossaryTarget -> cat_0 -> cat_1 -> term_0
         *
         * moving back term_0 glossaryTarget.cat_1 to glossarySource.root
         *
         * */


        term_0.setRelationshipAttribute(REL_CATEGORIES, null);
        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        term_0 = updateAndGetEntity(term_0);
        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossarySourceQname);
        Thread.sleep(2000);
        verifyES(term_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_CATEGORIES);

        LOG.info(">> moveRootTermToNestedCategory");
    }

    //Move from source.cat_0.cat_1 to target.cat_0.cat_1
    private static void moveNestedToNested() throws Exception {
        LOG.info(">> moveNestedToNested");

        AtlasEntity glossarySource, glossaryTarget, cat_0_source, cat_1_source, cat_0_target, cat_1_target, term_0;

        glossarySource = createGlossary();
        glossaryTarget = createGlossary();

        cat_0_source = getEntity(TYPE_CATEGORY, "cat_0_source");
        cat_0_source.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_0_source = createAndGetEntity(cat_0_source);

        cat_1_source = getEntity(TYPE_CATEGORY, "cat_1_source");
        cat_1_source.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        cat_1_source.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0_source.getGuid(), null));
        cat_1_source = createAndGetEntity(cat_1_source);

        cat_0_target = getEntity(TYPE_CATEGORY, "cat_0_target");
        cat_0_target.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
        cat_0_target = createAndGetEntity(cat_0_target);

        cat_1_target = getEntity(TYPE_CATEGORY, "cat_1_target");
        cat_1_target.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
        cat_1_target.setRelationshipAttribute(REL_PARENT_CAT, new AtlasEntityHeader(TYPE_CATEGORY, cat_0_target.getGuid(), null));
        cat_1_target = createAndGetEntity(cat_1_target);


        term_0 = getEntity(TYPE_TERM, "term_0");
        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singletonList(new AtlasEntityHeader(TYPE_CATEGORY, cat_1_source.getGuid(), null)));

        term_0 = createAndGetEntity(term_0);

        String glossarySourceQname = (String) glossarySource.getAttribute(QUALIFIED_NAME);
        String glossaryTargetQname = (String) glossaryTarget.getAttribute(QUALIFIED_NAME);

        String term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);


        String cat_1_source_Qname = (String) cat_1_source.getAttribute(QUALIFIED_NAME);
        String cat_1_target_Qname = (String) cat_1_target.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossarySourceQname);
        verifyES(term_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname, ES_CATEGORIES, cat_1_source_Qname));

        //LOG.info("glossarySourceQname, glossaryTargetQname => {}, {}", glossarySourceQname, glossaryTargetQname);
        //LOG.info("termQname : {}", term_0_Qname);

        //--------

        /*
        * glossarySource -> cat_0_source -> cat_1_source -> term_0
        * glossaryTarget -> cat_0_target -> cat_1_target
        *
        *
        * Update term_0 with Glossary as glossaryTarget & category as existing cat_1_source
        * */
        boolean failed = false;
        try {
            term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
            term_0 = updateAndGetEntity(term_0);

        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains(CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_MESSAGE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        /*
         * glossarySource -> cat_0_source -> cat_1_source -> term_0
         * glossaryTarget -> cat_0_target -> cat_1_target
         *
         *
         * remove REL_CATEGORIES from payload
         * Update term_0 with Glossary as glossaryTarget
         * */
        failed = false;
        try {
            term_0.getRelationshipAttributes().remove(REL_CATEGORIES);
            term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
            term_0 = updateAndGetEntity(term_0);

        } catch (Exception exception) {
            LOG.info(exception.getMessage());
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains(CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_CODE));
            assertTrue(exception.getMessage().contains(CATEGORY_FROM_DIFFERENT_GLOSSARY_ERROR_MESSAGE));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        /*
         * glossarySource -> cat_0_source -> cat_1_source -> term_0
         * glossaryTarget -> cat_0_target -> cat_1_target
         *
         *
         *
         * Update term_0 with Glossary as glossaryTarget category as cat_1_target
         * */


        term_0.setRelationshipAttribute(REL_CATEGORIES, Collections.singletonList(new AtlasEntityHeader(TYPE_CATEGORY, cat_1_target.getGuid(), null)));
        term_0 = updateAndGetEntity(term_0);
        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossaryTargetQname);
        Thread.sleep(2000);
        verifyES(term_0.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname, ES_CATEGORIES, cat_1_target_Qname));


        /*
         * glossarySource -> cat_0_source -> cat_1_source
         * glossaryTarget -> cat_0_target -> cat_1_target -> term_0
         *
         *
         *
         * Update term_0 with Glossary as glossarySource category as null
         * moving back from target.cat_0.cat_1.term_0 to source.root
         * */

        //
        term_0.setRelationshipAttribute(REL_CATEGORIES, null);
        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));
        term_0 = updateAndGetEntity(term_0);
        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossarySourceQname);
        Thread.sleep(2000);
        verifyES(term_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname), ES_CATEGORIES);

        LOG.info(">> moveNestedToNested");
    }

    private static void test__meaningsAttribute() throws Exception {
        LOG.info(">> test__meaningsAttribute");

        AtlasEntity glossarySource = createGlossary();
        AtlasEntity glossaryTarget = createGlossary();

        AtlasEntity term_0 = getEntity(TYPE_TERM, "term_0");
        term_0.setRelationshipAttribute("anchor", new AtlasEntityHeader(TYPE_GLOSSARY, glossarySource.getGuid(), null));

        term_0 = createAndGetEntity(term_0);

        String glossarySourceQname = (String) glossarySource.getAttribute(QUALIFIED_NAME);
        String glossaryTargetQname = (String) glossaryTarget.getAttribute(QUALIFIED_NAME);
        String term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);

        LOG.info("glossarySourceQname, glossaryTargetQname => {}, {}", glossarySourceQname, glossaryTargetQname);
        LOG.info("termQname : {}", term_0_Qname);

        /*
        * glossarySource -> term_0
        * glossarySource
        *
        * */

        AtlasEntity table_0 = getEntity("Table",  CONN_QN + "table_0_"+ getRandomName());
        table_0.setRelationshipAttribute(REL_MEANINGS, Collections.singletonList(new AtlasEntityHeader(TYPE_TERM, term_0.getGuid(), null)));
        table_0 = createAndGetEntity(table_0);

        AtlasEntity table_1 = getEntity("Table", CONN_QN + "table_1_"+ getRandomName());
        table_1.setRelationshipAttribute(REL_MEANINGS, Collections.singletonList(new AtlasEntityHeader(TYPE_TERM, term_0.getGuid(), null)));
        table_1 = createAndGetEntity(table_1);

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossarySourceQname);
        verifyESInLoop(term_0.getGuid(), mapOf(ES_GLOSSARY, glossarySourceQname));

        Object meanings = table_0.getRelationshipAttribute(REL_MEANINGS);
        assertNotNull(meanings);
        assertEquals("ACTIVE", ((List<HashMap>) meanings).get(0).get("relationshipStatus"));
        meanings = table_1.getRelationshipAttribute(REL_MEANINGS);
        assertNotNull(meanings);
        assertEquals("ACTIVE", ((List<HashMap>) meanings).get(0).get("relationshipStatus"));

        String term_0_name = (String) term_0.getAttribute(NAME);

        verifyESInLoop(table_0.getGuid(), mapOf(ES_MEANINGS, term_0_Qname, ES_MEANING_NAMES, term_0_name, ES_MEANING_TEXT, term_0_name));
        verifyESInLoop(table_1.getGuid(), mapOf(ES_MEANINGS, term_0_Qname, ES_MEANING_NAMES, term_0_name, ES_MEANING_TEXT, term_0_name));

        term_0.setAttribute(NAME, "term_0_updated_name");
        term_0.getRelationshipAttributes().remove("assignedEntities");
        term_0.setRelationshipAttribute(REL_ANCHOR, new AtlasEntityHeader(TYPE_GLOSSARY, glossaryTarget.getGuid(), null));
        term_0 = updateAndGetEntity(term_0);

        term_0_Qname = (String) term_0.getAttribute(QUALIFIED_NAME);
        table_0 = TestUtil.getEntity(table_0.getGuid());
        table_1 = TestUtil.getEntity(table_1.getGuid());

        meanings = table_0.getRelationshipAttribute(REL_MEANINGS);
        assertNotNull(meanings);
        assertEquals("ACTIVE", ((List<HashMap>) meanings).get(0).get("relationshipStatus"));
        meanings = table_1.getRelationshipAttribute(REL_MEANINGS);
        assertNotNull(meanings);
        assertEquals("ACTIVE", ((List<HashMap>) meanings).get(0).get("relationshipStatus"));

        //Thread.sleep(65000); //wait for 65 seconds to task to complete

        assertEquals(term_0_Qname, getNanoid(term_0_Qname) + "@" + glossaryTargetQname);
        verifyESInLoop(term_0.getGuid(), mapOf(ES_GLOSSARY, glossaryTargetQname));

        verifyESInLoop(table_0.getGuid(), mapOf(
                ES_MEANINGS, getNanoid(term_0_Qname) + "@" + glossaryTargetQname,
                ES_MEANING_NAMES, "term_0_updated_name",
                ES_MEANING_TEXT, "term_0_updated_name"
                ));
        verifyESInLoop(table_1.getGuid(), mapOf(
                ES_MEANINGS, getNanoid(term_0_Qname) + "@" + glossaryTargetQname,
                ES_MEANING_NAMES, "term_0_updated_name",
                ES_MEANING_TEXT, "term_0_updated_name"
                ));



        LOG.info(">> test__meaningsAttribute");
    }

    private static AtlasEntity getEntity(String type, String name) {
        AtlasEntity entity = new AtlasEntity(type);
        entity.setAttribute(NAME, name);
        entity.setAttribute(QUALIFIED_NAME, name);
        return entity;
    }
}