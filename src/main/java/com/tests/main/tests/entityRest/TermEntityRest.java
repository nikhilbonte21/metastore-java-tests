package com.tests.main.tests.entityRest;

import com.tests.main.tests.glossary.models.AtlasGlossaryTerm;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;


import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static com.tests.main.utils.TestUtil.*;
import static org.junit.Assert.*;


public class TermEntityRest implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(TermEntityRest.class);

    public static void main(String[] args) throws Exception {
        try {
            new TermEntityRest().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running TermEntityRest tests");

        long start = System.currentTimeMillis();
        try {
            testCreateTerm();
            testUpdateTerm();
            testCreateDupTerm();
            testUpdateDupTerm();
            testUpdateTermAnchorNotAllowed();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running TermEntityRest tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void testCreateTerm() throws Exception {
        AtlasEntity glossary_0 = createGlossary("glossary_0");
        Thread.sleep(2000);

        glossary_0 = getEntity(glossary_0.getGuid());
        AtlasEntity term_0 = createTerm("term_0", glossary_0.getGuid());

        AtlasEntity glossary_1 = createGlossary("glossary_1");
        AtlasEntity term_1 = createTerm("term_0", glossary_1.getGuid());
        AtlasEntity term_2 = createTerm("term_0", glossary_1.getGuid());


        assertEquals(getQualifiedName(term_0), getNanoid(getQualifiedName(term_0)) + "@" + getQualifiedName(glossary_0));
        assertEquals(getQualifiedName(term_1), getNanoid(getQualifiedName(term_1)) + "@" + getQualifiedName(glossary_1));
        assertEquals(getQualifiedName(term_2), getNanoid(getQualifiedName(term_2)) + "@" + getQualifiedName(glossary_1));

        AtlasGlossary glossary_0_f = getGlossary(glossary_0.getGuid());
        AtlasGlossary glossary_1_f = getGlossary(glossary_1.getGuid());
        assertEquals(glossary_0_f.getTerms().size(), 1);
        assertEquals(glossary_1_f.getTerms().size(), 2);
        assertEquals(glossary_1_f.getTerms().size(), 2);
    }

    private static void testUpdateTerm() throws Exception {
        AtlasEntity glossary_0 = createGlossary("glossary_0");
        AtlasEntity term_0 = createTerm("term_0", glossary_0.getGuid());


        assertEquals(getQualifiedName(term_0), getNanoid(getQualifiedName(term_0)) + "@" + getQualifiedName(glossary_0));

        AtlasGlossaryTerm termToUpdate = getTerm(term_0.getGuid());
        termToUpdate.setExamples(Collections.singletonList("ex_0"));
        updateTerm(term_0.getGuid(), termToUpdate);


        termToUpdate = getTerm(term_0.getGuid());
        termToUpdate.setLongDescription("update desc");
        updateTerm(term_0.getGuid(), termToUpdate);

        assertEquals(getGlossary(glossary_0.getGuid()).getTerms().size(), 1);
        assertEquals(getQualifiedName(term_0), getNanoid(getQualifiedName(term_0)) + "@" + getQualifiedName(glossary_0));
    }

    private static void testCreateDupTerm() throws Exception {
        AtlasEntity glossary_0 = createGlossary("glossary_0");
        AtlasEntity term_0 = createTerm("term_0", glossary_0.getGuid());

        createTerm("term_0", glossary_0.getGuid());

        AtlasEntity glossary_1 = createGlossary("glossary_1");
        //same term name in different glossary -> allowed
        AtlasEntity entity = createTerm("term_0", glossary_1.getGuid());
        Thread.sleep(2000);

        boolean failed= false;
        try {
            //same term name in same glossary -> not allowed
            AtlasEntity term = getAtlasEntityExt(TYPE_TERM, "").getEntity();
            term.setAttribute(NAME, entity.getAttribute(NAME));
            term.setRelationshipAttribute("anchor", getObjectId(glossary_1.getGuid(), TYPE_GLOSSARY));
            createEntity(term);

        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-409-00-009"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("Test testCreateDupTerm should have failed");
            }
        }
    }

    private static void testUpdateDupTerm() throws Exception {
        AtlasEntity glossary_0 = createGlossary("glossary_0");
        AtlasEntity term = createTerm("term_0", glossary_0.getGuid());

        String termName = getRandomName();
        AtlasEntity term_0 = createTerm(termName, glossary_0.getGuid());
        AtlasEntity term_1 = createTerm(getRandomName(), glossary_0.getGuid());;

        boolean failed= false;
        try {
            Thread.sleep(2000);
            //same term name in same glossary -> not allowed
            AtlasEntity updateTerm = getEntity(term_1.getGuid());
            updateTerm.setAttribute(NAME, term_0.getAttribute(NAME));
            createEntity(updateTerm);

        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-409-00-009"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("Test testCreateDupTerm should have failed");
            }
        }
    }

    private static void testUpdateTermAnchorNotAllowed() throws Exception {
        AtlasEntity glossary_0 = createGlossary("glossary_0");
        AtlasEntity term_0 = createTerm("term_0", glossary_0.getGuid());

        AtlasEntity glossary_1 = createGlossary("glossary_1");
        AtlasEntity term_1 = createTerm("term_1", glossary_1.getGuid());

        //updating term
        Thread.sleep(2000);
        AtlasEntity toUpdateTerm = getEntity(term_0.getGuid());
        toUpdateTerm.setRelationshipAttribute("anchor", getObjectId(glossary_1.getGuid(), TYPE_GLOSSARY));

        boolean failed = false;
        try {
            createEntity(toUpdateTerm);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-400-00-0010"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }
    }


    /*NOTE

  This will fail as we are not supporting changing of glossary for Terms & Categories
  But, https://linear.app/atlanproduct/issue/META-2694/use-uuid-in-qualifiedname-instead-of-name
  Feature has required changes for changing of acnhor & reevaluating qualifiedName for both Terms & Categories
  If chaging anchor is allowed, in that case this test must poss

   */
    private static void testUpdateTermAnchor() throws Exception {
        AtlasEntity glossary_0 = createGlossary("glossary_0");
        AtlasEntity term_0 = createTerm("term_0", glossary_0.getGuid());

        AtlasEntity glossary_1 = createGlossary("glossary_1");
        AtlasEntity term_1 = createTerm("term_1", glossary_1.getGuid());

        //updating term
        AtlasEntity toUpdateTerm = getEntity(term_1.getGuid());
        toUpdateTerm.setRelationshipAttribute("anchor", getObjectId(glossary_0.getGuid(), TYPE_GLOSSARY));
        createEntity(toUpdateTerm);

        assertEquals(getQualifiedName(term_0), getNanoid(getQualifiedName(term_0)) + "@" + getQualifiedName(glossary_0));
        assertEquals(getQualifiedName(term_1), getNanoid(getQualifiedName(term_1)) + "@" + getQualifiedName(glossary_1));

    }

    private static AtlasEntity createGlossary(String name) throws Exception {
        AtlasEntity glossary = getAtlasEntityExt(TYPE_GLOSSARY, name).getEntity();

        EntityMutationResponse reps = createEntity(glossary);
        return getEntity(reps.getCreatedEntities().get(0).getGuid());
    }

    private static AtlasEntity createTerm(String name, String gloGuid) throws Exception {
        AtlasEntity term = getAtlasEntityExt(TYPE_TERM, name).getEntity();
        term.setRelationshipAttribute("anchor", getObjectId(gloGuid, TYPE_GLOSSARY));

        EntityMutationResponse reps = createEntity(term);

        return getEntity(reps.getCreatedEntities().get(0).getGuid());
    }

}