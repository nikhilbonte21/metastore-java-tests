package com.tests.main.tests.glossary.tests.terms;



import org.apache.atlas.model.glossary.AtlasGlossary;
import com.tests.main.tests.glossary.models.AtlasGlossaryTerm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static com.tests.main.utils.TestUtil.*;

@Deprecated
public class Term {
    private static final Logger LOG = LoggerFactory.getLogger(Term.class);


    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws  Exception{
        LOG.info("Running Terms tests");

        long start = System.currentTimeMillis();
        try {
           /* testCreateTerm();
            testUpdateTerm();
            testCreateDupTerm();
            testUpdateDupTerm();
            testUpdateTermAnchorNotAllowed();*/

        } catch (Exception e){
            throw e;
        } finally {
            //cleanUpAll();
            LOG.info("Completed running Terms tests, took {} seconds", (System.currentTimeMillis() - start) / 1000 );
        }
    }

   /* private static void testCreateTerm() throws Exception {
        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel());
        AtlasGlossaryTerm term_0_toCreate = getTermModel(glossary_0.getGuid());
        term_0_toCreate.setOtherAttribute("testName", "random_tNAme");
        AtlasGlossaryTerm term_0 = createTerm(term_0_toCreate);

        AtlasGlossary glossary_1 = createGlossary(getGlossaryModel());
        AtlasGlossaryTerm term_1 = createTerm(getTermModel(glossary_1.getGuid()));
        AtlasGlossaryTerm term_2 = createTerm(getTermModel(glossary_1.getGuid()));


        //assertEquals(term_0.getOtherAttributes());
        assertEquals(term_0.getQualifiedName(), getNanoid(term_0.getQualifiedName()) + "@" + glossary_0.getQualifiedName());
        assertEquals(term_1.getQualifiedName(), getNanoid(term_1.getQualifiedName()) + "@" + glossary_1.getQualifiedName());
        assertEquals(term_2.getQualifiedName(), getNanoid(term_2.getQualifiedName()) + "@" + glossary_1.getQualifiedName());

        AtlasGlossary glossary_0_f = getGlossary(glossary_0.getGuid());
        AtlasGlossary glossary_1_f = getGlossary(glossary_1.getGuid());
        assertEquals(glossary_0_f.getTerms().size(), 1);
        assertEquals(glossary_1_f.getTerms().size(), 2);
        assertEquals(glossary_1_f.getTerms().size(), 2);
    }

    private static void testUpdateTerm() throws Exception {
        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel());
        AtlasGlossaryTerm term_0 = createTerm(getTermModel(glossary_0.getGuid()));


        assertEquals(term_0.getQualifiedName(), getNanoid(term_0.getQualifiedName()) + "@" + glossary_0.getQualifiedName());

        AtlasGlossaryTerm termToUpdate = getTerm(term_0.getGuid());
        termToUpdate.setExamples(Collections.singletonList("ex_0"));
        updateTerm(term_0.getGuid(), termToUpdate);


        termToUpdate = getTerm(term_0.getGuid());
        termToUpdate.setLongDescription("update desc");
        updateTerm(term_0.getGuid(), termToUpdate);

        assertEquals(getGlossary(glossary_0.getGuid()).getTerms().size(), 1);
        assertEquals(term_0.getQualifiedName(), getNanoid(term_0.getQualifiedName()) + "@" + glossary_0.getQualifiedName());
    }

    private static void testCreateDupTerm() throws Exception {
        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel());
        String termName = getRandomName();
        createTerm(getTermModel(termName, glossary_0.getGuid()));

        AtlasGlossary glossary_1 = createGlossary(getGlossaryModel());
        //same term name in different glossary -> allowed
        createTerm(getTermModel(termName, glossary_1.getGuid()));
        Thread.sleep(2000);

        boolean failed= false;
        try {
            //same term name in same glossary -> not allowed
            createTerm(getTermModel(termName, glossary_1.getGuid()));

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
        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel());
        String termName = getRandomName();
        AtlasGlossaryTerm term_0 = createTerm(getTermModel(termName, glossary_0.getGuid()));
        AtlasGlossaryTerm term_1 = createTerm(getTermModel(getRandomName(), glossary_0.getGuid()));

        boolean failed= false;
        try {
            Thread.sleep(2000);
            //same term name in same glossary -> not allowed
            AtlasGlossaryTerm updateTerm = getTerm(term_1.getGuid());
            updateTerm.setName(termName);
            updateTerm(updateTerm.getGuid(), updateTerm);

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

    *//*NOTE

    This will fail as we are not supporting changing of glossary for Terms & Categories
    But, https://linear.app/atlanproduct/issue/META-2694/use-uuid-in-qualifiedname-instead-of-name
    Feature has required changes for changing of acnhor & reevaluating qualifiedName for both Terms & Categories
    If chaging anchor is allowed, in that case this test must poss

     *//*
    private static void testUpdateTermAnchor() throws Exception {
        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel());
        AtlasGlossaryTerm term_0 = createTerm(getTermModel(glossary_0.getGuid()));

        AtlasGlossary glossary_1 = createGlossary(getGlossaryModel());
        AtlasGlossaryTerm term_1 = createTerm(getTermModel(glossary_1.getGuid()));

        //updating term
        AtlasGlossaryTerm toUpdateTerm = new AtlasGlossaryTerm(term_1);
        toUpdateTerm.setAnchor(getGlossaryHeader(glossary_0.getGuid()));
        updateTerm(toUpdateTerm.getGuid(), toUpdateTerm);

        assertEquals(term_0.getQualifiedName(), getNanoid(term_0.getQualifiedName()) + "@" + glossary_0.getQualifiedName());
        assertEquals(term_1.getQualifiedName(), getNanoid(term_1.getQualifiedName()) + "@" + glossary_1.getQualifiedName());

    }

    private static void testUpdateTermAnchorNotAllowed() throws Exception {
        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel());
        AtlasGlossaryTerm term_0 = createTerm(getTermModel(glossary_0.getGuid()));

        AtlasGlossary glossary_1 = createGlossary(getGlossaryModel());
        AtlasGlossaryTerm term_1 = createTerm(getTermModel(glossary_1.getGuid()));

        //updating term
        AtlasGlossaryTerm toUpdateTerm = new AtlasGlossaryTerm(term_1);
        toUpdateTerm.setAnchor(getGlossaryHeader(glossary_0.getGuid()));

        boolean failed = false;
        try {
            updateTerm(toUpdateTerm.getGuid(), toUpdateTerm);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-400-00-0010"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }
    }*/

    private static String getNanoid(String qualifiedName){
        String[] sp_a = qualifiedName.split("@");
        String[] sp_b = sp_a[0].split("\\.");

        return sp_b[sp_b.length-1];
    }
}
