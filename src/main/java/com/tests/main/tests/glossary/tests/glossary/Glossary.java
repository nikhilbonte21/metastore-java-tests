package com.tests.main.tests.glossary.tests.glossary;


import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.utils.TestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class Glossary {
    private static final Logger LOG = LoggerFactory.getLogger(Glossary.class);

    private static long SLEEP = 0;

    public static void main(String[] args) throws Exception {
        try {
            new Glossary().run();
            //TestRunner.runTests(SanityAttributesMutations.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    public static void run() throws Exception {
        LOG.info("Running Glossary tests");

        long start = System.currentTimeMillis();
        try {
            testCreateGlossary();
            testCreateGlossaryDupName();
            testUpdateGlossaryDupName();
            testLexoRank();

        } catch (Exception e) {
            throw e;
        } finally {
            cleanUpAll();
            LOG.info("Completed running Glossary tests, took {} seconds", (System.currentTimeMillis() - start) / 1000 );
        }
    }

    private static void testCreateGlossary() throws Exception {
        LOG.info(">> testCreateGlossary");

        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_0");
        String glossaryGuid = createEntity(glossary).getGuidAssignments().values().iterator().next();
        sleep(SLEEP);
        String unExpectedQn = (String) glossary.getAttribute(QUALIFIED_NAME);

        glossary = getEntity(glossaryGuid);
        assertFalse(unExpectedQn.equals(glossary.getAttribute(QUALIFIED_NAME)));

        //update Glossary
        String updatedName = getRandomName();
        glossary.setAttribute(NAME, updatedName);
        updateEntity(glossary);
        sleep(SLEEP);

        glossary = getEntity(glossaryGuid);
        assertEquals(updatedName, glossary.getAttribute(NAME));

        LOG.info("<< testCreateGlossary");
    }

    private static void testCreateGlossaryDupName() throws Exception {
        LOG.info(">> testCreateGlossaryDupQN");

        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_0");
        String glossaryGuid = createEntity(glossary).getGuidAssignments().values().iterator().next();
        sleep(SLEEP);

        boolean failed= false;
        try {
            AtlasEntity dupGlossary = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_0");
            dupGlossary.setAttribute(NAME, glossary.getAttribute(NAME));
            updateEntity(dupGlossary);

        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-409-00-007"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        LOG.info("<< testCreateGlossaryDupQN");
    }

    private static void testUpdateGlossaryDupName() throws Exception {
        LOG.info(">> testUpdateGlossaryDupQN");
        String name = getRandomName();

        AtlasEntity glossary_0 = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_0");
        String glossaryGuid_0 = createEntity(glossary_0).getGuidAssignments().values().iterator().next();

        AtlasEntity glossary_1 = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_1");
        String glossaryGuid_1 = createEntity(glossary_1).getGuidAssignments().values().iterator().next();
        sleep(SLEEP);

        boolean failed= false;
        try {
            Thread.sleep(2000);
            glossary_1.setAttribute(NAME, glossary_0.getAttribute(NAME));
            updateEntity(glossary_1);

        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-409-00-007"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        LOG.info("<< testUpdateGlossaryDupQN");
    }

    private static void testLexoRank() throws Exception {
        LOG.info(">> testLexoRank");

        //String lastLexoId = getLastLexoId();

        AtlasEntity glossary_0 = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_0");
        String glossaryGuid_0 = createEntity(glossary_0).getGuidAssignments().values().iterator().next();

        AtlasEntity glossary_1 = getAtlasEntity(TYPE_GLOSSARY, "test_glossary_1");
        String glossaryGuid_1 = createEntity(glossary_1).getGuidAssignments().values().iterator().next();
        sleep(SLEEP);



        glossary_0 = getEntity(glossaryGuid_0);
        assertEquals(glossary_0.getAttribute("lexicographicalSortOrder"), "0|10000g:");

        LOG.info(">> testLexoRank");
    }
}
