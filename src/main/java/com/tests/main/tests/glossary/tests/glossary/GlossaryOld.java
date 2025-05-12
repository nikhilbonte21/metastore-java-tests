package com.tests.main.tests.glossary.tests.glossary;


import org.apache.atlas.model.glossary.AtlasGlossary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.utils.TestUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Deprecated
public class GlossaryOld {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryOld.class);

    public GlossaryOld(){

    }

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        LOG.info("Running Glossary tests");

        long start = System.currentTimeMillis();
        try {
            testCreateGlossary();
            testCreateGlossaryDupName();
            testUpdateGlossaryDupName();
            deleteAllEntities();

        } catch (Exception e) {
            throw e;
        } finally {
            cleanUpAll();
            LOG.info("Completed running Glossary tests, took {} seconds", (System.currentTimeMillis() - start) / 1000 );
        }
    }

    private static void testCreateGlossary() throws Exception {
        LOG.info(">> testCreateGlossary");

        AtlasGlossary glossary = new AtlasGlossary();
        glossary.setName(getRandomName());
        glossary.setShortDescription("Short description");
        glossary.setLongDescription("Long description");

        AtlasGlossary result = createGlossary(glossary);

        String glossaryQn = result.getQualifiedName();


        //update Glossary
        String updatedName = getRandomName();
        glossary.setName(updatedName);
        glossary.setQualifiedName("newRandomQn");
        AtlasGlossary updateGlossaryresult = updateGlossary(result.getGuid(), glossary);

        assertEquals(updateGlossaryresult.getName(), updatedName);
        assertEquals(updateGlossaryresult.getQualifiedName(), glossaryQn);

        LOG.info("<< testCreateGlossary");
    }

    private static void testCreateGlossaryDupName() throws Exception {
        LOG.info(">> testCreateGlossaryDupQN");
        String name = getRandomName();

        AtlasGlossary glossary = new AtlasGlossary();
        glossary.setName(name);
        AtlasGlossary createdGlossary = getGlossary(createGlossary(glossary).getGuid());

        boolean failed= false;
        try {
            Thread.sleep(2000);
            AtlasGlossary dupGlossary = new AtlasGlossary();
            dupGlossary.setName(name);
            createGlossary(dupGlossary);

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

        AtlasGlossary glossary_0 = new AtlasGlossary();
        glossary_0.setName(name);

        AtlasGlossary glossary_1 = new AtlasGlossary();
        glossary_1.setName(getRandomName());

        createGlossary(glossary_0);
        AtlasGlossary updatedGlossary = getGlossary(createGlossary(glossary_1).getGuid());

        boolean failed= false;
        try {
            Thread.sleep(2000);
            updatedGlossary.setName(name);
            updateGlossary(updatedGlossary.getGuid(), updatedGlossary);

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
}
