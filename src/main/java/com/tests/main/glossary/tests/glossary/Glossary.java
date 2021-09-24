package com.tests.main.glossary.tests.glossary;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.glossary.utils.GlossaryUtils.*;
import static org.junit.Assert.assertEquals;

public class Glossary {
    private static final Logger LOG = LoggerFactory.getLogger(Glossary.class);

    public Glossary(){

    }

    public void runTests() throws AtlasServiceException {
        LOG.info("Running Glossary tests");
        testCreateGlossary();

        LOG.info("Completed running Glossary tests");
    }

    private void testCreateGlossary() throws AtlasServiceException {
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
}
