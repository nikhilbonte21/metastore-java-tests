package com.tests.main.entityRest;

import com.tests.main.glossary.tests.TestsMain;
import com.tests.main.glossary.utils.ESUtils;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.glossary.utils.TestUtils.*;
import static com.tests.main.glossary.utils.TestUtils.createGlossary;
import static org.junit.Assert.*;


public class GlossaryEntityRest implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryEntityRest.class);

    public static void main(String[] args) throws Exception {
        try {
            new GlossaryEntityRest().run();
        } finally {
            cleanUpAll();
            ESUtils.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running GlossaryEntityRest tests");

        long start = System.currentTimeMillis();
        try {
            testCreateGlossary();
            testCreateGlossaryDupName();
            testUpdateGlossaryDupName();
            testCreateGlossaryDupNameBulk();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running GlossaryEntityRest tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void testCreateGlossary() throws AtlasServiceException {
        LOG.info(">> testCreateGlossary");

        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "glossary_0").getEntity();
        glossary.setAttribute("shortDescription", "Short description");
        glossary.setAttribute("longDescription", "Long description");

        glossary = getEntity(createEntity(glossary).getCreatedEntities().get(0).getGuid());

        String glossaryQn = getQualifiedName(glossary);


        //update Glossary
        String updatedName = getRandomName();
        glossary.setAttribute(QUALIFIED_NAME, "newRandomQn");
        AtlasEntity updateGlossaryEntity = getEntity(createEntity(glossary).getUpdatedEntities().get(0).getGuid());

        assertEquals(getQualifiedName(updateGlossaryEntity), glossaryQn);

        LOG.info("<< testCreateGlossary");
    }

    private static void testCreateGlossaryDupName() throws Exception {
        LOG.info(">> testCreateGlossaryDupName");
        String name = getRandomName();

        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "glossary_0").getEntity();
        glossary.setAttribute(NAME, name);
        AtlasEntity glossary_0 = getEntity(createEntity(glossary).getCreatedEntities().get(0).getGuid());

        glossary = getAtlasEntity(TYPE_GLOSSARY, "glossary_1").getEntity();
        AtlasEntity glossary_1 = getEntity(createEntity(glossary).getCreatedEntities().get(0).getGuid());

        boolean failed= false;
        try {
            Thread.sleep(2000);
            AtlasEntity dupGlossary = getEntity(glossary_1.getGuid());
            dupGlossary.setAttribute(NAME, name);
            createEntity(dupGlossary);

        } catch (AtlasServiceException exception) {
            assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-409-00-007"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        LOG.info("<< testCreateGlossaryDupName");
    }

    private static void testUpdateGlossaryDupName() throws Exception {
        LOG.info(">> testUpdateGlossaryDupName");
        String name = getRandomName();

        AtlasEntity glossary_0 = getAtlasEntity(TYPE_GLOSSARY, "glossary_0").getEntity();
        glossary_0.setAttribute(NAME, name);

        AtlasEntity glossary_1 = getAtlasEntity(TYPE_GLOSSARY, "glossary_1").getEntity();
        glossary_1.setAttribute(NAME, getRandomName());

        createEntity(glossary_0);
        AtlasEntity updatedGlossary = getEntity(createEntity(glossary_1).getCreatedEntities().get(0).getGuid());

        boolean failed= false;
        try {
            Thread.sleep(2000);
            updatedGlossary.setAttribute(NAME, name);
            createEntity(updatedGlossary);

        } catch (AtlasServiceException exception) {
            assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-409-00-007"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        LOG.info("<< testUpdateGlossaryDupName");
    }

    private static void testCreateGlossaryDupNameBulk() throws Exception {
        LOG.info(">> testCreateGlossaryDupNameBulk");
        String name = getRandomName();

        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();

        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "glossary_0").getEntity();
        glossary.setAttribute(NAME, name);
        entitiesWithExtInfo.addEntity(glossary);

        AtlasEntity dupGlossary = getAtlasEntity(TYPE_GLOSSARY, "glossary_0").getEntity();
        dupGlossary.setAttribute(NAME, name);
        entitiesWithExtInfo.addEntity(dupGlossary);

        boolean failed= false;
        try {
            Thread.sleep(2000);
            createEntitiesBulk(entitiesWithExtInfo);

        } catch (AtlasServiceException exception) {
            assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-409-00-007"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        LOG.info("<< testCreateGlossaryDupNameBulk");
    }


    private static AtlasEntity createGlossary(String name) throws Exception {
        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, name).getEntity();

        EntityMutationResponse reps = createEntity(glossary);
        return getEntity(reps.getCreatedEntities().get(0).getGuid());
    }

}