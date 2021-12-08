package com.tests.main.entityCore;

import com.tests.main.glossary.tests.TestsMain;
import com.tests.main.glossary.tests.category.OldCategoryEntityRest;
import com.tests.main.glossary.utils.ESUtils;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.tests.main.glossary.utils.TestUtils.*;
import static org.junit.Assert.*;

public class RelationshipSameVertex implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(OldCategoryEntityRest.class);


    public static void main(String[] args) throws Exception {
        try {
            new RelationshipSameVertex().run();
        } finally {
            cleanUpAll();
            ESUtils.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running RelationshipSameVertex tests");

        long start = System.currentTimeMillis();
        try {
            testCreateCategoryWithItselfAsParent();
            testCreateRelationWithSameVertexAtOtherEnd();

        } catch (Exception e){
            throw e;
        } finally {
            LOG.info("Completed running RelationshipSameVertex tests, took {} seconds", (System.currentTimeMillis() - start) / 1000 );
        }
    }

    private static void testCreateCategoryWithItselfAsParent() throws Exception {
        LOG.info(">> testCreateCategoryWithItselfAsParent");

        AtlasEntity glossary = createGlossary();
        String gloGUID = glossary.getGuid();
        String gloQname = (String) glossary.getAttribute(QUALIFIED_NAME);

        AtlasEntity category_0 = getEntity(createCategory(gloGUID, null).getGuid());
        category_0.setRelationshipAttribute("parentCategory", getParentCategoryObjectId(category_0.getGuid()));

        boolean failed = false;
        try {
            createEntity(category_0);
        } catch (AtlasServiceException exception) {
            assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-409-00-014"));
            assertTrue(exception.getMessage().contains(category_0.getGuid()));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        LOG.info("<< testCreateCategoryWithItselfAsParent");
    }

    private static void testCreateRelationWithSameVertexAtOtherEnd() throws Exception {
        LOG.info(">> testCreateRelationWithSameVertexAtOtherEnd");

        AtlasEntity glossary = createGlossary();
        String gloGUID = glossary.getGuid();
        String gloQname = (String) glossary.getAttribute(QUALIFIED_NAME);

        AtlasEntity category_0 = getEntity(createCategory(gloGUID, null).getGuid());
        category_0.setRelationshipAttribute("parentCategory", getParentCategoryObjectId(category_0.getGuid()));

        AtlasRelationship relationship = new AtlasRelationship("AtlasGlossaryCategoryHierarchyLink");
        relationship.setEnd1(getObjectId(category_0.getGuid(), TYPE_CATEGORY));
        relationship.setEnd2(getObjectId(category_0.getGuid(), TYPE_CATEGORY));

        boolean failed = false;
        try {
            createRelationship(relationship);
        } catch (AtlasServiceException exception) {
            assertEquals(exception.getStatus().getStatusCode(),409);
            assertTrue(exception.getMessage().contains("ATLAS-409-00-014"));
            assertTrue(exception.getMessage().contains(category_0.getGuid()));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        category_0 = getEntity(category_0.getGuid());
        Map parent = getParentRelationshipAttribute(category_0);
        assertNull(parent);

        LOG.info("<< testCreateRelationWithSameVertexAtOtherEnd");
    }

}
