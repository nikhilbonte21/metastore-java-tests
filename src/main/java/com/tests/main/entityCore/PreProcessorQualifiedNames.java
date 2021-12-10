package com.tests.main.entityCore;

import com.tests.main.entityRest.CategoryEntityRest;
import com.tests.main.glossary.client.AtlasClientV2;
import com.tests.main.glossary.client.ClientBuilder;
import com.tests.main.glossary.tests.TestsMain;
import com.tests.main.glossary.utils.ESUtils;
import com.tests.main.glossary.utils.TestUtils;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.glossary.utils.TestUtils.*;
import static org.junit.Assert.*;

public class PreProcessorQualifiedNames {//implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(PreProcessorQualifiedNames.class);

    public static void main(String[] args) throws Exception {
        try {
            new PreProcessorQualifiedNames().run();
        } finally {
            cleanUpAll();
            ESUtils.close();
        }
    }

    //@Override
    public void run() throws Exception {
        LOG.info("Running PreProcessorQualifiedNames tests");

        long start = System.currentTimeMillis();
        try {
            /*testCreateQueryCollection();
            testCreateQuery();
            testCreateQueryCustomClient();
            testCreateQueryFolder();
            testCreateQueryFolderCustomClient();*/

            testAllWithBulkCreate();


        } catch (Exception e){
            throw e;
        } finally {
            LOG.info("Completed running PreProcessorQualifiedNames tests, took {} seconds", (System.currentTimeMillis() - start) / 1000 );
        }
    }

    private static void testCreateQueryCollection() throws Exception {
        LOG.info(">> testCreateQueryCollection");

        AtlasEntity collection = createEntity(getAtlasEntity(TYPE_QUERY_COLLECTION, "collection_0"));
        assertTrue(getQualifiedName(collection).startsWith(PREFIX_QUERY_QN));
        String collectionQulifiedName = getQualifiedName(collection);

        LOG.info("<< testCreateQueryCollection");
    }

    private static void testCreateQuery() throws Exception {
        LOG.info(">> testCreateQuery");

        AtlasEntity collection = createEntity(getAtlasEntity(TYPE_QUERY_COLLECTION, "collection_0"));
        assertTrue(getQualifiedName(collection).startsWith(PREFIX_QUERY_QN));
        String collectionQulifiedName = getQualifiedName(collection);

        boolean failed = false;
        try {
            AtlasEntity entity = getAtlasEntityQuery("query_0", "").getEntity();
            entity.setRelationshipAttribute(PARENT, getObjectId(collection.getGuid(), TYPE_QUERY_COLLECTION));
            TestUtils.createEntity(entity);
        } catch (AtlasServiceException exception) {
            assertEquals(exception.getStatus().getStatusCode(),400);
            assertTrue(exception.getMessage().contains("ATLAS-400-00-02B"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }


        AtlasEntity query_0 = getAtlasEntityQuery("query_0", collectionQulifiedName).getEntity();
        query_0.setRelationshipAttribute(PARENT, getObjectId(collection.getGuid(), TYPE_QUERY_COLLECTION));
        query_0 = createEntity(query_0);

        String query_0_qName = getQualifiedName(query_0);
        String query_0_nanoid = getQueryNanoId(query_0_qName);
        assertEquals(collectionQulifiedName + "/query/admin/" + query_0_nanoid, query_0_qName);

        LOG.info("<< testCreateQuery");
    }

    private static void testCreateQueryCustomClient() throws Exception {
        LOG.info(">> testCreateQueryCustomClient");

        AtlasEntity collection = createEntity(getAtlasEntity(TYPE_QUERY_COLLECTION, "collection_0"));
        assertTrue(getQualifiedName(collection).startsWith(PREFIX_QUERY_QN));
        String collectionQulifiedName = getQualifiedName(collection);

        AtlasClientV2 clientCustom = ClientBuilder.buildCustomClientLocal("nikhil", "admin");

        AtlasEntity query_1 = getAtlasEntityQuery("query_1", collectionQulifiedName).getEntity();
        query_1.setRelationshipAttribute(PARENT, getObjectId(collection.getGuid(), TYPE_QUERY_COLLECTION));
        EntityMutationResponse response = clientCustom.createEntity(new AtlasEntity.AtlasEntityWithExtInfo(query_1));
        query_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        String query_1_qName = getQualifiedName(query_1);
        String query_1_nanoid = getQueryNanoId(query_1_qName);
        assertEquals(collectionQulifiedName + "/query/nikhil/" + query_1_nanoid, query_1_qName);

        clientCustom.close();

        LOG.info("<< testCreateQueryCustomClient");
    }

    private static void testCreateQueryFolder() throws Exception {
        LOG.info(">> testCreateQueryFolder");

        AtlasEntity collection = createEntity(getAtlasEntity(TYPE_QUERY_COLLECTION, "collection_0"));
        assertTrue(getQualifiedName(collection).startsWith(PREFIX_QUERY_QN));
        String collectionQulifiedName = getQualifiedName(collection);

        boolean failed = false;
        try {
            AtlasEntity entity = getAtlasEntityQuery("folder_0", "").getEntity();
            entity.setRelationshipAttribute(PARENT, getObjectId(collection.getGuid(), TYPE_QUERY_COLLECTION));
            TestUtils.createEntity(entity);
        } catch (AtlasServiceException exception) {
            assertEquals(exception.getStatus().getStatusCode(),400);
            assertTrue(exception.getMessage().contains("ATLAS-400-00-02B"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        AtlasEntity folder_0 = getAtlasEntityQueryFolder("folder_0", collectionQulifiedName).getEntity();
        folder_0.setRelationshipAttribute(PARENT, getObjectId(collection.getGuid(), TYPE_QUERY_COLLECTION));
        folder_0 = createEntity(folder_0);

        String folder_0_qName = getQualifiedName(folder_0);
        String folder_0_nanoid = getQueryNanoId(folder_0_qName);
        assertEquals(collectionQulifiedName + "/folder/admin/" + folder_0_nanoid, folder_0_qName);

        LOG.info("<< testCreateQueryFolder");
    }

    private static void testCreateQueryFolderCustomClient() throws Exception {
        LOG.info(">> testCreateQueryFolderCustomClient");

        AtlasEntity collection = createEntity(getAtlasEntity(TYPE_QUERY_COLLECTION, "collection_0"));
        assertTrue(getQualifiedName(collection).startsWith(PREFIX_QUERY_QN));
        String collectionQulifiedName = getQualifiedName(collection);

        AtlasClientV2 clientCustom = ClientBuilder.buildCustomClientLocal("nikhil", "admin");

        AtlasEntity folder_1 = getAtlasEntityQueryFolder("folder_1", collectionQulifiedName).getEntity();
        folder_1.setRelationshipAttribute(PARENT, getObjectId(collection.getGuid(), TYPE_QUERY_COLLECTION));
        EntityMutationResponse response = clientCustom.createEntity(new AtlasEntity.AtlasEntityWithExtInfo(folder_1));
        folder_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        String folder_1_qName = getQualifiedName(folder_1);
        String folder_1_nanoid = getQueryNanoId(folder_1_qName);
        assertEquals(collectionQulifiedName + "/folder/nikhil/" + folder_1_nanoid, folder_1_qName);

        clientCustom.close();

        LOG.info("<< testCreateQueryFolderCustomClient");
    }

    private static void testAllWithBulkCreate() throws Exception {
        LOG.info(">> testAllWithBulkCreate");


        AtlasEntity collection = getAtlasEntity(TYPE_QUERY_COLLECTION, "collection_0").getEntity();
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo0 = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo0.addEntity(collection);
        EntityMutationResponse response = createEntitiesBulk(entitiesWithExtInfo0);

        assertNotNull(response);
        assertNotNull(response.getCreatedEntities());
        assertEquals(1, response.getCreatedEntities().size());

        collection = getEntity(response.getGuidAssignments().get(collection.getGuid()));
        String collectionQName = getQualifiedName(collection);

        AtlasEntity folder_0 = getAtlasEntityQueryFolder("folder_0", collectionQName).getEntity();
        folder_0.setRelationshipAttribute(PARENT, getObjectId(collection.getGuid(), TYPE_QUERY_COLLECTION));
        AtlasEntity folder_1 = getAtlasEntityQueryFolder("folder_1", collectionQName).getEntity();
        folder_1.setRelationshipAttribute(PARENT, getObjectId(collection.getGuid(), TYPE_QUERY_COLLECTION));

        AtlasEntity query_0 = getAtlasEntityQuery("query_0", collectionQName).getEntity();
        query_0.setRelationshipAttribute(PARENT, getObjectId(folder_0.getGuid(), TYPE_QUERY_FOLDER));
        AtlasEntity query_1 = getAtlasEntityQuery("query_1", collectionQName).getEntity();
        query_1.setRelationshipAttribute(PARENT, getObjectId(folder_1.getGuid(), TYPE_QUERY_FOLDER));


        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(query_0);
        entitiesWithExtInfo.addEntity(query_1);
        entitiesWithExtInfo.addEntity(folder_0);
        entitiesWithExtInfo.addEntity(folder_1);

        response = createEntitiesBulk(entitiesWithExtInfo);

        assertNotNull(response);
        assertNotNull(response.getCreatedEntities());
        assertEquals(4, response.getCreatedEntities().size());

        folder_0 = getEntity(response.getGuidAssignments().get(folder_0.getGuid()));
        folder_1 = getEntity(response.getGuidAssignments().get(folder_1.getGuid()));
        query_0 = getEntity(response.getGuidAssignments().get(query_0.getGuid()));
        query_1 = getEntity(response.getGuidAssignments().get(query_1.getGuid()));

        assertTrue(collectionQName.startsWith(PREFIX_QUERY_QN));

        assertTrue(getQualifiedName(folder_0).startsWith(collectionQName + "/folder/admin/"));
        assertTrue(getQualifiedName(folder_1).startsWith(collectionQName + "/folder/admin/"));
        assertTrue(getQualifiedName(query_0).startsWith(collectionQName + "/query/admin/"));
        assertTrue(getQualifiedName(query_1).startsWith(collectionQName + "/query/admin/"));




        LOG.info("<<> testAllWithBulkCreate");
    }

    private static AtlasEntity createEntity(AtlasEntity atlasEntity) throws Exception {
        return createEntity(new AtlasEntity.AtlasEntityWithExtInfo(atlasEntity));
    }

    private static AtlasEntity createEntity(AtlasEntity.AtlasEntityWithExtInfo atlasEntityWithExtInfo) throws Exception {
        EntityMutationResponse response = TestUtils.createEntity(atlasEntityWithExtInfo);
        Thread.sleep(1000);
        return getEntity(response.getCreatedEntities().get(0).getGuid());
    }

    private static String getQueryNanoId(String qName) {
        String[] split = qName.split("/");
        return split[split.length-1];
    }
}
