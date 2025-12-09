package com.tests.main.lineage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static com.tests.main.utils.TestUtil.*;
import static org.junit.Assert.*;


public class DeletePropagationsTaskTest implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(DeletePropagationsTaskTest.class);

    public static void main(String[] args) throws Exception {
        try {
            new DeletePropagationsTaskTest().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running DeletePropagationsTaskTest tests");

        long start = System.currentTimeMillis();
        try {
            //Sequence of methods is important here
            TagToColumnADeleteColumnProcess();
            TagToColumnADeleteColumnProcessVerifyTagPropagations();
            createNewProcessVerifyTagPropagations();
            deleteNewProcess();
            createNewProcessAgain();
            updateProcessToRemoveOutput();
            updateProcessToAddOutputAgain();
            deleteEdge();
            createEdge();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running DeletePropagationsTaskTest tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    static AtlasEntity col_0, col_1, col_2, col_3, col_4, cp_0, cp_1, cp_2, cp_3, cp_0_0, cp_0_1;
    static String tag, tag_1;
    static AtlasClassification classification, classification_1;
    static ObjectMapper objectMapper = new ObjectMapper();

    private static void TagToColumnADeleteColumnProcess() throws Exception {
        LOG.info(">> TagToColumnADeleteColumnProcess");

        /*
        * 2
        *                         -> cp_1 -> col_2
        * col_0  -> cp_0 -> col_1 -> cp_2 -> col_3
        *                         -> cp_3 -> col_4
        *
        * where col_0 has tag_0 with Propagate : true & removeOnDeleteEntity : true
        *
        * Now Delete cp_0
        * */


        EntityMutationResponse response;


        tag = createClassification("tag_0" + getRandomName());
        tag_1 = createClassification("tag_1" + getRandomName());

        col_0 = getAtlasEntityExt(TYPE_COLUMN, "column_0").getEntity();
        classification = new AtlasClassification(tag);
        classification.setPropagate(true);
        classification.setRemovePropagationsOnEntityDelete(true);
        col_0.addClassifications(Collections.singletonList(classification));

        classification_1 = new AtlasClassification(tag_1);
        classification_1.setPropagate(true);
        classification_1.setRemovePropagationsOnEntityDelete(true);
        col_0.addClassifications(Collections.singletonList(classification_1));

        col_1 = getAtlasEntityExt(TYPE_COLUMN, "column_1").getEntity();
        col_2 = getAtlasEntityExt(TYPE_COLUMN, "column_2").getEntity();
        col_3 = getAtlasEntityExt(TYPE_COLUMN, "column_3").getEntity();
        col_4 = getAtlasEntityExt(TYPE_COLUMN, "column_4").getEntity();

        cp_0 = getAtlasEntityExt(TYPE_COLUMN_PROCESS, "column_process_0").getEntity();
        cp_0.setRelationshipAttribute(INPUTS,  getObjectIdsAsList(TYPE_COLUMN, col_0.getGuid()));
        cp_0.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_1.getGuid()));

        cp_1 = getAtlasEntityExt(TYPE_COLUMN_PROCESS, "column_process_1").getEntity();
        cp_1.setRelationshipAttribute(INPUTS,  getObjectIdsAsList(TYPE_COLUMN, col_1.getGuid()));
        cp_1.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_2.getGuid()));

        cp_2 = getAtlasEntityExt(TYPE_COLUMN_PROCESS, "column_process_2").getEntity();
        cp_2.setRelationshipAttribute(INPUTS,  getObjectIdsAsList(TYPE_COLUMN, col_1.getGuid()));
        cp_2.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_3.getGuid()));

        cp_3 = getAtlasEntityExt(TYPE_COLUMN_PROCESS, "column_process_3").getEntity();
        cp_3.setRelationshipAttribute(INPUTS,  getObjectIdsAsList(TYPE_COLUMN, col_1.getGuid()));
        cp_3.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_4.getGuid()));

        AtlasEntity.AtlasEntitiesWithExtInfo extInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        extInfo.addEntity(col_0);
        extInfo.addEntity(col_1);
        extInfo.addEntity(col_2);
        extInfo.addEntity(col_3);
        extInfo.addEntity(col_4);

        extInfo.addEntity(cp_0);
        extInfo.addEntity(cp_1);
        extInfo.addEntity(cp_2);
        extInfo.addEntity(cp_3);

        response = createEntitiesBulk(extInfo);

        sleep();
        col_0 = getEntity(response.getGuidAssignments().get(col_0.getGuid()));
        col_1 = getEntity(response.getGuidAssignments().get(col_1.getGuid()));
        col_2 = getEntity(response.getGuidAssignments().get(col_2.getGuid()));
        col_3 = getEntity(response.getGuidAssignments().get(col_3.getGuid()));
        col_4 = getEntity(response.getGuidAssignments().get(col_4.getGuid()));

        cp_0  = getEntity(response.getGuidAssignments().get(cp_0.getGuid()));
        cp_1  = getEntity(response.getGuidAssignments().get(cp_1.getGuid()));
        cp_2  = getEntity(response.getGuidAssignments().get(cp_2.getGuid()));
        cp_3  = getEntity(response.getGuidAssignments().get(cp_3.getGuid()));


        assertNotNull(cp_0.getClassifications());
        assertEquals(2, cp_0.getClassifications().size());
        AtlasClassification classif = cp_0.getClassifications().get(0);
        assertEquals(col_0.getGuid(), classif.getEntityGuid());

        assertNotNull(col_1.getClassifications());
        assertNotNull(col_2.getClassifications());
        assertNotNull(col_3.getClassifications());
        assertNotNull(col_4.getClassifications());

        assertNotNull(cp_0.getClassifications());
        assertNotNull(cp_1.getClassifications());
        assertNotNull(cp_2.getClassifications());
        assertNotNull(cp_3.getClassifications());

        HashMap<String, String> uniqAttr = new HashMap<>();
        uniqAttr.put(QUALIFIED_NAME, cp_0.getAttribute(QUALIFIED_NAME).toString());

        deleteEntityUniqAttr(TYPE_COLUMN_PROCESS, uniqAttr);

        sleep();
        col_0 = getEntity(col_0.getGuid());
        col_1 = getEntity(col_1.getGuid());
        col_2 = getEntity(col_2.getGuid());
        col_3 = getEntity(col_3.getGuid());
        col_4 = getEntity(col_4.getGuid());

        cp_0 = getEntity(cp_0.getGuid());
        cp_1 = getEntity(cp_1.getGuid());
        cp_2 = getEntity(cp_2.getGuid());
        cp_3 = getEntity(cp_3.getGuid());


        classif = col_0.getClassifications().get(0);
        assertEquals(col_0.getGuid(), classif.getEntityGuid());

        assertNotNull(col_0.getClassifications());

        assertNull(col_1.getClassifications());
        assertNull(col_2.getClassifications());
        assertNull(col_3.getClassifications());
        assertNull(col_4.getClassifications());
        assertNull(cp_0.getClassifications());
        assertNull(cp_1.getClassifications());
        assertNull(cp_2.getClassifications());
        assertNull(cp_3.getClassifications());


        assertEquals(AtlasEntity.Status.DELETED, cp_0.getStatus());

        LOG.info("col_0.guid {}", col_0.getGuid());
        LOG.info(">> TagToColumnADeleteColumnProcess");
    }

    private static void TagToColumnADeleteColumnProcessVerifyTagPropagations() throws Exception {
        LOG.info("<< TagToColumnADeleteColumnProcessVerifyTagPropagations");


        /* 3
         * cp_0 is deleted in previous request
         *
         *                                     -> cp_1 -> col_2
         * col_0  ->  cp_0 (DELETED)  -> col_1 -> cp_2 -> col_3
         *                                     -> cp_3 -> col_4
         *
         * remove tag on col_0
         * Attach again & verify that it does not propagate as cp_0 is deleted
         *
         * */

        removeClassification(col_0.getGuid(), tag);
        removeClassification(col_0.getGuid(), tag_1);

        sleep();
        col_0 = getEntity(col_0.getGuid());

        assertNull(col_0.getClassifications());

        AddTag addTag_0 = new AddTag(col_0.getGuid(), Collections.singletonList(classification));
        AddTag addTag_1 = new AddTag(col_0.getGuid(), Collections.singletonList(classification_1));

        addTag_0.start();
        addTag_1.start();

        Thread.sleep(10000);
        col_0 = getEntity(col_0.getGuid());
        col_1 = getEntity(col_1.getGuid());
        col_2 = getEntity(col_2.getGuid());
        col_3 = getEntity(col_3.getGuid());
        col_4 = getEntity(col_4.getGuid());

        cp_0 = getEntity(cp_0.getGuid());
        cp_1 = getEntity(cp_1.getGuid());
        cp_2 = getEntity(cp_2.getGuid());
        cp_3 = getEntity(cp_3.getGuid());


        assertNotNull(col_0.getClassifications());
        assertNull(col_1.getClassifications());
        assertNull(col_2.getClassifications());
        assertNull(col_3.getClassifications());
        assertNull(col_4.getClassifications());

        assertNull(cp_0.getClassifications());
        assertNull(cp_1.getClassifications());
        assertNull(cp_2.getClassifications());
        assertNull(cp_3.getClassifications());


        LOG.info(">> TagToColumnADeleteColumnProcessVerifyTagPropagations");
    }

    private static void createNewProcessVerifyTagPropagations() throws Exception {
        LOG.info("<< createNewProcessVerifyTagPropagations");


        /*  4
         * cp_0 is deleted in previous request
         * Tag is on col_0 which is not propagated at all
         *                                     -> cp_1 -> col_2
         * col_0  ->  cp_0 (DELETED)  -> col_1 -> cp_2 -> col_3
         *                                     -> cp_3 -> col_4
         *
         *                                     -> cp_1 -> col_2
         * col_0  ->  cp_0 (DELETED)  -> col_1 -> cp_2 -> col_3
         *        ->  cp_0_0                   -> cp_3 -> col_4
         *
         * create new process cp_0_0 between col_0 & col_1
         * This should propagate tag attached to col_0 to all except cp_0 (DELETED)
         *
         * */

        cp_0_0 = getAtlasEntityExt(TYPE_COLUMN_PROCESS, "column_process_0_0").getEntity();
        cp_0_0.setRelationshipAttribute(INPUTS,  getObjectIdsAsList(TYPE_COLUMN, col_0.getGuid()));
        cp_0_0.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_1.getGuid()));

        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(cp_0_0);

        EntityMutationResponse response = createEntitiesBulk(entitiesWithExtInfo);

        assertEquals(1, response.getCreatedEntities().size());

        sleep();
        cp_0_0 = getEntity(response.getGuidAssignments().get(cp_0_0.getGuid()));
        col_0 = getEntity(col_0.getGuid());
        col_1 = getEntity(col_1.getGuid());
        col_2 = getEntity(col_2.getGuid());
        col_3 = getEntity(col_3.getGuid());
        col_4 = getEntity(col_4.getGuid());

        cp_0 = getEntity(cp_0.getGuid());
        cp_1 = getEntity(cp_1.getGuid());
        cp_2 = getEntity(cp_2.getGuid());
        cp_3 = getEntity(cp_3.getGuid());


        assertNotNull(cp_0_0.getClassifications());
        assertNotNull(col_0.getClassifications());
        assertNotNull(col_1.getClassifications());
        assertNotNull(col_2.getClassifications());
        assertNotNull(col_3.getClassifications());
        assertNotNull(col_4.getClassifications());

        assertNull(cp_0.getClassifications());
        assertNotNull(cp_1.getClassifications());
        assertNotNull(cp_2.getClassifications());
        assertNotNull(cp_3.getClassifications());


        LOG.info(">> createNewProcessVerifyTagPropagations");
    }

    private static void deleteNewProcess() throws Exception {
        LOG.info("<< deleteNewProcess");

        /* 5
         *
         *
         *        -> cp_0 (DELETED)            -> cp_1 -> col_2
         * col_0  -> cp_0_0           -> col_1 -> cp_2 -> col_3
         *                                     -> cp_3 -> col_4
         *
         * Delete cp_0_0
         *      Verify tag_0 is attached to col_0 but propagation are removed form all other entities
         * */

        deleteEntities(Collections.singletonList(cp_0_0.getGuid()));

        sleep();
        cp_0_0 = getEntity(cp_0_0.getGuid());
        col_0 = getEntity(col_0.getGuid());
        col_1 = getEntity(col_1.getGuid());
        col_2 = getEntity(col_2.getGuid());
        col_3 = getEntity(col_3.getGuid());
        col_4 = getEntity(col_4.getGuid());

        cp_0 = getEntity(cp_0.getGuid());
        cp_1 = getEntity(cp_1.getGuid());
        cp_2 = getEntity(cp_2.getGuid());
        cp_3 = getEntity(cp_3.getGuid());


        assertNull(cp_0_0.getClassifications());
        assertNotNull(col_0.getClassifications());
        assertNull(col_1.getClassifications());
        assertNull(col_2.getClassifications());
        assertNull(col_3.getClassifications());
        assertNull(col_4.getClassifications());

        assertNull(cp_0.getClassifications());
        assertNull(cp_1.getClassifications());
        assertNull(cp_2.getClassifications());
        assertNull(cp_3.getClassifications());


        LOG.info(">> deleteNewProcess");
    }

    private static void createNewProcessAgain() throws Exception {
        LOG.info("<< createNewProcessAgain");
        /*  6
         * cp_0, cp_0_0 are deleted in previous requests
         *
         * Tag is on col_0 which is not propagated at all
         *                                     -> cp_1 -> col_2
         * col_0  ->  cp_0 (DELETED)  -> col_1 -> cp_2 -> col_3
         *                                     -> cp_3 -> col_4
         *
         *                                     -> cp_1 -> col_2
         * col_0  ->  cp_0 (DELETED)  -> col_1 -> cp_2 -> col_3
         *        ->  cp_0_0 (DELETED)         -> cp_3 -> col_4
         *        ->  cp_0_1
         *
         * create new process cp_0_1 between col_0 & col_1
         *          This will get dataset to original state again with 2 deleted & one Active lineage with  cp_0_1
         *          Verify tag_0 is propagated to all other entities than col_0 (except  cp_0, cp_0_0)
         *
         * */


        cp_0_1 = getAtlasEntityExt(TYPE_COLUMN_PROCESS, "column_process_0_1").getEntity();
        cp_0_1.setRelationshipAttribute(INPUTS,  getObjectIdsAsList(TYPE_COLUMN, col_0.getGuid()));
        cp_0_1.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_1.getGuid()));

        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(cp_0_1);

        EntityMutationResponse response = createEntitiesBulk(entitiesWithExtInfo);

        assertEquals(1, response.getCreatedEntities().size());

        sleep();
        cp_0_1 = getEntity(response.getGuidAssignments().get(cp_0_1.getGuid()));
        cp_0_0 = getEntity(cp_0_0.getGuid());
        col_0 = getEntity(col_0.getGuid());
        col_1 = getEntity(col_1.getGuid());
        col_2 = getEntity(col_2.getGuid());
        col_3 = getEntity(col_3.getGuid());
        col_4 = getEntity(col_4.getGuid());
        cp_0 =  getEntity(cp_0.getGuid());
        cp_1 =  getEntity(cp_1.getGuid());
        cp_2 =  getEntity(cp_2.getGuid());
        cp_3 =  getEntity(cp_3.getGuid());

        assertNotNull(cp_0_1.getClassifications());

        assertNull(cp_0_0.getClassifications());
        assertNotNull(col_0.getClassifications());
        assertNotNull(col_1.getClassifications());
        assertNotNull(col_2.getClassifications());
        assertNotNull(col_3.getClassifications());
        assertNotNull(col_4.getClassifications());

        assertNull(cp_0.getClassifications());
        assertNotNull(cp_1.getClassifications());
        assertNotNull(cp_2.getClassifications());
        assertNotNull(cp_3.getClassifications());

        LOG.info(">> createNewProcessAgain");
    }

    private static void updateProcessToRemoveOutput() throws Exception {
        LOG.info("<< updateProcessToRemoveOutput");
        /*  7
         *                                     -> cp_1 -> col_2
         * col_0  ->  cp_0 (DELETED)  -> col_1 -> cp_2 -> col_3
         *        ->  cp_0_0 (DELETED)         -> cp_3 -> col_4
         *        ->  cp_0_1
         *
         * Update cp_0_1 to remove relationship between cp_0 -> col_1 (update outputs of cp_0 )
         *          Verify tag_0 is attached to col_0,  propagated to col_0_1 but propagation are removed form all other entities
         * */

        cp_0_1.setAttribute(OUTPUTS, null);
        cp_0_1.setRelationshipAttribute(OUTPUTS, new ArrayList<AtlasObjectId>());
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo(cp_0_1);

        LOG.info(AtlasType.toJson(entitiesWithExtInfo));

        EntityMutationResponse response = createEntitiesBulk(entitiesWithExtInfo);

        assertEquals(1, response.getUpdatedEntities().size());

        sleep();
        refreshAllEntities();

        assertNotNull(cp_0_1.getClassifications());

        assertNull(cp_0_0.getClassifications());
        assertNotNull(col_0.getClassifications());
        assertNull(col_1.getClassifications());
        assertNull(col_2.getClassifications());
        assertNull(col_3.getClassifications());
        assertNull(col_4.getClassifications());

        assertNull(cp_0.getClassifications());
        assertNull(cp_1.getClassifications());
        assertNull(cp_2.getClassifications());
        assertNull(cp_3.getClassifications());

        LOG.info(">> updateProcessToRemoveOutput");
    }

    private static void updateProcessToAddOutputAgain() throws Exception {
        LOG.info("<< updateProcessToAddOutputAgain");
        /*  8
         *                                     -> cp_1 -> col_2
         * col_0  ->  cp_0 (DELETED)  -> col_1 -> cp_2 -> col_3
         *        ->  cp_0_0 (DELETED)         -> cp_3 -> col_4
         *        ->  cp_0_1
         *
         * Update cp_0_1 to to create relationship between cp_0 -> col_1
         *      Verify tag_0 is attached to col_0 &  propagated to all other entities (except  cp_0, cp_0_0)
         * */


        cp_0_1.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_1.getGuid()));
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo(cp_0_1);

        LOG.info(AtlasType.toJson(entitiesWithExtInfo));

        EntityMutationResponse response = createEntitiesBulk(entitiesWithExtInfo);

        sleep();
        refreshAllEntities();

        assertNotNull(col_0.getClassifications());
        assertNull(cp_0_0.getClassifications());
        assertNull(cp_0.getClassifications());

        assertNotNull(cp_0_1.getClassifications());
        assertNotNull(col_1.getClassifications());
        assertNotNull(col_2.getClassifications());
        assertNotNull(col_3.getClassifications());
        assertNotNull(col_4.getClassifications());
        assertNotNull(cp_1.getClassifications());
        assertNotNull(cp_2.getClassifications());
        assertNotNull(cp_3.getClassifications());

        List<AtlasRelatedObjectId> outputs = getRelationsAsList(cp_0_1.getRelationshipAttribute(OUTPUTS));
        assertNotNull(outputs);
        assertEquals(2, outputs.size());
        assertEquals(1, outputs.stream().filter(x -> x.getRelationshipStatus() == AtlasRelationship.Status.DELETED).count());
        assertEquals(1, outputs.stream().filter(x -> x.getRelationshipStatus() == AtlasRelationship.Status.ACTIVE).count());

        LOG.info(">> updateProcessToAddOutputAgain");
    }


    private static void deleteEdge() throws Exception {
        LOG.info("<< deleteEdge");
        /*  9
         *
         *                                     -> cp_1 -> col_2
         * col_0  ->  cp_0 (DELETED)  -> col_1 -> cp_2 -> col_3
         *        ->  cp_0_0 (DELETED)         -> cp_3 -> col_4
         *        ->  cp_0_1
         *
         * Delete ACTIVE edge between cp_0_1 & col_1 (relationship API)
         *      Verify tag_0 is attached to col_0, propagated to col_0_1 but propagation are removed from all other entities
         * */

        List<AtlasRelatedObjectId> outputs = getRelationsAsList(cp_0_1.getRelationshipAttribute(OUTPUTS));
        AtlasRelatedObjectId activeOutput = outputs.stream().filter(x -> x.getRelationshipStatus() == AtlasRelationship.Status.ACTIVE).findFirst().get();
        assertNotNull(activeOutput);

        deleteRelationshipByGuid(activeOutput.getRelationshipGuid());

        sleep();
        refreshAllEntities();

        assertNotNull(col_0.getClassifications());
        assertNotNull(cp_0_1.getClassifications());

        assertNull(cp_0_0.getClassifications());
        assertNull(cp_0.getClassifications());
        assertNull(col_1.getClassifications());
        assertNull(col_2.getClassifications());
        assertNull(col_3.getClassifications());
        assertNull(col_4.getClassifications());
        assertNull(cp_1.getClassifications());
        assertNull(cp_2.getClassifications());
        assertNull(cp_3.getClassifications());

        LOG.info(">> deleteEdge");
    }


    private static void createEdge() throws Exception {
        LOG.info("<< createEdge");
        /*  9
         *
         *                                     -> cp_1 -> col_2
         * col_0  ->  cp_0 (DELETED)  -> col_1 -> cp_2 -> col_3
         *        ->  cp_0_0 (DELETED)         -> cp_3 -> col_4
         *        ->  cp_0_1
         *
         * Create edge between cp_0_1 & col_1 (relationship API)
         *      Verify tag_0 is attached to col_0  propagated to all other entities  (except  cp_0, cp_0_0)
         * */


        AtlasRelationship relationship = new AtlasRelationship("process_catalog_outputs");
        relationship.setEnd1(getObjectId(cp_0_1.getGuid(), TYPE_CATEGORY));
        relationship.setEnd2(getObjectId(col_1.getGuid(), TYPE_CATEGORY));

        createRelationship(relationship);

        sleep();
        refreshAllEntities();

        assertNull(cp_0_0.getClassifications());
        assertNull(cp_0.getClassifications());

        assertNotNull(cp_0_1.getClassifications());
        assertNotNull(col_0.getClassifications());
        assertNotNull(col_1.getClassifications());
        assertNotNull(col_2.getClassifications());
        assertNotNull(col_3.getClassifications());
        assertNotNull(col_4.getClassifications());
        assertNotNull(cp_1.getClassifications());
        assertNotNull(cp_2.getClassifications());
        assertNotNull(cp_3.getClassifications());

        LOG.info(">> createEdge");
    }


    private static void sleep() {
        LOG.info("Sleeping for 30 seconds");
        sleep(30000);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void refreshAllEntities() throws Exception {
        cp_0_1 = getEntity(cp_0_1.getGuid());
        cp_0_0 = getEntity(cp_0_0.getGuid());
        col_0 = getEntity(col_0.getGuid());
        col_1 = getEntity(col_1.getGuid());
        col_2 = getEntity(col_2.getGuid());
        col_3 = getEntity(col_3.getGuid());
        col_4 = getEntity(col_4.getGuid());
        cp_0 =  getEntity(cp_0.getGuid());
        cp_1 =  getEntity(cp_1.getGuid());
        cp_2 =  getEntity(cp_2.getGuid());
        cp_3 =  getEntity(cp_3.getGuid());
    }

    private static List<AtlasRelatedObjectId> getRelationsAsList(Object relationsList) throws JsonProcessingException {
        return objectMapper.readValue(AtlasType.toJson(relationsList), objectMapper.getTypeFactory().constructCollectionType(List.class, AtlasRelatedObjectId.class));
    }
}
class AddTag extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(AddTag.class);

    String guid;
    List<AtlasClassification> classifications;

    AddTag(String guid, List<AtlasClassification> classifications) {
        this.guid = guid;
        this.classifications = classifications;
    }

    @Override
    public void run() {
        LOG.info("Adding classification {}", classifications.get(0));
        try {
            addClassifications(guid, classifications);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}