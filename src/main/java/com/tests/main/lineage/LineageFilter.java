package com.tests.main.lineage;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtils;
import com.tests.main.utils.Utils;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.tests.main.utils.TestUtils.*;
import static com.tests.main.utils.TestUtils.getEntity;
import static org.junit.Assert.*;


public class LineageFilter implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(LineageFilter.class);

    public static void main(String[] args) throws Exception {
        try {
            new LineageFilter().run();
        } finally {
            //cleanUpAll();
            ESUtils.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running LineageFilter tests");

        long start = System.currentTimeMillis();
        try {
            getLineage();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running LineageFilter tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void getLineage() throws Exception {
        LOG.info(">> getLineage");

        /*
        *                                                 -> column_0   -> process_n -> column_1
        * table_0 -> process_0 -> table_1 -> process_1    -> table_3    -> process_2 -> table_4
        *
        *                                                               -> process_3 -> column_2
        *                                                 -> table_2    -> process 4 -> table_5
        *                                                               -> process 5 -> table_6
        *                                                                            -> table_7
        *                                                                            -> table_8
        * */

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0").getEntity();
        AtlasEntity table_1 = getAtlasEntity(TYPE_TABLE, "table_1").getEntity();
        AtlasEntity table_2 = getAtlasEntity(TYPE_TABLE, "table_2").getEntity();
        AtlasEntity table_3 = getAtlasEntity(TYPE_TABLE, "table_3").getEntity();
        AtlasEntity table_4 = getAtlasEntity(TYPE_TABLE, "table_4").getEntity();
        AtlasEntity table_5 = getAtlasEntity(TYPE_TABLE, "table_5").getEntity();
        AtlasEntity table_6 = getAtlasEntity(TYPE_TABLE, "table_6").getEntity();
        AtlasEntity table_7 = getAtlasEntity(TYPE_TABLE, "table_7").getEntity();
        AtlasEntity table_8 = getAtlasEntity(TYPE_TABLE, "table_8").getEntity();
        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "column_0").getEntity();
        AtlasEntity column_1 = getAtlasEntity(TYPE_COLUMN, "column_1").getEntity();
        AtlasEntity column_2 = getAtlasEntity(TYPE_COLUMN, "column_2").getEntity();

        AtlasEntity process_0 = getAtlasEntity(TYPE_PROCESS, "process_0").getEntity();
        process_0.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid()));
        process_0.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));

        AtlasEntity process_1 = getAtlasEntity(TYPE_PROCESS, "process_1").getEntity();
        process_1.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        List<AtlasObjectId> relatedObjectIdList = getObjectIdsAsList(TYPE_TABLE, table_2.getGuid(), table_3.getGuid());
        relatedObjectIdList.add(getObjectId(column_0.getGuid(), TYPE_COLUMN));
        process_1.setRelationshipAttribute(OUTPUTS, relatedObjectIdList);

        AtlasEntity process_2 = getAtlasEntity(TYPE_PROCESS, "process_2").getEntity();
        process_2.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_3.getGuid()));
        process_2.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_4.getGuid()));

        AtlasEntity process_3 = getAtlasEntity(TYPE_PROCESS, "process_3").getEntity();
        process_3.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        process_3.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, column_2.getGuid()));


        AtlasEntity process_4 = getAtlasEntity(TYPE_PROCESS, "process_4").getEntity();
        process_4.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        process_4.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, table_5.getGuid()));

        AtlasEntity process_5 = getAtlasEntity(TYPE_PROCESS, "process_5").getEntity();
        process_5.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        process_5.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, table_6.getGuid(), table_7.getGuid(), table_8.getGuid()));

        AtlasEntity process_n = getAtlasEntity(TYPE_PROCESS, "process_n").getEntity();
        process_n.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_COLUMN, column_0.getGuid()));
        process_n.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, column_1.getGuid()));

        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(table_0);
        entitiesWithExtInfo.addEntity(table_1);
        entitiesWithExtInfo.addEntity(table_2);
        entitiesWithExtInfo.addEntity(table_3);
        entitiesWithExtInfo.addEntity(table_4);
        entitiesWithExtInfo.addEntity(table_5);
        entitiesWithExtInfo.addEntity(table_6);
        entitiesWithExtInfo.addEntity(table_7);
        entitiesWithExtInfo.addEntity(table_8);
        entitiesWithExtInfo.addEntity(column_0);
        entitiesWithExtInfo.addEntity(column_1);
        entitiesWithExtInfo.addEntity(column_2);
        entitiesWithExtInfo.addEntity(process_0);
        entitiesWithExtInfo.addEntity(process_1);
        entitiesWithExtInfo.addEntity(process_2);
        entitiesWithExtInfo.addEntity(process_3);
        entitiesWithExtInfo.addEntity(process_4);
        entitiesWithExtInfo.addEntity(process_5);
        entitiesWithExtInfo.addEntity(process_n);

        LOG.info(Utils.toJson(entitiesWithExtInfo));

        EntityMutationResponse response = createEntitiesBulk(entitiesWithExtInfo);

        //assertEquals(response.getCreatedEntities().size(), 13);

        table_0 = getEntity(response.getGuidAssignments().get(table_0.getGuid()));
        table_1 = getEntity(response.getGuidAssignments().get(table_1.getGuid()));
        table_2 = getEntity(response.getGuidAssignments().get(table_2.getGuid()));
        table_3 = getEntity(response.getGuidAssignments().get(table_3.getGuid()));
        table_4 = getEntity(response.getGuidAssignments().get(table_4.getGuid()));
        column_0 = getEntity(response.getGuidAssignments().get(column_0.getGuid()));
        column_1 = getEntity(response.getGuidAssignments().get(column_1.getGuid()));
        column_2 = getEntity(response.getGuidAssignments().get(column_2.getGuid()));

        process_0 = getEntity(response.getGuidAssignments().get(process_0.getGuid()));
        process_1 = getEntity(response.getGuidAssignments().get(process_1.getGuid()));
        process_2 = getEntity(response.getGuidAssignments().get(process_2.getGuid()));
        process_3 = getEntity(response.getGuidAssignments().get(process_3.getGuid()));
        process_n = getEntity(response.getGuidAssignments().get(process_n.getGuid()));
        LOG.info(table_1.getGuid());


        //TODO



        LOG.info(">> getLineage");
    }
}