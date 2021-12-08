package com.tests.main.lineage;

import com.tests.main.glossary.tests.TestsMain;
import com.tests.main.glossary.utils.ESUtils;
import com.tests.main.glossary.utils.TestUtils;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.tests.main.glossary.utils.TestUtils.*;
import static com.tests.main.glossary.utils.TestUtils.verifyESHasLineage;
import static org.junit.Assert.*;

public class Lineage implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(Lineage.class);

    public static void main(String[] args) throws Exception {
        try {
            new Lineage().run();
        } finally {
            cleanUpAll();
            ESUtils.close();
        }
    }

    @Override
    public void run() throws  Exception {
        LOG.info("Running Lineage tests");

        long start = System.currentTimeMillis();
        try {
            testHasLineageProperty();
            testHasLineagePropertyBulk();

            testHasLineageHideProcess();

        } catch (Exception e){
            throw e;
        } finally {
            Thread.sleep(2000);
            cleanUpAll();
            ESUtils.close();
            LOG.info("Completed running Lineage tests, took {} seconds", (System.currentTimeMillis() - start) / 1000 );
        }
    }

    private static void testHasLineageProperty() throws Exception {
        LOG.info(">> testHasLineageProperty");

        EntityMutationResponse response;
        AtlasEntity table_0, table_1, table_3, table_4, col_5, col_6, col_7, col_8, process_0, column_process_1;

        table_0 = createCustomEntity(TYPE_TABLE, "table_0");
        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        table_3 = createCustomEntity(TYPE_TABLE, "table_3");
        table_4 = createCustomEntity(TYPE_TABLE, "table_4");

        col_5 = createCustomEntity(TYPE_COLUMN, "col_5");
        col_6 = createCustomEntity(TYPE_COLUMN, "col_6");
        col_7 = createCustomEntity(TYPE_COLUMN, "col_7");
        col_8 = createCustomEntity(TYPE_COLUMN, "col_8");

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_0");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid(), table_1.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_0 = getEntity(response.getCreatedEntities().get(0).getGuid());

        extInfo = getAtlasEntity(TYPE_COLUMN_PROCESS, "column_process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, col_5.getGuid(), col_6.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        column_process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        verifyESHasLineage(process_0.getGuid());
        verifyESHasLineage(table_0.getGuid());
        verifyESHasLineage(table_1.getGuid());
        verifyESHasLineage(table_3.getGuid(), true);
        verifyESHasLineage(table_4.getGuid(), true);

        verifyESHasLineage(column_process_1.getGuid());
        verifyESHasLineage(col_5.getGuid());
        verifyESHasLineage(col_6.getGuid());
        verifyESHasLineage(col_7.getGuid(), true);
        verifyESHasLineage(col_8.getGuid(), true);

        //--------------------
        process_0 = getEntity(process_0.getGuid());
        process_0.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_3.getGuid(), table_4.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(process_0));

        column_process_1 = getEntity(column_process_1.getGuid());
        column_process_1.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_7.getGuid(), col_8.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(column_process_1));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid());
        verifyESHasLineage(column_process_1.getGuid());
        verifyESHasLineage(table_0.getGuid());
        verifyESHasLineage(table_1.getGuid());
        verifyESHasLineage(table_3.getGuid());
        verifyESHasLineage(table_4.getGuid());
        verifyESHasLineage(col_5.getGuid());
        verifyESHasLineage(col_6.getGuid());
        verifyESHasLineage(col_7.getGuid());
        verifyESHasLineage(col_8.getGuid());

        table_0 = getEntity(table_0.getGuid());


        //--------------------
        //remove inputs
        process_0 = getEntity(process_0.getGuid());
        process_0.setRelationshipAttribute(INPUTS, new ArrayList<>());
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(process_0));

        column_process_1 = getEntity(column_process_1.getGuid());
        column_process_1.setRelationshipAttribute(INPUTS, new ArrayList<>());
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(column_process_1));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid());
        verifyESHasLineage(column_process_1.getGuid());
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_3.getGuid());
        verifyESHasLineage(table_4.getGuid());
        verifyESHasLineage(col_5.getGuid(), true);
        verifyESHasLineage(col_6.getGuid(), true);
        verifyESHasLineage(col_7.getGuid());
        verifyESHasLineage(col_8.getGuid());


        //--------------------
        //remove outputs
        process_0 = getEntity(process_0.getGuid());
        process_0.setRelationshipAttribute(INPUTS, new ArrayList<>());
        process_0.setRelationshipAttribute(OUTPUTS, new ArrayList<>());
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(process_0));


        column_process_1 = getEntity(column_process_1.getGuid());
        column_process_1.setRelationshipAttribute(INPUTS, new ArrayList<>());
        column_process_1.setRelationshipAttribute(OUTPUTS, new ArrayList<>());
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(column_process_1));


        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid(), true);
        verifyESHasLineage(column_process_1.getGuid(), true);
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_3.getGuid(), true);
        verifyESHasLineage(table_4.getGuid(), true);
        verifyESHasLineage(col_5.getGuid(), true);
        verifyESHasLineage(col_6.getGuid(), true);
        verifyESHasLineage(col_7.getGuid(), true);
        verifyESHasLineage(col_8.getGuid(), true);


        //---------------------------
        //table_0 & table_1 add process_0 again
        //col_5 & col_6  add column_process_1 again
        table_0 = getEntity(table_0.getGuid());
        table_1 = getEntity(table_1.getGuid());
        table_0.setRelationshipAttribute(INPUTS_TO_P, getObjectIdsAsList(TYPE_PROCESS, process_0.getGuid()));
        table_1.setRelationshipAttribute(INPUTS_TO_P, getObjectIdsAsList(TYPE_PROCESS, process_0.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(table_0));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(table_1));

        col_5 = getEntity(col_5.getGuid());
        col_6 = getEntity(col_6.getGuid());
        col_5.setRelationshipAttribute(INPUTS_TO_P, getObjectIdsAsList(TYPE_PROCESS, column_process_1.getGuid()));
        col_6.setRelationshipAttribute(INPUTS_TO_P, getObjectIdsAsList(TYPE_PROCESS, column_process_1.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(col_5));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(col_6));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid());
        verifyESHasLineage(column_process_1.getGuid());
        verifyESHasLineage(table_0.getGuid());
        verifyESHasLineage(table_1.getGuid());
        verifyESHasLineage(table_3.getGuid(), true);
        verifyESHasLineage(table_4.getGuid(), true);
        verifyESHasLineage(col_5.getGuid());
        verifyESHasLineage(col_6.getGuid());
        verifyESHasLineage(col_7.getGuid(), true);
        verifyESHasLineage(col_8.getGuid(), true);

        //---------------------------
        //table_0 & table_1 remove from process_0 again
        //col_5 & col_6 remove from column_process_1 again
        table_0 = getEntity(table_0.getGuid());
        table_1 = getEntity(table_1.getGuid());
        table_0.setRelationshipAttribute(INPUTS_TO_P, new ArrayList<>());
        table_1.setRelationshipAttribute(INPUTS_TO_P, new ArrayList<>());
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(table_0));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(table_1));

        col_5 = getEntity(col_5.getGuid());
        col_6 = getEntity(col_6.getGuid());
        col_5.setRelationshipAttribute(INPUTS_TO_P, new ArrayList<>());
        col_6.setRelationshipAttribute(INPUTS_TO_P, new ArrayList<>());
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(col_5));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(col_6));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid(), true);
        verifyESHasLineage(column_process_1.getGuid(), true);
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_3.getGuid(), true);
        verifyESHasLineage(table_4.getGuid(), true);
        verifyESHasLineage(col_5.getGuid(), true);
        verifyESHasLineage(col_6.getGuid(), true);
        verifyESHasLineage(col_7.getGuid(), true);
        verifyESHasLineage(col_8.getGuid(), true);

        //newwwwwwwwwwwwwww
        //---------------------------
        //table_3 &  add process_0 again as output this time
        //col_7 & col_8 add column_process_1 again as input this time
        table_3 = getEntity(table_3.getGuid());
        table_4 = getEntity(table_4.getGuid());
        table_3.setRelationshipAttribute(OUTPUTS_FROM_P, getObjectIdsAsList(TYPE_PROCESS, process_0.getGuid()));
        table_4.setRelationshipAttribute(OUTPUTS_FROM_P, getObjectIdsAsList(TYPE_PROCESS, process_0.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(table_3));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(table_4));

        col_7 = getEntity(col_7.getGuid());
        col_8 = getEntity(col_8.getGuid());
        col_7.setRelationshipAttribute(OUTPUTS_FROM_P, getObjectIdsAsList(TYPE_PROCESS, column_process_1.getGuid()));
        col_8.setRelationshipAttribute(OUTPUTS_FROM_P, getObjectIdsAsList(TYPE_PROCESS, column_process_1.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(col_7));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(col_8));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid());
        verifyESHasLineage(column_process_1.getGuid());
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_3.getGuid());
        verifyESHasLineage(table_4.getGuid());
        verifyESHasLineage(col_5.getGuid(), true);
        verifyESHasLineage(col_6.getGuid(), true);
        verifyESHasLineage(col_7.getGuid());
        verifyESHasLineage(col_8.getGuid());

        //---------------------------
        //table_3 & table_4 remove from process_0 again
        //col_7 & col_8  remove from column_process_1 again
        table_3 = getEntity(table_3.getGuid());
        table_4 = getEntity(table_4.getGuid());
        table_3.setRelationshipAttribute(OUTPUTS_FROM_P, new ArrayList<>());
        table_4.setRelationshipAttribute(OUTPUTS_FROM_P, new ArrayList<>());
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(table_3));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(table_4));

        col_7 = getEntity(col_7.getGuid());
        col_8 = getEntity(col_8.getGuid());
        col_7.setRelationshipAttribute(OUTPUTS_FROM_P, new ArrayList<>());
        col_8.setRelationshipAttribute(OUTPUTS_FROM_P, new ArrayList<>());
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(col_7));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(col_8));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid(), true);
        verifyESHasLineage(column_process_1.getGuid(), true);
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_3.getGuid(), true);
        verifyESHasLineage(table_4.getGuid(), true);
        verifyESHasLineage(col_5.getGuid(), true);
        verifyESHasLineage(col_6.getGuid(), true);
        verifyESHasLineage(col_7.getGuid(), true);
        verifyESHasLineage(col_8.getGuid(), true);

        LOG.info("<< testHasLineageProperty");
    }

    private static void testHasLineagePropertyBulk() throws Exception {
        LOG.info(">> testHasLineagePropertyBulk");

        EntityMutationResponse response;
        AtlasEntity.AtlasEntitiesWithExtInfo extInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        AtlasEntity table_0, table_1, table_2, col_0, col_1, process_0, process_1, column_process_3;

        table_0 = getAtlasEntity(TYPE_TABLE, "table_0").getEntity();
        table_0.setGuid("-1");
        table_1 = getAtlasEntity(TYPE_TABLE, "table_1").getEntity();
        table_1.setGuid("-2");
        table_2 = getAtlasEntity(TYPE_TABLE, "table_2").getEntity();
        table_2.setGuid("-3");
        col_0 = getAtlasEntity(TYPE_COLUMN, "col_0").getEntity();
        col_0.setGuid("-51");
        col_1 = getAtlasEntity(TYPE_COLUMN, "col_1").getEntity();
        col_1.setGuid("-52");
        extInfo.addEntity(table_0);
        extInfo.addEntity(table_1);
        extInfo.addEntity(table_2);
        extInfo.addEntity(col_0);
        extInfo.addEntity(col_1);

        process_0 = getAtlasEntity(TYPE_PROCESS, "process_0").getEntity();
        process_0.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid()));
        process_0.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        process_0.setGuid("-4");

        process_1 = getAtlasEntity(TYPE_PROCESS, "process_1").getEntity();
        process_1.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        process_1.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        process_1.setGuid("-5");

        column_process_3 = getAtlasEntity(TYPE_COLUMN_PROCESS, "column_process_3").getEntity();
        column_process_3.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_COLUMN, col_0.getGuid()));
        column_process_3.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_1.getGuid()));
        column_process_3.setGuid("-53");

        extInfo.addEntity(process_0);
        extInfo.addEntity(process_1);
        extInfo.addEntity(column_process_3);

        response = createEntitiesBulk(extInfo);

        table_0 = getEntity(response.getGuidAssignments().get("-1"));
        table_1 = getEntity(response.getGuidAssignments().get("-2"));
        table_2 = getEntity(response.getGuidAssignments().get("-3"));
        process_0 = getEntity(response.getGuidAssignments().get("-4"));
        process_1 = getEntity(response.getGuidAssignments().get("-5"));
        col_0 = getEntity(response.getGuidAssignments().get("-51"));
        col_1 = getEntity(response.getGuidAssignments().get("-52"));
        column_process_3 = getEntity(response.getGuidAssignments().get("-53"));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid());
        verifyESHasLineage(process_1.getGuid());
        verifyESHasLineage(column_process_3.getGuid());
        verifyESHasLineage(table_0.getGuid());
        verifyESHasLineage(table_1.getGuid());
        verifyESHasLineage(table_2.getGuid());
        verifyESHasLineage(col_0.getGuid());
        verifyESHasLineage(col_1.getGuid());

        //remove inputs & outputs from process_0, process_1 & column_process_3
        extInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();

        process_0.setRelationshipAttribute(INPUTS, new ArrayList<>());
        process_0.setRelationshipAttribute(OUTPUTS, new ArrayList<>());
        process_1.setRelationshipAttribute(INPUTS, new ArrayList<>());
        process_1.setRelationshipAttribute(OUTPUTS, new ArrayList<>());
        column_process_3.setRelationshipAttribute(INPUTS, new ArrayList<>());
        column_process_3.setRelationshipAttribute(OUTPUTS, new ArrayList<>());

        extInfo.addEntity(process_0);
        extInfo.addEntity(process_1);
        extInfo.addEntity(column_process_3);

        response = createEntitiesBulk(extInfo);

        Thread.sleep(2000);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(col_0.getGuid(), true);
        verifyESHasLineage(col_1.getGuid(), true);
        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(process_0.getGuid(), true);
        verifyESHasLineage(column_process_3.getGuid(), true);


        //add table_0 as input to process_1
        //add col_0 as input to column_process_3
        extInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();

        process_1 = getEntity(process_1.getGuid());
        process_1.getAttributes().remove(OUTPUTS);
        process_1.getRelationshipAttributes().remove(OUTPUTS);
        process_1.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid()));
        extInfo.addEntity(process_1);

        column_process_3 = getEntity(column_process_3.getGuid());
        column_process_3.getAttributes().remove(OUTPUTS);
        column_process_3.getRelationshipAttributes().remove(OUTPUTS);
        column_process_3.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_COLUMN, col_0.getGuid()));
        extInfo.addEntity(column_process_3);

        response = createEntitiesBulk(extInfo);

        Thread.sleep(2000);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);
        verifyESHasLineage(table_0.getGuid());
        verifyESHasLineage(col_0.getGuid());
        verifyESHasLineage(col_1.getGuid(), true);
        verifyESHasLineage(process_1.getGuid());
        verifyESHasLineage(process_0.getGuid(), true);
        verifyESHasLineage(column_process_3.getGuid());


        LOG.info("<< testHasLineagePropertyBulk");
    }

    private static void testHasLineageHideProcess() throws Exception {
        LOG.info(">> testHasLineageHideProcess");

        EntityMutationResponse response;
        AtlasEntity.AtlasEntitiesWithExtInfo extInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        AtlasEntity table_0, table_1, table_2, table_3, col_0, col_1, process_0, process_1, column_process_3;

        AtlasLineageInfo lineageInfo;
        Set<AtlasLineageInfo.LineageRelation> relations;
        Map<String, AtlasEntityHeader> guidEntityMap;

        table_0 = getAtlasEntity(TYPE_TABLE, "table_0").getEntity();
        table_0.setGuid("-1");
        table_1 = getAtlasEntity(TYPE_TABLE, "table_1").getEntity();
        table_1.setGuid("-2");
        table_2 = getAtlasEntity(TYPE_TABLE, "table_2").getEntity();
        table_2.setGuid("-3");
        table_3 = getAtlasEntity(TYPE_TABLE, "table_3").getEntity();
        table_3.setGuid("-4");
        col_0 = getAtlasEntity(TYPE_TABLE, "col_0").getEntity();
        col_0.setGuid("-51");
        col_1 = getAtlasEntity(TYPE_TABLE, "col_1").getEntity();
        col_1.setGuid("-52");

        extInfo.addEntity(table_0);
        extInfo.addEntity(table_1);
        extInfo.addEntity(table_2);
        extInfo.addEntity(table_3);
        extInfo.addEntity(col_0);
        extInfo.addEntity(col_1);

        process_0 = getAtlasEntity(TYPE_PROCESS, "process_0").getEntity();
        process_0.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid(), table_1.getGuid()));
        process_0.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid(), table_3.getGuid()));
        process_0.setGuid("-21");

        column_process_3 = getAtlasEntity(TYPE_COLUMN_PROCESS, "column_process_3").getEntity();
        column_process_3.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_COLUMN, col_0.getGuid()));
        column_process_3.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_1.getGuid()));
        column_process_3.setGuid("-53");

        process_1 = getAtlasEntity(TYPE_PROCESS, "process_1").getEntity();
        process_1.setGuid("-22");

        extInfo.addEntity(process_0);
        extInfo.addEntity(process_1);
        extInfo.addEntity(column_process_3);

        response = createEntitiesBulk(extInfo);

        table_0 = getEntity(response.getGuidAssignments().get("-1"));
        table_1 = getEntity(response.getGuidAssignments().get("-2"));
        table_2 = getEntity(response.getGuidAssignments().get("-3"));
        table_3 = getEntity(response.getGuidAssignments().get("-4"));
        col_0 = getEntity(response.getGuidAssignments().get("-51"));
        col_1 = getEntity(response.getGuidAssignments().get("-52"));
        process_0 = getEntity(response.getGuidAssignments().get("-21"));
        process_1 = getEntity(response.getGuidAssignments().get("-22"));
        column_process_3 = getEntity(response.getGuidAssignments().get("-53"));

        Thread.sleep(2000);
        //TODO: enable after merging with __hasLineage patch
        verifyESHasLineage(process_0.getGuid());
        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(column_process_3.getGuid());
        verifyESHasLineage(table_0.getGuid());
        verifyESHasLineage(table_1.getGuid());
        verifyESHasLineage(table_2.getGuid());
        verifyESHasLineage(table_3.getGuid());
        verifyESHasLineage(col_0.getGuid());
        verifyESHasLineage(col_1.getGuid());

        lineageInfo = getLineageInfo(table_0.getGuid(), AtlasLineageInfo.LineageDirection.BOTH, 3, false);
        relations = lineageInfo.getRelations();
        guidEntityMap = lineageInfo.getGuidEntityMap();
        assertNotNull(relations);
        assertNotNull(guidEntityMap);

        assertEquals(3, relations.size());
        assertEquals(4, guidEntityMap.size());

        Set<String> guids = relations.stream().map(x-> x.getFromEntityId()).collect(Collectors.toSet());
        guids.addAll(relations.stream().map(x-> x.getToEntityId()).collect(Collectors.toSet()));

        assertTrue(guids.contains(process_0.getGuid()));
        assertTrue(guidEntityMap.keySet().contains(process_0.getGuid()));

        lineageInfo = getLineageInfo(col_0.getGuid(), AtlasLineageInfo.LineageDirection.BOTH, 3, false);
        relations = lineageInfo.getRelations();
        guidEntityMap = lineageInfo.getGuidEntityMap();
        assertNotNull(relations);
        assertNotNull(guidEntityMap);

        assertEquals(2, relations.size());
        assertEquals(3, guidEntityMap.size());

        guids = relations.stream().map(x-> x.getFromEntityId()).collect(Collectors.toSet());
        guids.addAll(relations.stream().map(x-> x.getToEntityId()).collect(Collectors.toSet()));

        assertTrue(guids.contains(column_process_3.getGuid()));
        assertTrue(guidEntityMap.keySet().contains(column_process_3.getGuid()));


        lineageInfo = getLineageInfo(table_0.getGuid(), AtlasLineageInfo.LineageDirection.BOTH, 3, true);
        relations = lineageInfo.getRelations();
        guidEntityMap = lineageInfo.getGuidEntityMap();
        assertNotNull(relations);
        assertEquals(2, relations.size());
        guids = relations.stream().map(x-> x.getFromEntityId()).collect(Collectors.toSet());
        guids.addAll(relations.stream().map(x-> x.getToEntityId()).collect(Collectors.toSet()));

        assertFalse(guids.contains(process_0.getGuid()));

        assertNotNull(guidEntityMap);
        assertEquals(3, guidEntityMap.size());
        assertFalse(guidEntityMap.keySet().contains(process_0.getGuid()));


        lineageInfo = getLineageInfo(col_0.getGuid(), AtlasLineageInfo.LineageDirection.BOTH, 3, true);
        relations = lineageInfo.getRelations();
        guidEntityMap = lineageInfo.getGuidEntityMap();
        assertNotNull(relations);
        assertEquals(1, relations.size());
        guids = relations.stream().map(x-> x.getFromEntityId()).collect(Collectors.toSet());
        guids.addAll(relations.stream().map(x-> x.getToEntityId()).collect(Collectors.toSet()));

        assertFalse(guids.contains(column_process_3.getGuid()));

        assertNotNull(guidEntityMap);
        assertEquals(2, guidEntityMap.size());
        assertFalse(guidEntityMap.keySet().contains(process_0.getGuid()));


        //fetch with process guid -> should fail
        boolean failed = false;
        try {
            lineageInfo = getLineageInfo(process_0.getGuid(), AtlasLineageInfo.LineageDirection.BOTH, 3, true);
        } catch (AtlasServiceException exception) {
            assertEquals(exception.getStatus().getStatusCode(),404);
            assertTrue(exception.getMessage().contains("ATLAS-404-00-017"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        failed = false;
        try {
            lineageInfo = getLineageInfo(column_process_3.getGuid(), AtlasLineageInfo.LineageDirection.BOTH, 3, true);
        } catch (AtlasServiceException exception) {
            assertEquals(exception.getStatus().getStatusCode(),404);
            assertTrue(exception.getMessage().contains("ATLAS-404-00-017"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }
        LOG.info("<< testHasLineageHideProcess");
    }

    private static AtlasEntity createCustomEntity(String typeName, String entityName) throws Exception {

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = getAtlasEntity(typeName, entityName);

        return getEntity(TestUtils.createEntity(entityWithExtInfo).getCreatedEntities().get(0).getGuid());

    }

    private static AtlasEntity.AtlasEntityWithExtInfo getAtlasEntity(String typeName, String entityName) {
        AtlasEntity entity = new AtlasEntity(typeName);
        entityName = StringUtils.isNotEmpty(entityName) ? entityName : getRandomName();
        entity.setAttribute(NAME, entityName);
        entity.setAttribute(QUALIFIED_NAME, entityName + "_" + getRandomName());

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        entityWithExtInfo.setEntity(entity);

        return entityWithExtInfo;
    }

    private static void verifyESHasLineage(String entityGuid) {
        verifyESHasLineage(entityGuid, false);
    }

    private static void verifyESHasLineage(String entityGuid, boolean expectedNull) {

        SearchHit[] searchHit = ESUtils.searchWithGuid(entityGuid).getHits().getHits();
        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            if (expectedNull) {
                assertNull(sourceAsMap.get("__hasLineage"));
            } else {
                assertNotNull(sourceAsMap.get("__hasLineage"));
                boolean value = (boolean) sourceAsMap.get("__hasLineage");
                assertTrue(value);
            }
        }
    }
}