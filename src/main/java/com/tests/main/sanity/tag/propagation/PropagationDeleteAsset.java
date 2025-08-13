package com.tests.main.sanity.tag.propagation;

import com.tests.main.Test;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.AtlasException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.tests.main.sanity.tag.propagation.PropagationUtils.TASK_TYPE_ADD_PROP;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.TASK_TYPE_DELETE_PROP;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.TASK_TYPE_REFRESH_PROP;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.getTagTypeDefs;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.verifyEntityHasTags;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.verifyEntityNotHaveTags;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.waitForPropagationTasksToComplete;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.waitForPropagationTasksToCompleteDelayed;
import static com.tests.main.utils.TestUtil.TYPE_COLUMN;
import static com.tests.main.utils.TestUtil.TYPE_DATABASE;
import static com.tests.main.utils.TestUtil.TYPE_PROCESS;
import static com.tests.main.utils.TestUtil.TYPE_SCHEMA;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.deleteEntityHard;
import static com.tests.main.utils.TestUtil.deleteEntitySoft;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getObjectId;
import static com.tests.main.utils.TestUtil.getObjectIdsAsList;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class PropagationDeleteAsset implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(PropagationDeleteAsset.class);

    private static long SLEEP = 2000;

    private static List<String> tagTypeNames;

    static {
        tagTypeNames = getTagTypeDefs(2);
    }

    public static void main(String[] args) throws Exception {
        try {
            new PropagationDeleteAsset().run();
            //TestRunner.runTests(PropagationDeleteAsset.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running PropagationDeleteAsset tests");

        long start = System.currentTimeMillis();
        try {

            deletePropagatedProcess();

            deleteDirectTaggedAsset();

            deletePropagatedAsset();

            deleteNestedPropagatedAsset();

            deleteNestedPropagatedAssetMultipleSources();


        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running PropagationDeleteAsset tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }


    @Test
    public void deletePropagatedProcess() throws Exception {
        LOG.info(">> deletePropagatedProcess");

        AtlasEntity table0 = getAtlasEntity(TYPE_TABLE, "test_table0" + getRandomName());
        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "test_table1" + getRandomName());
        AtlasEntity process = getAtlasEntity(TYPE_PROCESS, "test_process" + getRandomName());

        process.setRelationshipAttribute("inputs", getObjectIdsAsList(TYPE_TABLE, table0.getGuid()));
        process.setRelationshipAttribute("outputs", getObjectIdsAsList(TYPE_TABLE, table1.getGuid()));

        Map<String, String> guidAssignments = createEntitiesBulk(process, table1, table0).getGuidAssignments();

        String table0Guid = guidAssignments.get(table0.getGuid());
        String table1Guid = guidAssignments.get(table1.getGuid());
        String processGuid = guidAssignments.get(process.getGuid());

        sleep(SLEEP);

        table0 = getEntity(table0Guid);
        table0.setClassifications(Arrays.asList(
                new AtlasClassification(tagTypeNames.get(0)),
                new AtlasClassification(tagTypeNames.get(1))
        ));
        updateEntity(table0);

        waitForPropagationTasksToCompleteDelayed(table0.getGuid(), TASK_TYPE_ADD_PROP);
        verifyEntityHasTags(processGuid, tagTypeNames);
        verifyEntityHasTags(table1Guid, tagTypeNames);

        deleteEntityHard(processGuid);
        //deleteEntitySoft(processGuid);

        //waitForPropagationTasksToCompleteDelayed(schemaGuid, TASK_TYPE_REFRESH_PROP);
        waitForPropagationTasksToCompleteDelayed(table0Guid, TASK_TYPE_REFRESH_PROP);

        verifyEntityHasTags(table0Guid, tagTypeNames);
        verifyEntityNotHaveTags(table1Guid, tagTypeNames);

        boolean failed = false;
        try {
            verifyEntityNotHaveTags(processGuid, tagTypeNames);
        } catch (Exception exception) {
            assertTrue(exception.getMessage().contains(String.format("Given instance guid %s is invalid/not found", processGuid)));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        LOG.info("<< deletePropagatedProcess");
    }


    @Test
    public void deleteDirectTaggedAsset() throws Exception {
        LOG.info(">> deleteDirectTaggedAsset");

        // Create a test table with tag
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        table.setClassifications(Arrays.asList(
                new AtlasClassification(tagTypeNames.get(0)),
                new AtlasClassification(tagTypeNames.get(1))
        ));
        String tableGuid = createEntitiesBulk(table).getCreatedEntities().get(0).getGuid();

        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        AtlasEntity column1 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        column0.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        column1.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        String column0_Guid = createEntity(column0).getCreatedEntities().get(0).getGuid();
        String column1_Guid = createEntity(column1).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        waitForPropagationTasksToCompleteDelayed(tableGuid, TASK_TYPE_ADD_PROP);
        waitForPropagationTasksToComplete(column0_Guid, TASK_TYPE_ADD_PROP);
        waitForPropagationTasksToComplete(column1_Guid, TASK_TYPE_ADD_PROP);

        deleteEntitySoft(tableGuid);

        waitForPropagationTasksToCompleteDelayed(tableGuid, TASK_TYPE_DELETE_PROP);

        verifyEntityHasTags(tableGuid, tagTypeNames);
        verifyEntityNotHaveTags(column0_Guid, tagTypeNames);
        verifyEntityNotHaveTags(column1_Guid, tagTypeNames);

        LOG.info("<< deleteDirectTaggedAsset");
    }

    @Test
    public void deletePropagatedAsset() throws Exception {
        LOG.info(">> deletePropagatedAsset");

        AtlasEntity database = getAtlasEntity(TYPE_DATABASE, "test_database" + getRandomName());
        AtlasEntity schema = getAtlasEntity(TYPE_SCHEMA, "test_schema" + getRandomName());
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        AtlasEntity column1 = getAtlasEntity(TYPE_COLUMN, "test_column1" + getRandomName());

        schema.setRelationshipAttribute("database", getObjectId(database.getGuid(), TYPE_DATABASE));
        table.setRelationshipAttribute("atlanSchema", getObjectId(schema.getGuid(), TYPE_SCHEMA));
        column0.setRelationshipAttribute("table", getObjectId(table.getGuid(), TYPE_TABLE));
        column1.setRelationshipAttribute("table", getObjectId(table.getGuid(), TYPE_TABLE));

        Map<String, String> guidAssignments = createEntitiesBulk(database, schema, table, column0, column1).getGuidAssignments();

        String databaseGuid = guidAssignments.get(database.getGuid());
        String schemaGuid = guidAssignments.get(schema.getGuid());
        String tableGuid = guidAssignments.get(table.getGuid());
        String column0_Guid = guidAssignments.get(column0.getGuid());
        String column1_Guid = guidAssignments.get(column1.getGuid());

        sleep(SLEEP);

        database = getEntity(databaseGuid);
        database.setClassifications(Arrays.asList(
                new AtlasClassification(tagTypeNames.get(0)),
                new AtlasClassification(tagTypeNames.get(1))
        ));
        updateEntity(database);

        waitForPropagationTasksToCompleteDelayed(databaseGuid, TASK_TYPE_ADD_PROP);
        verifyEntityHasTags(column0_Guid, tagTypeNames);

        deleteEntitySoft(schemaGuid);

        //waitForPropagationTasksToCompleteDelayed(schemaGuid, TASK_TYPE_REFRESH_PROP);
        waitForPropagationTasksToCompleteDelayed(databaseGuid, TASK_TYPE_REFRESH_PROP);

        verifyEntityHasTags(databaseGuid, tagTypeNames);
        verifyEntityNotHaveTags(schemaGuid, tagTypeNames);
        verifyEntityNotHaveTags(tableGuid, tagTypeNames);
        verifyEntityNotHaveTags(column0_Guid, tagTypeNames);
        verifyEntityNotHaveTags(column1_Guid, tagTypeNames);

        LOG.info("<< deletePropagatedAsset");
    }

    @Test
    public void deleteNestedPropagatedAsset() throws Exception {
        LOG.info(">> deleteNestedPropagatedAsset");

        AtlasEntity database = getAtlasEntity(TYPE_DATABASE, "test_database" + getRandomName());
        AtlasEntity schema = getAtlasEntity(TYPE_SCHEMA, "test_schema" + getRandomName());
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        AtlasEntity column1 = getAtlasEntity(TYPE_COLUMN, "test_column1" + getRandomName());

        schema.setRelationshipAttribute("database", getObjectId(database.getGuid(), TYPE_DATABASE));
        table.setRelationshipAttribute("atlanSchema", getObjectId(schema.getGuid(), TYPE_SCHEMA));
        column0.setRelationshipAttribute("table", getObjectId(table.getGuid(), TYPE_TABLE));
        column1.setRelationshipAttribute("table", getObjectId(table.getGuid(), TYPE_TABLE));

        Map<String, String> guidAssignments = createEntitiesBulk(database, schema, table, column0, column1).getGuidAssignments();

        String databaseGuid = guidAssignments.get(database.getGuid());
        String schemaGuid = guidAssignments.get(schema.getGuid());
        String tableGuid = guidAssignments.get(table.getGuid());
        String column0_Guid = guidAssignments.get(column0.getGuid());
        String column1_Guid = guidAssignments.get(column1.getGuid());

        sleep(SLEEP);

        database = getEntity(databaseGuid);
        database.setClassifications(Arrays.asList(
                new AtlasClassification(tagTypeNames.get(0)),
                new AtlasClassification(tagTypeNames.get(1))
        ));
        updateEntity(database);

        waitForPropagationTasksToCompleteDelayed(databaseGuid, TASK_TYPE_ADD_PROP);
        verifyEntityHasTags(column0_Guid, tagTypeNames);

        deleteEntitySoft(tableGuid);

        waitForPropagationTasksToCompleteDelayed(schemaGuid, TASK_TYPE_REFRESH_PROP);
        waitForPropagationTasksToCompleteDelayed(databaseGuid, TASK_TYPE_REFRESH_PROP);

        verifyEntityHasTags(databaseGuid, tagTypeNames);
        verifyEntityHasTags(schemaGuid, tagTypeNames);
        verifyEntityNotHaveTags(tableGuid, tagTypeNames);
        verifyEntityNotHaveTags(column0_Guid, tagTypeNames);
        verifyEntityNotHaveTags(column1_Guid, tagTypeNames);

        LOG.info("<< deleteNestedPropagatedAsset");
    }


    @Test
    public void deleteNestedPropagatedAssetMultipleSources() throws Exception {
        LOG.info(">> deleteNestedPropagatedAssetMultipleSources");

        AtlasEntity database = getAtlasEntity(TYPE_DATABASE, "test_database" + getRandomName());
        AtlasEntity schema = getAtlasEntity(TYPE_SCHEMA, "test_schema" + getRandomName());
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        AtlasEntity column1 = getAtlasEntity(TYPE_COLUMN, "test_column1" + getRandomName());

        schema.setRelationshipAttribute("database", getObjectId(database.getGuid(), TYPE_DATABASE));
        table.setRelationshipAttribute("atlanSchema", getObjectId(schema.getGuid(), TYPE_SCHEMA));
        column0.setRelationshipAttribute("table", getObjectId(table.getGuid(), TYPE_TABLE));
        column1.setRelationshipAttribute("table", getObjectId(table.getGuid(), TYPE_TABLE));

        Map<String, String> guidAssignments = createEntitiesBulk(database, schema, table, column0, column1).getGuidAssignments();

        String databaseGuid = guidAssignments.get(database.getGuid());
        String schemaGuid = guidAssignments.get(schema.getGuid());
        String tableGuid = guidAssignments.get(table.getGuid());
        String column0_Guid = guidAssignments.get(column0.getGuid());
        String column1_Guid = guidAssignments.get(column1.getGuid());

        sleep(SLEEP);

        database = getEntity(databaseGuid);
        database.setClassifications(Arrays.asList(
                new AtlasClassification(tagTypeNames.get(0))
        ));
        updateEntity(database);


        schema = getEntity(schemaGuid);
        schema.setClassifications(Arrays.asList(
                new AtlasClassification(tagTypeNames.get(1))
        ));
        updateEntity(schema);

        waitForPropagationTasksToCompleteDelayed(databaseGuid, TASK_TYPE_ADD_PROP);
        waitForPropagationTasksToComplete(schemaGuid, TASK_TYPE_ADD_PROP);

        verifyEntityHasTags(column0_Guid, tagTypeNames);


        deleteEntitySoft(tableGuid);

        waitForPropagationTasksToCompleteDelayed(schemaGuid, TASK_TYPE_REFRESH_PROP);
        //waitForPropagationTasksToCompleteDelayed(databaseGuid, TASK_TYPE_REFRESH_PROP);

        verifyEntityHasTags(databaseGuid, tagTypeNames.subList(0, 1));
        verifyEntityHasTags(schemaGuid, tagTypeNames);
        verifyEntityNotHaveTags(tableGuid, tagTypeNames);
        verifyEntityNotHaveTags(column0_Guid, tagTypeNames);
        verifyEntityNotHaveTags(column1_Guid, tagTypeNames);

        LOG.info("<< deleteNestedPropagatedAssetMultipleSources");
    }
}
