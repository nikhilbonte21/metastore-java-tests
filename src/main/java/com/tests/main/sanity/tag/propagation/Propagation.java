package com.tests.main.sanity.tag.propagation;

import com.tests.main.Test;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.tests.main.sanity.tag.propagation.PropagationUtils.*;
import static com.tests.main.utils.TestUtil.TYPE_COLUMN;
import static com.tests.main.utils.TestUtil.TYPE_DATABASE;
import static com.tests.main.utils.TestUtil.TYPE_SCHEMA;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getObjectId;
import static com.tests.main.utils.TestUtil.getObjectIdsAsList;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;


public class Propagation implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(Propagation.class);

    private static long SLEEP = 2000;

    private static List<String> tagTypeNames;

    static {
        tagTypeNames = getTagTypeDefs(1);
    }

    public static void main(String[] args) throws Exception {
        try {
            new Propagation().run();
            //TestRunner.runTests(Propagation.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running Propagation tests");

        long start = System.currentTimeMillis();
        try {
            tableColumnAddRemove();

            columnTableAddRemoveInverse();

            schemaAddRemove();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running Propagation tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    @Test
    public void tableColumnAddRemove() throws Exception {
        LOG.info(">> tableColumnAddRemove");

        // Create a test table with tags
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        table.setClassifications(Arrays.asList(new AtlasClassification(tagTypeNames.get(0))));
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        String columnGuid = updateTableToLinkColumn(tableGuid);

        updateTableToUnlinkColumn(tableGuid, columnGuid);

        LOG.info("<< tableColumnAddRemove");
    }

    @Test
    public void columnTableAddRemoveInverse() throws Exception {
        LOG.info(">> columnTableAddRemoveInverse");

        // Create a test table with tags
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        table.setClassifications(Arrays.asList(new AtlasClassification(tagTypeNames.get(0))));
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        String columnGuid = updateColumnToLinkTable(tableGuid);

        updateColumnToUnlinkTable(tableGuid, columnGuid);

        LOG.info("<< columnTableAddRemoveInverse");
    }

    @Test
    public void schemaAddRemove() throws Exception {
        LOG.info(">> schemaAddRemove");

        AtlasEntity database = getAtlasEntity(TYPE_DATABASE, "test_database" + getRandomName());
        database.setClassifications(Arrays.asList(new AtlasClassification(tagTypeNames.get(0))));

        AtlasEntity schema = getAtlasEntity(TYPE_SCHEMA, "test_schema" + getRandomName());
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        AtlasEntity column1 = getAtlasEntity(TYPE_COLUMN, "test_column1" + getRandomName());

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

        updateSchemaToLinkDatabase(databaseGuid, schemaGuid, tableGuid, column0_Guid, column1_Guid);

        updateSchemaToUnlinkDatabase(databaseGuid, schemaGuid, tableGuid, column0_Guid, column1_Guid);

        LOG.info("<< schemaAddRemove");
    }

    private String updateTableToLinkColumn(String tableGuid) throws Exception {
        // Create a test column & link to table
        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        String columnGuid = createEntitiesBulk(column0).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        AtlasEntity table = getEntity(tableGuid);
        table.setRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, columnGuid));
        updateEntity(table);
        sleep(SLEEP);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToCompleteDelayed(columnGuid, TASK_TYPE_ADD_PROP);
        waitForPropagationTasksToComplete(tableGuid, TASK_TYPE_ADD_PROP);

        /*
        {
            "type": "CLASSIFICATION_PROPAGATION_ADD",
            "guid": "666d3e57-b710-4e36-918c-21c69241dabf",
            "createdBy": "service-account-atlan-argo",
            "createdTime": 1751526134832,
            "updatedTime": 1751526134832,
            "parameters": {
                "relationshipGuid": "3e62c54c-bae7-4aa3-bcbc-c233532debdb",
                "entityGuid": "d7cdd181-9fc1-4ca0-ad70-bd854305817d",
                "classificationVertexId": "14295076912"
            },
            "attemptCount": 0,
            "status": "PENDING",
            "classificationId": "14295076912",
            "entityGuid": "d7cdd181-9fc1-4ca0-ad70-bd854305817d",
            "classificationTypeName": "Qx79LklNtYoH5Sw1rUsnt3"
        }
        */

        sleep(SLEEP);
        // Verify the column has received the expected classification
        verifyEntityHasTags(columnGuid, tagTypeNames);

        return columnGuid;
    }

    private String updateTableToUnlinkColumn(String tableGuid, String columnGuid) throws Exception {
        // Create a test column & unlink Column from table
        AtlasEntity table = getEntity(tableGuid);
        table.setRelationshipAttribute("columns", new ArrayList<>(0));
        table.removeAttribute("columns");
        updateEntity(table);
        sleep(SLEEP);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToCompleteDelayed(tableGuid, TASK_TYPE_REFRESH_PROP);

        // Verify the column has received the expected classification
        verifyEntityNotHaveTags(columnGuid, tagTypeNames);

        return columnGuid;
    }

    private String updateColumnToLinkTable(String tableGuid) throws Exception {
        // Create a test column & link to table
        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        column0.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        String columnGuid = createEntitiesBulk(column0).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        // Wait for classification propagation tasks to complete
        //waitForPropagationTasksToComplete(columnGuid, TASK_TYPE_ADD_PROP);
        waitForPropagationTasksToCompleteDelayed(tableGuid, TASK_TYPE_ADD_PROP);

        sleep(SLEEP);
        // Verify the column has received the expected classification
        verifyEntityHasTags(columnGuid, tagTypeNames);

        return columnGuid;
    }

    private String updateColumnToUnlinkTable(String tableGuid, String columnGuid) throws Exception {
        // Create a test column & unlink Column from table
        AtlasEntity column = getEntity(columnGuid);
        column.removeAttribute("table");
        column.setRelationshipAttribute("table", null);
        column.removeAttribute("table");
        updateEntity(column);
        sleep(SLEEP);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToCompleteDelayed(tableGuid, TASK_TYPE_REFRESH_PROP);

        sleep(SLEEP);
        // Verify the column has received the expected classification
        verifyEntityNotHaveTags(columnGuid, tagTypeNames);

        return columnGuid;
    }

    private void updateSchemaToLinkDatabase(String databaseGuid, String schemaGuid,
                                         String tableGuid, String column0_Guid,
                                         String column1_Guid) throws Exception {
        // Add link between Database & Schema
        AtlasEntity schema = getEntity(schemaGuid);
        schema.setRelationshipAttribute("database", getObjectId(databaseGuid, TYPE_DATABASE));
        updateEntity(schema);
        sleep(SLEEP);

        // Wait for classification propagation tasks to complete
        //waitForPropagationTasksToComplete(columnGuid, TASK_TYPE_ADD_PROP);
        waitForPropagationTasksToCompleteDelayed(databaseGuid, TASK_TYPE_ADD_PROP);

        /*
        {
            "type": "CLASSIFICATION_PROPAGATION_ADD",
            "guid": "666d3e57-b710-4e36-918c-21c69241dabf",
            "createdBy": "service-account-atlan-argo",
            "createdTime": 1751526134832,
            "updatedTime": 1751526134832,
            "parameters": {
                "relationshipGuid": "3e62c54c-bae7-4aa3-bcbc-c233532debdb",
                "entityGuid": "d7cdd181-9fc1-4ca0-ad70-bd854305817d",
                "classificationVertexId": "14295076912"
            },
            "attemptCount": 0,
            "status": "PENDING",
            "classificationId": "14295076912",
            "entityGuid": "d7cdd181-9fc1-4ca0-ad70-bd854305817d",
            "classificationTypeName": "Qx79LklNtYoH5Sw1rUsnt3"
        }
        */

        /* new Task */
        /*
        *       {
            "type": "CLASSIFICATION_PROPAGATION_ADD",
            "guid": "210c2fa0-9c36-4a69-b81f-31ce5f736727",
            "createdBy": "service-account-atlan-argo",
            "createdTime": 1751576699274,
            "updatedTime": 1751576711815,
            "startTime": 1751576711719,
            "endTime": 1751576711813,
            "timeTakenInSeconds": 0,
            "parameters": {
                "__task_classificationTypeName": "",
                "toEntityGuid": "schemaGuid",
                "entityGuid": "database guid"
            },
            "attemptCount": 0,
            "status": "COMPLETE",
            "entityGuid": "database guid",
            "tagTypeName": ""
        },
        * */

        sleep(SLEEP);

        verifyEntityHasTags(databaseGuid, tagTypeNames);

        // Verify the assets has received the expected classification
        verifyEntityHasTags(schemaGuid, tagTypeNames);
        verifyEntityHasTags(tableGuid, tagTypeNames);
        verifyEntityHasTags(column0_Guid, tagTypeNames);
        verifyEntityHasTags(column1_Guid, tagTypeNames);
    }

    private void updateSchemaToUnlinkDatabase(String databaseGuid, String schemaGuid,
                                           String tableGuid, String column0_Guid,
                                           String column1_Guid) throws Exception {
        // Remove link between Database & Schema
        AtlasEntity schema = getEntity(schemaGuid);
        schema.setRelationshipAttribute("database", null);
        schema.removeAttribute("database");
        updateEntity(schema);
        sleep(SLEEP);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToCompleteDelayed(databaseGuid, TASK_TYPE_REFRESH_PROP);

        sleep(SLEEP);

        verifyEntityHasTags(databaseGuid, tagTypeNames);

        // Verify the assets has received the expected classification
        verifyEntityNotHaveTags(schemaGuid, tagTypeNames);
        verifyEntityNotHaveTags(tableGuid, tagTypeNames);
        verifyEntityNotHaveTags(column0_Guid, tagTypeNames);
        verifyEntityNotHaveTags(column1_Guid, tagTypeNames);
    }

}
