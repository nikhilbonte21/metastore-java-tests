package com.tests.main.sanity.tag.propagation;

import com.tests.main.Test;
import com.tests.main.TestRunner;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

import static com.tests.main.sanity.tag.propagation.PropagationUtils.TASK_TYPE_ADD_PROP;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.TASK_TYPE_DELETE_PROP;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.TASK_TYPE_REFRESH_PROP;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.getTagTypeDef;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.verifyEntityHasTag;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.verifyEntityNotHaveTag;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.waitForPropagationTasksToComplete;
import static com.tests.main.utils.TestUtil.TYPE_COLUMN;
import static com.tests.main.utils.TestUtil.TYPE_DATABASE;
import static com.tests.main.utils.TestUtil.TYPE_SCHEMA;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.deleteEntity;
import static com.tests.main.utils.TestUtil.deleteEntitySoft;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getObjectId;
import static com.tests.main.utils.TestUtil.getObjectIdsAsList;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;


public class PropagationDeleteAsset implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(PropagationDeleteAsset.class);

    private static long SLEEP = 2000;

    private static String TAG_TYPE_NAME;

    static {
        TAG_TYPE_NAME = getTagTypeDef();
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
            //deleteTable();
            deleteSchema();


        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running PropagationDeleteAsset tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    @Test
    public void deleteTable() throws Exception {
        LOG.info(">> deleteTable");

        // Create a test table with tags
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        table.setClassifications(Arrays.asList(new AtlasClassification(TAG_TYPE_NAME)));
        String tableGuid = createEntitiesBulk(table).getCreatedEntities().get(0).getGuid();

        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        AtlasEntity column1 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        String column0_Guid = createEntity(column0).getCreatedEntities().get(0).getGuid();
        String column1_Guid = createEntity(column1).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        waitForPropagationTasksToComplete(tableGuid, TASK_TYPE_ADD_PROP);

        deleteEntitySoft(tableGuid);

        waitForPropagationTasksToComplete(tableGuid, TASK_TYPE_DELETE_PROP);

        verifyEntityHasTag(tableGuid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(column0_Guid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(column1_Guid, TAG_TYPE_NAME);

        LOG.info("<< deleteTable");
    }

    @Test
    public void deleteSchema() throws Exception {
        LOG.info(">> deleteSchema");

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
        database.setClassifications(Arrays.asList(new AtlasClassification(TAG_TYPE_NAME)));
        updateEntity(database);

        waitForPropagationTasksToComplete(databaseGuid, TASK_TYPE_ADD_PROP);
        verifyEntityHasTag(column0_Guid, TAG_TYPE_NAME);

        deleteEntitySoft(schemaGuid);

        waitForPropagationTasksToComplete(databaseGuid, TASK_TYPE_REFRESH_PROP);

        verifyEntityHasTag(databaseGuid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(schemaGuid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(tableGuid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(column0_Guid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(column1_Guid, TAG_TYPE_NAME);

        LOG.info("<< deleteSchema");
    }
}
