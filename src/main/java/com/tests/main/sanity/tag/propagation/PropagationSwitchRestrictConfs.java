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
import java.util.Collections;
import java.util.Map;

import static com.tests.main.sanity.tag.propagation.PropagationUtils.TASK_TYPE_ADD_PROP;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.TASK_TYPE_DELETE_PROP;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.TASK_TYPE_REFRESH_PROP;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.getTagTypeDef;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.verifyEntityHasTag;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.verifyEntityNotHaveTag;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.waitForPropagationTasksToComplete;
import static com.tests.main.utils.TestUtil.TYPE_COLUMN;
import static com.tests.main.utils.TestUtil.TYPE_PROCESS;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getObjectIdsAsList;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;


public class PropagationSwitchRestrictConfs implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(PropagationSwitchRestrictConfs.class);

    private static long SLEEP = 2000;

    private static String TAG_TYPE_NAME;

    private static String table_guid;
    private static String column_0_guid;
    private static String column_1_guid;
    private static String process_0_guid;
    private static String process_1_guid;

    static {
        TAG_TYPE_NAME = getTagTypeDef();
        try {
            createDataset();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            new PropagationSwitchRestrictConfs().run();
            //TestRunner.runTests(PropagationSwitchRestrictConfs.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running PropagationSwitchRestrictConfs tests");

        long start = System.currentTimeMillis();
        try {

            // ---------------------------

            updateTableTagDisableLineage();
            toggleConfs(); // enable lineage, disable hierarchy
            tagToggleConfs2(); // disable lineage, enable hierarchy

            enableAllConfs(); // disable lineage, enable hierarchy

            disablePropagation();
            reEnablePropagation();
        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running Propagation tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }


    public static void createDataset() throws Exception {
        LOG.info(">> createDataset");
        Thread.currentThread().setName("main-createDataset");

        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "test_column_0" + getRandomName());
        AtlasEntity column_1 = getAtlasEntity(TYPE_COLUMN, "test_column_1" + getRandomName());

        AtlasEntity process_0 = getAtlasEntity(TYPE_PROCESS, "test_process_0" + getRandomName());
        AtlasEntity process_1 = getAtlasEntity(TYPE_PROCESS, "test_process_1" + getRandomName());

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        table.setRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, column_0.getGuid(), column_1.getGuid()));
        table.setRelationshipAttribute("inputToProcesses", getObjectIdsAsList(TYPE_PROCESS, process_0.getGuid(), process_1.getGuid()));

        Map<String, String> guidAssignments = createEntitiesBulk(table, column_0, column_1, process_0, process_1).getGuidAssignments();
        table_guid = guidAssignments.get(table.getGuid());
        column_0_guid = guidAssignments.get(column_0.getGuid());
        column_1_guid = guidAssignments.get(column_1.getGuid());
        process_0_guid = guidAssignments.get(process_0.getGuid());
        process_1_guid = guidAssignments.get(process_1.getGuid());
        sleep(SLEEP);

        // Attach tag to table
        table = getEntity(table_guid);
        table.setClassifications(Collections.singletonList(new AtlasClassification(TAG_TYPE_NAME)));
        updateEntity(table);
        sleep(SLEEP);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToComplete(table_guid, TASK_TYPE_ADD_PROP);

        sleep(SLEEP);
        // Verify the column has received the expected classification
        verifyEntityHasTag(table_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(column_0_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(column_1_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(process_0_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(process_1_guid, TAG_TYPE_NAME);

        LOG.info("<< createDataset");
    }

    @Test
    public void updateTableTagDisableLineage() throws Exception {
        LOG.info(">> updateTableTagDisableLineage");
        Thread.currentThread().setName("main-updateTableTagDisableLineage");

        AtlasEntity table = getEntity(table_guid);

        AtlasClassification tag = new AtlasClassification(TAG_TYPE_NAME);
        tag.setPropagate(true);
        tag.setRestrictPropagationThroughLineage(true);
        tag.setRestrictPropagationThroughHierarchy(false);

        table.setClassifications(Collections.singletonList(tag));
        updateEntity(table);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToComplete(table_guid, TASK_TYPE_REFRESH_PROP);

        // Verify the column has received the expected classification
        verifyEntityHasTag(table_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(column_0_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(column_1_guid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(process_0_guid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(process_1_guid, TAG_TYPE_NAME);

        LOG.info("<< updateTableTagDisableLineage");
    }

    @Test
    public void toggleConfs() throws Exception {
        LOG.info(">> updateTableTagToggleConfs");

        AtlasEntity table = getEntity(table_guid);

        AtlasClassification tag = new AtlasClassification(TAG_TYPE_NAME);
        tag.setPropagate(true);
        tag.setRestrictPropagationThroughLineage(false);
        tag.setRestrictPropagationThroughHierarchy(true);

        table.setClassifications(Collections.singletonList(tag));
        updateEntity(table);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToComplete(table_guid, TASK_TYPE_REFRESH_PROP);

        // Verify the column has received the expected classification
        verifyEntityHasTag(table_guid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(column_0_guid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(column_1_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(process_0_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(process_1_guid, TAG_TYPE_NAME);

        LOG.info("<< updateTableTagToggleConfs");
    }

    @Test
    public void tagToggleConfs2() throws Exception {
        LOG.info(">> updateTableTagToggleConfs2");
        // disable lineage, enable hierarchy

        AtlasEntity table = getEntity(table_guid);

        AtlasClassification tag = new AtlasClassification(TAG_TYPE_NAME);
        tag.setPropagate(true);
        tag.setRestrictPropagationThroughLineage(true);
        tag.setRestrictPropagationThroughHierarchy(false);

        table.setClassifications(Collections.singletonList(tag));
        updateEntity(table);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToComplete(table_guid, TASK_TYPE_REFRESH_PROP);

        // Verify the column has received the expected classification
        verifyEntityHasTag(table_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(column_0_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(column_1_guid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(process_0_guid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(process_1_guid, TAG_TYPE_NAME);

        LOG.info("<< updateTableTagToggleConfs2");
    }

    @Test
    public void enableAllConfs() throws Exception {
        LOG.info(">> enableAllConfs");
        // enable all

        AtlasEntity table = getEntity(table_guid);

        AtlasClassification tag = new AtlasClassification(TAG_TYPE_NAME);
        tag.setPropagate(true);
        tag.setRestrictPropagationThroughLineage(false);
        tag.setRestrictPropagationThroughHierarchy(false);

        table.setClassifications(Collections.singletonList(tag));
        updateEntity(table);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToComplete(table_guid, TASK_TYPE_REFRESH_PROP);

        // Verify the column has received the expected classification
        verifyEntityHasTag(table_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(column_0_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(column_1_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(process_0_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(process_1_guid, TAG_TYPE_NAME);

        LOG.info("<< enableAllConfs");
    }

    @Test
    public void disablePropagation() throws Exception {
        LOG.info(">> disablePropagation");

        AtlasEntity table = getEntity(table_guid);

        AtlasClassification tag = new AtlasClassification(TAG_TYPE_NAME);
        tag.setPropagate(false);

        table.setClassifications(Collections.singletonList(tag));
        updateEntity(table);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToComplete(table_guid, TASK_TYPE_DELETE_PROP);

        // Verify the column has received the expected classification
        verifyEntityHasTag(table_guid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(column_0_guid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(column_1_guid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(process_0_guid, TAG_TYPE_NAME);
        verifyEntityNotHaveTag(process_1_guid, TAG_TYPE_NAME);

        LOG.info("<< disablePropagation");
    }

    @Test
    public void reEnablePropagation() throws Exception {
        LOG.info(">> reEnablePropagation");
        // enable lineage, enable hierarchy

        AtlasEntity table = getEntity(table_guid);

        AtlasClassification tag = new AtlasClassification(TAG_TYPE_NAME);
        tag.setPropagate(true);

        table.setClassifications(Collections.singletonList(tag));
        updateEntity(table);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToComplete(table_guid, TASK_TYPE_ADD_PROP);
        /*
        {
            "type": "CLASSIFICATION_PROPAGATION_ADD",
            "guid": "ecde4ef5-98a6-48a2-ad32-cdf01b8e3e1f",
            "createdBy": "service-account-atlan-argo",
            "createdTime": 1751468926601,
            "updatedTime": 1751468926601,
            "parameters": {
                "relationshipGuid": null,
                "previousRestrictPropagationThroughHierarchy": false,
                "entityGuid": "26ec2ad4-07ea-4de9-ab7f-8e9bbc401035",
                "classificationVertexId": "15524036680",
                "previousRestrictPropagationThroughLineage": false
            },
            "attemptCount": 0,
            "status": "PENDING",
            "classificationId": "15524036680",
            "entityGuid": "26ec2ad4-07ea-4de9-ab7f-8e9bbc401035",
            "classificationTypeName": "Qx79LklNtYoH5Sw1rUsnt3"
        }
        * */

        // Verify the column has received the expected classification
        verifyEntityHasTag(table_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(column_0_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(column_1_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(process_0_guid, TAG_TYPE_NAME);
        verifyEntityHasTag(process_1_guid, TAG_TYPE_NAME);

        LOG.info("<< reEnablePropagation");
    }
}
