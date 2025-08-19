package com.tests.main.sanity.tag.propagation;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.FeatureFlagManager;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.tests.main.sanity.tag.propagation.PropagationUtils.TASK_TYPE_REFRESH_PROP;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.getTagTypeDefs;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.getTasks;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.verifyEntityNotHaveTags;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.waitForPropagationTasksToCompleteDelayed;
import static com.tests.main.utils.TestUtil.TYPE_COLUMN;
import static com.tests.main.utils.TestUtil.TYPE_PROCESS;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.deleteEntity;
import static com.tests.main.utils.TestUtil.deleteEntityHard;
import static com.tests.main.utils.TestUtil.deleteEntitySoft;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getObjectIdsAsList;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class ValidateNumberOfTasks implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(ValidateNumberOfTasks.class);

    private static long SLEEP = 2000;

    private static List<String> tagTypeNames;

    static {
        tagTypeNames = getTagTypeDefs(2);
    }

    public static void main(String[] args) throws Exception {
        try {
            new ValidateNumberOfTasks().run();
            //TestRunner.runTests(Propagation.class);
        } finally {
            //cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running ValidateNumberOfTasks tests");

        long start = System.currentTimeMillis();
        try {

            addAndRemoveColumnWithoutTag();
            addAndRemoveColumnWithTagPropFalse();
            addAndRemoveColumnWithTagPropTrue();
            addAndRemoveProcessWithTagPropTrueDifferentRequests();
            addAndDELETEProcessWithTagPropTrueDifferentRequests();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running ValidateNumberOfTasks tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private void addAndRemoveColumnWithoutTag() throws Exception {
        LOG.info(">> addAndRemoveColumnWithoutTag");
        // Create a test column & link to table

        Long currentMillis = System.currentTimeMillis();

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        AtlasEntity column1 = getAtlasEntity(TYPE_COLUMN, "test_column1" + getRandomName());
        AtlasEntity column2 = getAtlasEntity(TYPE_COLUMN, "test_column2" + getRandomName());
        Map<String, String> guidMappings = createEntitiesBulk(column0, column1, column2).getGuidAssignments();

        String column0Guid = guidMappings.get(column0.getGuid());
        String column1Guid = guidMappings.get(column1.getGuid());
        String column2Guid = guidMappings.get(column2.getGuid());
        sleep(SLEEP);
        assertEquals(0, getTasks(tableGuid, null, null, currentMillis).size());

        currentMillis = System.currentTimeMillis();
        table = getEntity(tableGuid);
        table.setRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, column0Guid, column1Guid, column2Guid));
        updateEntity(table);
        sleep(SLEEP);
        assertEquals(0, getTasks(tableGuid, null, null, currentMillis).size());
        assertEquals(0, getTasks(column0Guid, null, null, currentMillis).size());
        assertEquals(0, getTasks(column1Guid, null, null, currentMillis).size());
        assertEquals(0, getTasks(column2Guid, null, null, currentMillis).size());


        table = getEntity(tableGuid);
        table.setRelationshipAttribute("columns", Collections.EMPTY_LIST);
        updateEntity(table);
        sleep(SLEEP);
        assertEquals(0, getTasks(tableGuid, null, null, currentMillis).size());
        assertEquals(0, getTasks(column0Guid, null, null, currentMillis).size());
        assertEquals(0, getTasks(column1Guid, null, null, currentMillis).size());
        assertEquals(0, getTasks(column2Guid, null, null, currentMillis).size());

        LOG.info("<< addAndRemoveColumnWithoutTag");
    }

    private void addAndRemoveColumnWithTagPropFalse() throws Exception {
        LOG.info(">> addAndRemoveColumnWithTagPropFalse");
        // Create a test column & link to table

        Long currentMillis = System.currentTimeMillis();

        AtlasClassification classification = new AtlasClassification(tagTypeNames.get(0));
        AtlasClassification classification_1 = new AtlasClassification(tagTypeNames.get(1));

        classification.setPropagate(false);
        classification_1.setPropagate(false);

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        table.setClassifications(Arrays.asList(classification, classification_1));
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);
        assertEquals(0, getTasks(tableGuid, null, null, currentMillis).size());

        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        AtlasEntity column1 = getAtlasEntity(TYPE_COLUMN, "test_column1" + getRandomName());
        AtlasEntity column2 = getAtlasEntity(TYPE_COLUMN, "test_column2" + getRandomName());
        Map<String, String> guidMappings = createEntitiesBulk(column0, column1, column2).getGuidAssignments();

        String column0Guid = guidMappings.get(column0.getGuid());
        String column1Guid = guidMappings.get(column1.getGuid());
        String column2Guid = guidMappings.get(column2.getGuid());
        sleep(SLEEP);

        currentMillis = System.currentTimeMillis();
        table = getEntity(tableGuid);
        table.setRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, column0Guid, column1Guid, column2Guid));
        updateEntity(table);
        sleep(SLEEP);
        assertEquals(0, getTasks(tableGuid, null, null, currentMillis).size());
        assertEquals(0, getTasks(column0Guid, null, null, currentMillis).size());
        assertEquals(0, getTasks(column1Guid, null, null, currentMillis).size());
        assertEquals(0, getTasks(column2Guid, null, null, currentMillis).size());

        table = getEntity(tableGuid);
        table.setRelationshipAttribute("columns", Collections.EMPTY_LIST);
        updateEntity(table);
        sleep(SLEEP);
        assertEquals(0, getTasks(tableGuid, null, null, currentMillis).size());
        assertEquals(0, getTasks(column0Guid, null, null, currentMillis).size());
        assertEquals(0, getTasks(column1Guid, null, null, currentMillis).size());
        assertEquals(0, getTasks(column2Guid, null, null, currentMillis).size());

        LOG.info("<< addAndRemoveColumnWithTagPropFalse");
    }

    private void addAndRemoveColumnWithTagPropTrue() throws Exception {
        LOG.info(">> addAndRemoveColumnWithTagPropTrue");

        Long currentMillis = System.currentTimeMillis();

        AtlasClassification classification = new AtlasClassification(tagTypeNames.get(0));
        AtlasClassification classification_1 = new AtlasClassification(tagTypeNames.get(1));

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        table.setClassifications(Arrays.asList(classification, classification_1));
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);
        assertEquals(2, getTasks(tableGuid, null, null, currentMillis).size());

        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        AtlasEntity column1 = getAtlasEntity(TYPE_COLUMN, "test_column1" + getRandomName());
        AtlasEntity column2 = getAtlasEntity(TYPE_COLUMN, "test_column2" + getRandomName());
        Map<String, String> guidMappings = createEntitiesBulk(column0, column1, column2).getGuidAssignments();

        String column0Guid = guidMappings.get(column0.getGuid());
        String column1Guid = guidMappings.get(column1.getGuid());
        String column2Guid = guidMappings.get(column2.getGuid());
        sleep(SLEEP);

        currentMillis = System.currentTimeMillis();
        table = getEntity(tableGuid);
        table.setRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, column0Guid, column1Guid, column2Guid));
        updateEntity(table);
        sleep(SLEEP);
        if (FeatureFlagManager.isTagsV2Enabled()) {
            assertEquals(3, getTasks(tableGuid, "CLASSIFICATION_PROPAGATION_ADD", null, currentMillis).size());
        } else {
            assertEquals(2, getTasks(column0Guid, "CLASSIFICATION_PROPAGATION_ADD", null, currentMillis).size());
            assertEquals(2, getTasks(column1Guid, "CLASSIFICATION_PROPAGATION_ADD", null, currentMillis).size());
            assertEquals(2, getTasks(column2Guid, "CLASSIFICATION_PROPAGATION_ADD", null, currentMillis).size());
        }

        currentMillis = System.currentTimeMillis();
        table = getEntity(tableGuid);
        table.setRelationshipAttribute("columns", Collections.EMPTY_LIST);
        updateEntity(table);
        sleep(SLEEP);
        assertEquals(2, getTasks(tableGuid, "CLASSIFICATION_REFRESH_PROPAGATION", null, currentMillis).size());

        LOG.info("<< addAndRemoveColumnWithTagPropTrue");
    }

    private void addAndRemoveProcessWithTagPropTrueDifferentRequests() throws Exception {
        LOG.info(">> addAndRemoveProcessWithTagPropTrueDifferentRequests");

        Long currentMillis = System.currentTimeMillis();

        AtlasClassification classification = new AtlasClassification(tagTypeNames.get(0));

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        table.setClassifications(Arrays.asList(classification));
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);
        assertEquals(1, getTasks(tableGuid, null, null, currentMillis).size());

        AtlasEntity process0 = getAtlasEntity(TYPE_PROCESS, "test_process0" + getRandomName());
        AtlasEntity process1 = getAtlasEntity(TYPE_PROCESS, "test_process1" + getRandomName());
        AtlasEntity process2 = getAtlasEntity(TYPE_PROCESS, "test_process2" + getRandomName());
        Map<String, String> guidMappings = createEntitiesBulk(process0, process1, process2).getGuidAssignments();

        String process0Guid = guidMappings.get(process0.getGuid());
        String process1Guid = guidMappings.get(process1.getGuid());
        String process2Guid = guidMappings.get(process2.getGuid());
        sleep(SLEEP);

        currentMillis = System.currentTimeMillis();
        table = getEntity(tableGuid);
        table.setRelationshipAttribute("inputToProcesses", getObjectIdsAsList(TYPE_PROCESS, process0Guid, process1Guid, process2Guid));
        updateEntity(table);
        sleep(SLEEP);
        if (FeatureFlagManager.isTagsV2Enabled()) {
            assertEquals(3, getTasks(tableGuid, "CLASSIFICATION_PROPAGATION_ADD", null, currentMillis).size());
        } else {
            assertEquals(1, getTasks(process0Guid, "CLASSIFICATION_PROPAGATION_ADD", null, currentMillis).size());
            assertEquals(1, getTasks(process1Guid, "CLASSIFICATION_PROPAGATION_ADD", null, currentMillis).size());
            assertEquals(1, getTasks(process2Guid, "CLASSIFICATION_PROPAGATION_ADD", null, currentMillis).size());
        }

        currentMillis = System.currentTimeMillis();
        table = getEntity(tableGuid);

        table.setRelationshipAttribute("inputToProcesses", getObjectIdsAsList(TYPE_PROCESS, process0Guid, process1Guid));
        updateEntity(table);
        sleep(1000);
        //assertEquals(1, getTasks(null, "CLASSIFICATION_REFRESH_PROPAGATION", null, currentMillis).size());

        table.setRelationshipAttribute("inputToProcesses", getObjectIdsAsList(TYPE_PROCESS, process0Guid));
        updateEntity(table);
        sleep(1000);
        //assertEquals(1, getTasks(null, "CLASSIFICATION_REFRESH_PROPAGATION", null, currentMillis).size());

        table.setRelationshipAttribute("inputToProcesses", Collections.EMPTY_LIST);
        updateEntity(table);
        sleep(SLEEP);
        assertEquals(1, getTasks(tableGuid, "CLASSIFICATION_REFRESH_PROPAGATION", null, currentMillis).size());


        LOG.info("<< addAndRemoveProcessWithTagPropTrueDifferentRequests");
    }

    private void addAndDELETEProcessWithTagPropTrueDifferentRequests() throws Exception {
        LOG.info(">> addAndDELETEProcessWithTagPropTrueDifferentRequests");

        Long currentMillis = System.currentTimeMillis();

        AtlasClassification classification = new AtlasClassification(tagTypeNames.get(0));

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        table.setClassifications(Arrays.asList(classification));
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);
        assertEquals(1, getTasks(tableGuid, null, null, currentMillis).size());

        AtlasEntity process0 = getAtlasEntity(TYPE_PROCESS, "test_process0" + getRandomName());
        AtlasEntity process1 = getAtlasEntity(TYPE_PROCESS, "test_process1" + getRandomName());
        AtlasEntity process2 = getAtlasEntity(TYPE_PROCESS, "test_process2" + getRandomName());
        Map<String, String> guidMappings = createEntitiesBulk(process0, process1, process2).getGuidAssignments();

        String process0Guid = guidMappings.get(process0.getGuid());
        String process1Guid = guidMappings.get(process1.getGuid());
        String process2Guid = guidMappings.get(process2.getGuid());
        sleep(SLEEP);

        currentMillis = System.currentTimeMillis();
        table = getEntity(tableGuid);
        table.setRelationshipAttribute("inputToProcesses", getObjectIdsAsList(TYPE_PROCESS, process0Guid, process1Guid, process2Guid));
        updateEntity(table);
        sleep(SLEEP);
        if (FeatureFlagManager.isTagsV2Enabled()) {
            assertEquals(3, getTasks(tableGuid, "CLASSIFICATION_PROPAGATION_ADD", null, currentMillis).size());
        } else {
            assertEquals(1, getTasks(process0Guid, "CLASSIFICATION_PROPAGATION_ADD", null, currentMillis).size());
            assertEquals(1, getTasks(process1Guid, "CLASSIFICATION_PROPAGATION_ADD", null, currentMillis).size());
            assertEquals(1, getTasks(process2Guid, "CLASSIFICATION_PROPAGATION_ADD", null, currentMillis).size());
        }

        waitForPropagationTasksToCompleteDelayed(tableGuid, "CLASSIFICATION_PROPAGATION_ADD");

        currentMillis = System.currentTimeMillis();
        table = getEntity(tableGuid);

        deleteEntitySoft(process0Guid);
        sleep(1000);
        //assertEquals(1, getTasks(null, "CLASSIFICATION_REFRESH_PROPAGATION", null, currentMillis).size());

        deleteEntitySoft(process1Guid);
        sleep(1000);
        //assertEquals(1, getTasks(null, "CLASSIFICATION_REFRESH_PROPAGATION", null, currentMillis).size());

        deleteEntitySoft(process2Guid);
        sleep(SLEEP);
        assertEquals(1, getTasks(tableGuid, "CLASSIFICATION_REFRESH_PROPAGATION", null, currentMillis).size());


        LOG.info("<< addAndDELETEProcessWithTagPropTrueDifferentRequests");
    }
}

