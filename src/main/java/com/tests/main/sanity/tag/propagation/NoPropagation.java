package com.tests.main.sanity.tag.propagation;

import com.tests.main.Test;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.tests.main.sanity.tag.propagation.PropagationUtils.TASK_TYPE_ADD_PROP;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.TASK_TYPE_REFRESH_PROP;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.getTagTypeDefs;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.getTask;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.verifyEntityHasTags;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.verifyEntityNotHaveTags;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.waitForPropagationTasksToCompleteDelayed;
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
import static org.junit.Assert.assertTrue;


public class NoPropagation implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(NoPropagation.class);

    private static long SLEEP = 2000;

    private static List<String> tagTypeNames;

    static {
        tagTypeNames = getTagTypeDefs(2);
    }

    public static void main(String[] args) throws Exception {
        try {
            new NoPropagation().run();
            //TestRunner.runTests(Propagation.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running NoPropagation tests");

        long start = System.currentTimeMillis();
        try {
            tableColumnAddRemove();

            tableWithFalsePropTagColumnAddRemove();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running NoPropagation tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    @Test
    public void tableColumnAddRemove() throws Exception {
        LOG.info(">> tableColumnAddRemove");

        // Create a test table with tags
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());


        table.setClassifications(Arrays.asList());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        String columnGuid = updateTableToLinkColumn(tableGuid);

        updateTableToUnlinkColumn(tableGuid, columnGuid);

        LOG.info("<< tableColumnAddRemove");
    }

    @Test
    public void tableWithFalsePropTagColumnAddRemove() throws Exception {
        LOG.info(">> tableWithFalsePropTagColumnAddRemove");

        AtlasClassification tag = new AtlasClassification(tagTypeNames.get(0));
        tag.setPropagate(false);

        AtlasClassification tag1 = new AtlasClassification(tagTypeNames.get(1));
        tag1.setPropagate(false);

        // Create a test table with tags
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());


        table.setClassifications(Arrays.asList(tag, tag1));
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        List<String> tagsToValidate = Arrays.asList(tag.getTypeName(), tag1.getTypeName());

        String columnGuid = updateTableToLinkColumn(tableGuid);
        verifyEntityHasTags(tableGuid, tagsToValidate);

        updateTableToUnlinkColumn(tableGuid, columnGuid);
        verifyEntityHasTags(tableGuid, tagsToValidate);

        LOG.info("<< tableWithFalsePropTagColumnAddRemove");
    }


    private String updateTableToLinkColumn(String tableGuid) throws Exception {
        // Create a test column & link to table

        Long currentMillis = System.currentTimeMillis();

        AtlasEntity column0 = getAtlasEntity(TYPE_COLUMN, "test_column0" + getRandomName());
        String columnGuid = createEntitiesBulk(column0).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        AtlasEntity table = getEntity(tableGuid);
        table.setRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, columnGuid));
        updateEntity(table);
        sleep(SLEEP);

        assertTrue(CollectionUtils.isEmpty(getTask(columnGuid, TASK_TYPE_REFRESH_PROP, "PENDING", currentMillis))) ;
        assertTrue(CollectionUtils.isEmpty(getTask(tableGuid, TASK_TYPE_REFRESH_PROP, "PENDING", currentMillis)));
        assertTrue(CollectionUtils.isEmpty(getTask(columnGuid, TASK_TYPE_REFRESH_PROP, "IN_PROGRESS", currentMillis)));
        assertTrue(CollectionUtils.isEmpty(getTask(tableGuid, TASK_TYPE_REFRESH_PROP, "IN_PROGRESS", currentMillis)));
        assertTrue(CollectionUtils.isEmpty(getTask(columnGuid, TASK_TYPE_REFRESH_PROP, "COMPLETE", currentMillis)));
        assertTrue(CollectionUtils.isEmpty(getTask(tableGuid, TASK_TYPE_REFRESH_PROP, "COMPLETE", currentMillis)));


        sleep(SLEEP);
        // Verify the column has not received the expected classification
        verifyEntityNotHaveTags(columnGuid, tagTypeNames);

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
}
