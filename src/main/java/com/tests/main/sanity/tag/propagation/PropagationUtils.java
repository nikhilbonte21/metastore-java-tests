package com.tests.main.sanity.tag.propagation;

import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getTagDefs;
import static com.tests.main.utils.TestUtil.mapOf;
import static com.tests.main.utils.TestUtil.searchTasks;
import static com.tests.main.utils.TestUtil.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PropagationUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PropagationUtils.class);

    public static final String TASK_TYPE_ADD_PROP = "CLASSIFICATION_PROPAGATION_ADD";
    public static final String TASK_TYPE_REFRESH_PROP = "CLASSIFICATION_REFRESH_PROPAGATION";
    public static final String TASK_TYPE_DELETE_PROP = "CLASSIFICATION_PROPAGATION_DELETE";

    public static String getTagTypeDef() throws RuntimeException {
        AtlasTypesDef typesDef = null;
        try {
            typesDef = getTagDefs();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        assertTrue("Sufficient tags not found", typesDef != null
                && CollectionUtils.isNotEmpty(typesDef.getClassificationDefs())
                && typesDef.getClassificationDefs().size() >= 1);

        String TAG_TYPE_NAME = typesDef.getClassificationDefs().get(0).getName();

        LOG.info("\n");
        LOG.info("====================================");
        LOG.info("TAG_TYPE_NAME : {}", TAG_TYPE_NAME);
        LOG.info("====================================");
        LOG.info("\n");

        return TAG_TYPE_NAME;
    }

    public static void waitForPropagationTasksToComplete(String entityGuid, String taskType) throws Exception {
        LOG.info("Waiting for propagation tasks to complete for entity: {}", entityGuid);

        int maxAttempts = 24; // Maximum number of attempts // 2 minutes
        long waitInterval = 5000; // 5 seconds between checks
        int attempts = 0;
        sleep(5000);

        while (attempts < maxAttempts) {
            attempts++;
            try {
                // Create task search request based on the curl example
                Map<String, Object> taskSearchRequest = createTaskSearchRequest(entityGuid, taskType);
                LOG.info("Finding pending task for entityGuid: {}, taskType {} attempts", entityGuid, taskType);

                // Search for tasks
                Map<String, Object> response = searchTasks(taskSearchRequest);

                // Check if tasks array is empty
                if (response != null && response.containsKey("tasks")) {
                    List<Map<String, Object>> tasks = (List<Map<String, Object>>) response.get("tasks");
                    if (tasks == null || tasks.isEmpty()) {
                        LOG.info("All propagation tasks completed after {} attempts", attempts);
                        return;
                    } else {
                        LOG.info("Found {} pending propagation tasks, attempt {}/{}", tasks.size(), attempts, maxAttempts);
                    }
                } else {
                    LOG.info("No tasks found in response");
                    return;
                }

                // Wait before checking again (except on last attempt)
                if (attempts < maxAttempts) {
                    sleep(waitInterval);
                }

            } catch (Exception e) {
                LOG.warn("Error checking task status on attempt {}: {}", attempts, e.getMessage());
                if (attempts < maxAttempts) {
                    sleep(waitInterval);
                }
            }
        }

        LOG.warn("Reached maximum attempts ({}) waiting for propagation tasks to complete", maxAttempts);
    }

    public static Map<String, Object> createTaskSearchRequest(String entityGuid, String taskType) throws Exception {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> dsl = mapOf("size", 1);

        // Set sort
        dsl.put("sort", mapOf("__task_timestamp", mapOf("order", "desc")));

        // Set query
        List<Map<String, Object>> mustConditions = new ArrayList<>();

        mustConditions.add(mapOf("term", mapOf("__task_type", taskType)));
        mustConditions.add(mapOf("term", mapOf("__task_entityGuid", entityGuid)));
        mustConditions.add(mapOf("term", mapOf("__task_status.keyword", "PENDING")));

        dsl.put("query", mapOf("bool", mapOf("must", mustConditions)));

        request.put("dsl", dsl);
        return request;
    }

    public static void verifyEntityHasTag(String assetGuid, String expectedTagName) throws Exception {
        LOG.info("Verifying entity {} has tag {}", assetGuid, expectedTagName);

        // Fetch the entity
        AtlasEntity entity = getEntity(assetGuid);

        // Check if the column has classifications
        List<AtlasClassification> classifications = entity.getClassifications();

        assertTrue("Entity should have tags", CollectionUtils.isNotEmpty(classifications));
        assertEquals(1, classifications.size());

        // Check if the expected tag is present
        boolean hasExpectedTag = classifications.stream()
                .anyMatch(classification -> expectedTagName.equals(classification.getTypeName()));

        assertTrue("Entity should have tag with typeName: " + expectedTagName, hasExpectedTag);

        LOG.info("Successfully verified entity has expected tags: {}", expectedTagName);
    }

    public static void verifyEntityNotHaveTag(String assetGuid, String unExpectedTagName) throws Exception {
        LOG.info("Verifying entity {} not have has tag {}", assetGuid, unExpectedTagName);

        // Fetch the entity
        AtlasEntity entity = getEntity(assetGuid);

        // Check if the column has classifications
        List<AtlasClassification> classifications = entity.getClassifications();

        if (CollectionUtils.isNotEmpty(classifications)) {
            boolean hasExpectedTag = classifications.stream()
                    .anyMatch(classification -> unExpectedTagName.equals(classification.getTypeName()));

            assertFalse("Entity should not tag with typeName: " + unExpectedTagName, hasExpectedTag);

        }

        LOG.info("Successfully verified entity has expected tags: {}", unExpectedTagName);
    }

}
