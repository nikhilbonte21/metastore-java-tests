package com.tests.main.sanity.tag.propagation;

import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.tasks.AtlasTask;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.tests.main.utils.TestUtil.createClassification;
import static com.tests.main.utils.TestUtil.createClassificationDefs;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getRandomName;
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

    public static List<String> getTagTypeDefs(int expectedCount) throws RuntimeException {
        AtlasTypesDef typesDef = null;
        try {
            typesDef = getTagDefs();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        List<String> tagTypeNames = new ArrayList<>(expectedCount);

        if (typesDef == null
                || CollectionUtils.isEmpty(typesDef.getClassificationDefs())
                || typesDef.getClassificationDefs().size() < expectedCount) {

            LOG.warn("Sufficient tags not found, creating tags");

            List<AtlasClassificationDef> tags = new ArrayList<>(expectedCount);
            for (int i = 0; i < expectedCount; i++) {
                AtlasClassificationDef classificationDef = new AtlasClassificationDef();
                classificationDef.setDisplayName("tag_" + i + "_" + getRandomName());
                classificationDef.setName("tag_" + i + "_" + getRandomName());

                tags.add(classificationDef);
            }

            try {
                tagTypeNames = createClassificationDefs(tags).stream().map(x -> x.getName()).collect(Collectors.toList());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            LOG.info("Created new tag types");
        } else {

            tagTypeNames = typesDef.getClassificationDefs().subList(0, expectedCount).stream().map(x -> x.getName()).collect(Collectors.toList());

            LOG.info("Fetched existing tag types");
        }

        LOG.info("\n");
        LOG.info("====================================");
        LOG.info("tagTypeNames : {}", tagTypeNames);
        LOG.info("====================================");
        LOG.info("\n");

        return tagTypeNames;
    }

    public static List<AtlasTask> getTask(String entityGuid, String taskType, String taskStatus, long currentMillis) throws Exception {
        Map<String, Object> taskSearchRequest = createTaskSearchRequest(entityGuid, taskType, taskStatus, currentMillis);

        return (List<AtlasTask>) searchTasks(taskSearchRequest).get("tasks");
    }

    public static void waitForPropagationTasksToCompleteDelayed(String entityGuid, String taskType) throws Exception {
        sleep(5000);
        waitForPropagationTasksToComplete(entityGuid, taskType);
    }

    public static void waitForPropagationTasksToComplete(String entityGuid, String taskType) throws  Exception {
        LOG.info("Waiting for propagation tasks to complete for entity: {}", entityGuid);

        int maxAttempts = 24; // Maximum number of attempts // 2 minutes
        long waitInterval = 5000; // 5 seconds between checks
        int attempts = 0;

        while (attempts < maxAttempts) {
            attempts++;
            try {
                // Create task search request based on the curl example
                Map<String, Object> taskSearchRequest = createTaskSearchRequest(entityGuid, taskType, "PENDING");
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

    public static Map<String, Object> createTaskSearchRequest(String entityGuid, String taskType, String taskStatus, long timeInMillis) throws Exception {
        Map<String, Object> request = new HashMap<>();
        Map<String, Object> dsl = mapOf("size", 1);

        // Set sort
        dsl.put("sort", mapOf("__task_timestamp", mapOf("order", "desc")));

        // Set query
        List<Map<String, Object>> mustConditions = new ArrayList<>();

        if (StringUtils.isNotEmpty(taskType)) {
            mustConditions.add(mapOf("term", mapOf("__task_type", taskType)));
        }

        if (StringUtils.isNotEmpty(entityGuid)) {
            mustConditions.add(mapOf("term", mapOf("__task_entityGuid", entityGuid)));
        }

        if (StringUtils.isNotEmpty(taskStatus)) {
            mustConditions.add(mapOf("term", mapOf("__task_status.keyword", taskStatus)));
        }

        if (timeInMillis > 0) {
            mustConditions.add(mapOf("range", mapOf("created", mapOf("gt", timeInMillis))));
        }

        dsl.put("query", mapOf("bool", mapOf("must", mustConditions)));

        request.put("dsl", dsl);
        return request;
    }

    public static Map<String, Object> createTaskSearchRequest(String entityGuid, String taskType, String taskStatus) throws Exception {
        return createTaskSearchRequest(entityGuid, taskType, taskStatus, 0);
    }

    public static void verifyEntityHasTags(String assetGuid, List<String> expectedTagNames) throws Exception {
        LOG.info("Verifying entity {} has tags {}", assetGuid, expectedTagNames);

        // Fetch the entity
        AtlasEntity entity = getEntity(assetGuid);

        // Check if the column has classifications
        List<AtlasClassification> classifications = entity.getClassifications();

        assertTrue("Entity should have tags", CollectionUtils.isNotEmpty(classifications));
        assertEquals(expectedTagNames.size(), classifications.size());

        // Check if the expected tags are present
        boolean hasExpectedTags = classifications.stream()
                .anyMatch(classification -> expectedTagNames.contains(classification.getTypeName()));

        assertTrue("Entity should have tags with typeName: " + expectedTagNames, hasExpectedTags);
    }

    public static void verifyEntityNotHaveTags(String assetGuid, List<String> unExpectedTagNames) throws Exception {
        LOG.info("Verifying entity {} not have has tags {}", assetGuid, unExpectedTagNames);

        // Fetch the entity
        AtlasEntity entity = getEntity(assetGuid);

        // Check if the column has classifications
        List<AtlasClassification> classifications = entity.getClassifications();

        if (CollectionUtils.isNotEmpty(classifications)) {
            boolean hasExpectedTag = classifications.stream()
                    .anyMatch(classification -> unExpectedTagNames.contains(classification.getTypeName()));

            assertFalse("Entity should not tags: " + unExpectedTagNames, hasExpectedTag);

        }

        LOG.info("Successfully verified entity does not have tags: {}", unExpectedTagNames);
    }
}
