package com.tests.main.sanity.tag.propagation;

import com.tests.main.Test;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.tests.main.sanity.tag.propagation.PropagationUtils.*;
import static com.tests.main.utils.ESUtil.overrideESDocByGuid;
import static com.tests.main.utils.ESUtil.updateESDocByGuid;
import static com.tests.main.utils.TestUtil.TYPE_COLUMN;
import static com.tests.main.utils.TestUtil.TYPE_DATABASE;
import static com.tests.main.utils.TestUtil.TYPE_SCHEMA;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getESDoc;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getObjectId;
import static com.tests.main.utils.TestUtil.getObjectIdsAsList;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.repairClassificationsMappings;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;
import static com.tests.main.utils.TestUtil.verifyESAttributes;
import static com.tests.main.utils.TestUtil.verifyESHasNot;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class RepairTagsMappings implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(RepairTagsMappings.class);

    private static long SLEEP = 2000;

    private static List<String> tagTypeNames;

    static {
        tagTypeNames = getTagTypeDefs(3);
    }

    public static void main(String[] args) throws Exception {
        try {
            new RepairTagsMappings().run();
            //TestRunner.runTests(RepairTagsMappings.class);
        } finally {
            //cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running RepairTagsMappings tests");

        long start = System.currentTimeMillis();
        try {
            /*invalidGuid();
            noTags();
            oneDirectTag();
            onePropagatedTag();
            manyDirectManyPropagatedTags();*/

            manyAssetsOneDirectTags();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running RepairTagsMappings tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    public void invalidGuid() throws Exception {
        LOG.info("Running invalidGuid test");

        Map<String, String> errorMapEmpty = repairClassificationsMappings(Collections.singletonList(""));
        Map<String, String> errorMapNull = repairClassificationsMappings(Collections.singletonList(null));
        Map<String, String> errorMapInvalidGuid = repairClassificationsMappings(Collections.singletonList("d980af42-dd9a-418c-bcd3-f9d2bebe3f63"));
        Map<String, String> errorMapInvalidGuids = repairClassificationsMappings(Arrays.asList(
                "d980af42-dd9a-418c-bcd3-f9d2bebe3f63",
                "0e483029-61a3-4bf0-9b23-2441deb7bfaa",
                "1234"
        ));

        sleep(SLEEP);

        assertEquals(1, errorMapEmpty.size());
        errorMapEmpty.values().forEach(value -> assertTrue("Given instance guid  is invalid/not found".equals(value)));


        assertEquals(1, errorMapNull.size());
        //assertTrue("Given instance guid  is invalid/not found".equals(errorMap.get(null)));
        errorMapNull.forEach((key, value) -> assertTrue("Given instance guid null is invalid/not found".equals(value)));

        assertEquals(1, errorMapInvalidGuid.size());
        assertTrue("Given instance guid d980af42-dd9a-418c-bcd3-f9d2bebe3f63 is invalid/not found".equals(errorMapInvalidGuid.get("d980af42-dd9a-418c-bcd3-f9d2bebe3f63")));

        assertEquals(3, errorMapInvalidGuids.size());
        assertTrue("Given instance guid d980af42-dd9a-418c-bcd3-f9d2bebe3f63 is invalid/not found".equals(errorMapInvalidGuids.get("d980af42-dd9a-418c-bcd3-f9d2bebe3f63")));
        assertTrue("Given instance guid 0e483029-61a3-4bf0-9b23-2441deb7bfaa is invalid/not found".equals(errorMapInvalidGuids.get("0e483029-61a3-4bf0-9b23-2441deb7bfaa")));
        assertTrue("Given instance guid 1234 is invalid/not found".equals(errorMapInvalidGuids.get("1234")));


        LOG.info("invalidGuid test completed successfully");
    }

    public void noTags() throws Exception {
        LOG.info("Running noTags test");

        // Step 1: Create a table
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();

        sleep(SLEEP);
        LOG.info("Created table with GUID: {}", tableGuid);

        // Step 2: Save original ES document state
        Map<String, Object> originalESDoc = getESDoc(tableGuid);
        LOG.info("Saved original ES document state for table GUID: {}", tableGuid);

        // Step 3: Corrupt ES document with rubbish values for classification attributes
        Map<String, Object> corruptedAttributes = new java.util.HashMap<>();
        corruptedAttributes.put("__classificationsText", "rubbish_classification_text");
        corruptedAttributes.put("__classificationNames", "rubbish_classification_name");
        corruptedAttributes.put("__propagatedClassificationNames", "rubbish_propagated_name");
        corruptedAttributes.put("__traitNames", Arrays.asList("rubbish_trait1", "rubbish_trait2"));
        corruptedAttributes.put("__propagatedTraitNames", Arrays.asList("rubbish_prop_trait1", "rubbish_prop_trait2"));

        // Update ES document with corrupted values
        updateESDocByGuid(tableGuid, corruptedAttributes);
        LOG.info("Corrupted ES document attributes for table GUID: {}", tableGuid);

        // Step 4: Call repair classifications mappings API
        repairClassificationsMappings(Arrays.asList(tableGuid));
        sleep(SLEEP);

        // Step 5: Verify all original attributes are restored to their original state
        verifyESAttributes(tableGuid, originalESDoc);

        // Step 6: Verify that the corrupted rubbish attributes are no longer present
        verifyESHasNot(tableGuid, "__classificationsText", "__classificationNames",
                "__propagatedClassificationNames", "__traitNames", "__propagatedTraitNames");

        LOG.info("noTags test completed successfully");
    }

    public void oneDirectTag() throws Exception {
        LOG.info("Running oneDirectTag test");

        // Step 1: Create a table
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_" + getRandomName());
        table.setClassifications(Collections.singletonList(new AtlasClassification(tagTypeNames.get(0))));
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();

        sleep(SLEEP);

        // Step 2: Save original ES document state
        Map<String, Object> originalESDoc = getESDoc(tableGuid);
        LOG.info("Saved original ES document state for table GUID: {}", tableGuid);

        // Step 3: Corrupt ES document with rubbish values for classification attributes
        Map<String, Object> corruptedAttributes = new java.util.HashMap<>();
        corruptedAttributes.put("__classificationsText", "rubbish_classification_text");
        corruptedAttributes.put("__classificationNames", "rubbish_classification_name");
        corruptedAttributes.put("__propagatedClassificationNames", "rubbish_propagated_name");
        corruptedAttributes.put("__traitNames", Arrays.asList("rubbish_trait1", "rubbish_trait2"));
        corruptedAttributes.put("__propagatedTraitNames", Arrays.asList("rubbish_prop_trait1", "rubbish_prop_trait2"));

        // Update ES document with corrupted values
        updateESDocByGuid(tableGuid, corruptedAttributes);
        LOG.info("Corrupted ES document attributes for table GUID: {}", tableGuid);

        // Step 4: Call repair classifications mappings API
        repairClassificationsMappings(Arrays.asList(tableGuid));
        sleep(SLEEP);

        // Step 5: Verify all original attributes are restored to their original state
        verifyESAttributes(tableGuid, originalESDoc);

        // Corrupt ES document by removing classification attributes
        Map<String, Object> copyOfAttributes = new HashMap<>();
        originalESDoc.keySet().forEach(orig -> copyOfAttributes.put(orig, originalESDoc.get(orig)));
        copyOfAttributes.remove("__classificationsText");
        copyOfAttributes.remove("__classificationNames");
        copyOfAttributes.remove("__propagatedClassificationNames");
        copyOfAttributes.remove("__traitNames");
        copyOfAttributes.remove("__propagatedTraitNames");

        overrideESDocByGuid(tableGuid, copyOfAttributes);

        verifyESHasNot(tableGuid, "__classificationsText", "__classificationNames",
                "__propagatedClassificationNames", "__traitNames", "__propagatedTraitNames");

        // Call repair classifications mappings API
        repairClassificationsMappings(Arrays.asList(tableGuid));
        sleep(SLEEP);

        // Verify all original attributes are restored to their original state
        verifyESAttributes(tableGuid, originalESDoc);

        LOG.info("oneDirectTag test completed successfully");
    }

    public void onePropagatedTag() throws Exception {
        LOG.info("Running onePropagatedTag test");

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        table.setClassifications(Arrays.asList(new AtlasClassification(tagTypeNames.get(0))));
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();

        AtlasEntity column = getAtlasEntity(TYPE_COLUMN, "test_column" + getRandomName());
        String columnGuid = createEntitiesBulk(column).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        table = getEntity(tableGuid);
        table.setRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, columnGuid));
        updateEntity(table);
        sleep(SLEEP);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToCompleteDelayed(columnGuid, TASK_TYPE_ADD_PROP);
        waitForPropagationTasksToComplete(tableGuid, TASK_TYPE_ADD_PROP);

        // Step 2: Save original ES document state
        Map<String, Object> originalESDoc = getESDoc(columnGuid);
        LOG.info("Saved original ES document state for table GUID: {}", columnGuid);

        // Step 3: Corrupt ES document with rubbish values for classification attributes
        Map<String, Object> corruptedAttributes = new java.util.HashMap<>();
        corruptedAttributes.put("__classificationsText", "rubbish_classification_text");
        corruptedAttributes.put("__classificationNames", "rubbish_classification_name");
        corruptedAttributes.put("__propagatedClassificationNames", "rubbish_propagated_name");
        corruptedAttributes.put("__traitNames", Arrays.asList("rubbish_trait1", "rubbish_trait2"));
        corruptedAttributes.put("__propagatedTraitNames", Arrays.asList("rubbish_prop_trait1", "rubbish_prop_trait2"));

        // Update ES document with corrupted values
        updateESDocByGuid(columnGuid, corruptedAttributes);
        LOG.info("Corrupted ES document attributes for table GUID: {}", columnGuid);

        // Step 4: Call repair classifications mappings API
        repairClassificationsMappings(Arrays.asList(columnGuid));
        sleep(SLEEP);

        // Step 5: Verify all original attributes are restored to their original state
        verifyESAttributes(columnGuid, originalESDoc);

        // Corrupt ES document by removing classification attributes
        Map<String, Object> copyOfAttributes = new HashMap<>();
        originalESDoc.keySet().forEach(orig -> copyOfAttributes.put(orig, originalESDoc.get(orig)));
        copyOfAttributes.remove("__classificationsText");
        copyOfAttributes.remove("__classificationNames");
        copyOfAttributes.remove("__propagatedClassificationNames");
        copyOfAttributes.remove("__traitNames");
        copyOfAttributes.remove("__propagatedTraitNames");

        overrideESDocByGuid(columnGuid, copyOfAttributes);

        verifyESHasNot(columnGuid, "__classificationsText", "__classificationNames",
                "__propagatedClassificationNames", "__traitNames", "__propagatedTraitNames");

        // Call repair classifications mappings API
        repairClassificationsMappings(Arrays.asList(columnGuid));
        sleep(SLEEP);

        // Verify all original attributes are restored to their original state
        verifyESAttributes(columnGuid, originalESDoc);

        LOG.info("onePropagatedTag test completed successfully");
    }

    public void manyDirectManyPropagatedTags() throws Exception {
        LOG.info("Running manyDirectManyPropagatedTags test");

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        table.setClassifications(Arrays.asList(new AtlasClassification(tagTypeNames.get(0))));
        table.setClassifications(Arrays.asList(new AtlasClassification(tagTypeNames.get(1))));
        String tableGuid = createEntitiesBulk(table).getCreatedEntities().get(0).getGuid();

        AtlasEntity column = getAtlasEntity(TYPE_COLUMN, "test_column" + getRandomName());
        column.setClassifications(Arrays.asList(new AtlasClassification(tagTypeNames.get(1))));
        column.setClassifications(Arrays.asList(new AtlasClassification(tagTypeNames.get(2))));
        String columnGuid = createEntitiesBulk(column).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        table = getEntity(tableGuid);
        table.setRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, columnGuid));
        updateEntity(table);
        sleep(SLEEP);

        // Wait for classification propagation tasks to complete
        waitForPropagationTasksToCompleteDelayed(columnGuid, TASK_TYPE_ADD_PROP);
        waitForPropagationTasksToComplete(tableGuid, TASK_TYPE_ADD_PROP);

        // Step 2: Save original ES document state
        Map<String, Object> originalESDoc = getESDoc(columnGuid);
        LOG.info("Saved original ES document state for table GUID: {}", columnGuid);

        // Step 3: Corrupt ES document with rubbish values for classification attributes
        // Update ES document with corrupted values
        updateESDocByGuid(columnGuid, getCorruptedValuesDeNormMap());
        LOG.info("Corrupted ES document attributes for table GUID: {}", columnGuid);

        // Step 4: Call repair classifications mappings API
        repairClassificationsMappings(Arrays.asList(columnGuid));
        sleep(SLEEP);

        // Step 5: Verify all original attributes are restored to their original state
        verifyESAttributes(columnGuid, originalESDoc);

        // Corrupt ES document by removing classification attributes
        Map<String, Object> copyOfAttributes = new HashMap<>();
        originalESDoc.keySet().forEach(orig -> copyOfAttributes.put(orig, originalESDoc.get(orig)));
        copyOfAttributes.remove("__classificationsText");
        copyOfAttributes.remove("__classificationNames");
        copyOfAttributes.remove("__propagatedClassificationNames");
        copyOfAttributes.remove("__traitNames");
        copyOfAttributes.remove("__propagatedTraitNames");

        overrideESDocByGuid(columnGuid, copyOfAttributes);

        verifyESHasNot(columnGuid, "__classificationsText", "__classificationNames",
                "__propagatedClassificationNames", "__traitNames", "__propagatedTraitNames");

        // Call repair classifications mappings API
        repairClassificationsMappings(Arrays.asList(columnGuid));
        sleep(SLEEP);

        // Verify all original attributes are restored to their original state
        verifyESAttributes(columnGuid, originalESDoc);

        LOG.info("manyDirectManyPropagatedTags test completed successfully");
    }

    public void manyAssetsOneDirectTags() throws Exception {
        LOG.info("Running manyAssetsOneDirectTags test");


        List<AtlasEntity> entities = new ArrayList<>();
        for (int i = 0; i < 100 ; i++) {
            AtlasEntity table = getAtlasEntity(TYPE_TABLE, "table_" + i);
            table.setClassifications(Arrays.asList(new AtlasClassification(tagTypeNames.get(0))));
            entities.add(table);
        }

        EntityMutationResponse response = createEntitiesBulk(entities);
        sleep(SLEEP);

        List<String> entitiesGuids = response.getGuidAssignments().values().stream().collect(Collectors.toList());


        for (String entityGuid : entitiesGuids) {
            // Update ES document with corrupted values
            updateESDocByGuid(entityGuid, getCorruptedValuesDeNormMap());
        }
        sleep(SLEEP);

        repairClassificationsMappings(entitiesGuids);
        sleep(SLEEP);

        for (String entityGuid : entitiesGuids) {
            Map<String, Object> originalESDoc = getESDoc(entityGuid);
            verifyESAttributes(entityGuid, originalESDoc);
        }


        for (String entityGuid : entitiesGuids) {
            Map<String, Object> originalESDoc = getESDoc(entityGuid);

            Map<String, Object> copyOfAttributes = new HashMap<>();
            originalESDoc.keySet().forEach(orig -> copyOfAttributes.put(orig, originalESDoc.get(orig)));
            copyOfAttributes.remove("__classificationsText");
            copyOfAttributes.remove("__classificationNames");
            copyOfAttributes.remove("__propagatedClassificationNames");
            copyOfAttributes.remove("__traitNames");
            copyOfAttributes.remove("__propagatedTraitNames");

            overrideESDocByGuid(entityGuid, copyOfAttributes);
        }
        sleep(SLEEP);

        for (String entityGuid : entitiesGuids) {
            verifyESHasNot(entityGuid, "__classificationsText", "__classificationNames",
                    "__propagatedClassificationNames", "__traitNames", "__propagatedTraitNames");
        }

        repairClassificationsMappings(entitiesGuids);
        sleep(SLEEP);
        for (String entityGuid : entitiesGuids) {
            Map<String, Object> originalESDoc = getESDoc(entityGuid);
            verifyESAttributes(entityGuid, originalESDoc);
        }

        LOG.info("manyAssetsOneDirectTags test completed successfully");
    }

    private Map<String, Object> getCorruptedValuesDeNormMap() {
        Map<String, Object> corruptedAttributes = new HashMap<>();
        corruptedAttributes.put("__classificationsText", "rubbish_classification_text");
        corruptedAttributes.put("__classificationNames", "rubbish_classification_name");
        corruptedAttributes.put("__propagatedClassificationNames", "rubbish_propagated_name");
        corruptedAttributes.put("__traitNames", Arrays.asList("rubbish_trait1", "rubbish_trait2"));
        corruptedAttributes.put("__propagatedTraitNames", Arrays.asList("rubbish_prop_trait1", "rubbish_prop_trait2"));

        return corruptedAttributes;
    }
}
