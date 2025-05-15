package com.tests.main.sanity.bulk;

import com.tests.main.Test;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.TestUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.tests.main.utils.TestUtil.*;
import static org.junit.Assert.*;

public class SanityComplexTypeAttributesMutations implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(SanityComplexTypeAttributesMutations.class);

    public static String TYPE_COMPLEX_TEST = "ComplexTest0";
    public static String ATTR_POPULARITY_INSIGHTS = "popularityInsightsTest";
    public static String ATTR_ATLAS_SERVER = "atlasServerTest";
    public static String ATTR_TAG = "tagAttributeTest";
    public static String STRUCT_TYPE_POPULARITY_INSIGHTS = "PopularityInsights";
    public static String TYPE_ATLAS_SERVER = "AtlasServer";
    public static String STRUCT_TYPE_SOURCE_TAG_ATTACHMENT = "SourceTagAttachment";

    public static void main(String[] args) throws Exception {
        try {
            new SanityComplexTypeAttributesMutations().run();
            //TestRunner.runTests(SanityComplexTypeAttributesMutations.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running SanityComplexTypeAttributesMutations tests");

        long start = System.currentTimeMillis();
        try {
            createTypeDefs();

            structTypeAttribute(); // ComplexTest.popularityInsights
            tagAttributeTest(); // ComplexTest.tagAttributeTest

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running SanityComplexTypeAttributesMutations tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private void createTypeDefs() throws Exception {

        // Create entity definition for ComplexTest
        AtlasEntityDef complexTestDef = new AtlasEntityDef();
        complexTestDef.setName(TYPE_COMPLEX_TEST);
        complexTestDef.setDescription("Test entity for complex type attributes");
        complexTestDef.setServiceType("test");
        complexTestDef.setTypeVersion("1.0");
        complexTestDef.setSuperTypes(Collections.singleton("Asset"));
        
        List<AtlasAttributeDef> complexTestAttributes = new ArrayList<>();
        
        // Add PopularityInsights struct attribute
        AtlasAttributeDef popularityInsightsAttr = new AtlasAttributeDef();
        popularityInsightsAttr.setName(ATTR_POPULARITY_INSIGHTS);
        popularityInsightsAttr.setTypeName(STRUCT_TYPE_POPULARITY_INSIGHTS);
        popularityInsightsAttr.setIsOptional(true);
        popularityInsightsAttr.setIsUnique(false);
        complexTestAttributes.add(popularityInsightsAttr);
        
        // Add AtlasServer entity attribute
        AtlasAttributeDef atlasServerAttr = new AtlasAttributeDef();
        atlasServerAttr.setName(ATTR_ATLAS_SERVER);
        atlasServerAttr.setTypeName(TYPE_ATLAS_SERVER);
        atlasServerAttr.setIsOptional(true);
        atlasServerAttr.setIsUnique(false);
        complexTestAttributes.add(atlasServerAttr);

        // Add SourceTagAttachment struct attribute
        AtlasAttributeDef tagAttr = new AtlasAttributeDef();
        tagAttr.setName(ATTR_TAG);
        tagAttr.setTypeName(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT);
        tagAttr.setIsOptional(true);
        tagAttr.setIsUnique(false);
        complexTestAttributes.add(tagAttr);
        
        complexTestDef.setAttributeDefs(complexTestAttributes);
        
        // Create type definitions
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setEntityDefs(Collections.singletonList(complexTestDef));
        
        // Create types in Atlas
        TestUtil.createTypeDefs(typesDef);
    }

    @Test
    private static void structTypeAttribute() throws Exception {
        LOG.info(">> structTypeAttribute");

        /*
         * ComplexTest.popularityInsights ==> PopularityInsights
         * PopularityInsights has attributes:
         * - recordUser (string)
         * - recordComputeCostUnit (SourceCostUnitType) with values: Credits, bytes, slot-ms
         *
         * Update ComplexTest
         * 1. Set struct attribute with one field (recordUser)
         * 2. Update struct attribute with both fields (recordUser and recordComputeCostUnit)
         * 3. Update struct attribute with different field values
         * 4. Remove struct attribute
         * 5. Set struct attribute again with both fields
         * */

        AtlasEntity complexTest = getAtlasEntity(TYPE_COMPLEX_TEST, "complextest_0");
        String complexTestGuid = createEntity(complexTest).getGuidAssignments().values().iterator().next();

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        // 1. Set struct attribute with one field (recordUser)
        AtlasStruct popularityInsights = new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_0"));
        complexTest.setAttribute(ATTR_POPULARITY_INSIGHTS, popularityInsights);
        createEntity(complexTest);

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        assertNotNull(complexTest.getAttribute(ATTR_POPULARITY_INSIGHTS));
        Map<String, Object> structMap = (Map<String, Object>) complexTest.getAttribute(ATTR_POPULARITY_INSIGHTS);
        assertEquals(STRUCT_TYPE_POPULARITY_INSIGHTS, structMap.get("typeName"));
        Map<String, Object> attributes = (Map<String, Object>) structMap.get("attributes");
        assertEquals("user_0", attributes.get("recordUser"));
        assertNull(attributes.get("recordComputeCostUnit"));

        // 2. Update struct attribute with both fields
        popularityInsights = new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, 
            mapOf("recordUser", "user_1", "recordComputeCostUnit", "Credits"));
        complexTest.setAttribute(ATTR_POPULARITY_INSIGHTS, popularityInsights);
        createEntity(complexTest);

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        assertNotNull(complexTest.getAttribute(ATTR_POPULARITY_INSIGHTS));
        structMap = (Map<String, Object>) complexTest.getAttribute(ATTR_POPULARITY_INSIGHTS);
        assertEquals(STRUCT_TYPE_POPULARITY_INSIGHTS, structMap.get("typeName"));
        attributes = (Map<String, Object>) structMap.get("attributes");
        assertEquals("user_1", attributes.get("recordUser"));
        assertEquals("Credits", attributes.get("recordComputeCostUnit"));

        // 3. Update struct attribute with different field values
        popularityInsights = new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, 
            mapOf("recordUser", "user_2", "recordComputeCostUnit", "bytes"));
        complexTest.setAttribute(ATTR_POPULARITY_INSIGHTS, popularityInsights);
        createEntity(complexTest);

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        assertNotNull(complexTest.getAttribute(ATTR_POPULARITY_INSIGHTS));
        structMap = (Map<String, Object>) complexTest.getAttribute(ATTR_POPULARITY_INSIGHTS);
        assertEquals(STRUCT_TYPE_POPULARITY_INSIGHTS, structMap.get("typeName"));
        attributes = (Map<String, Object>) structMap.get("attributes");
        assertEquals("user_2", attributes.get("recordUser"));
        assertEquals("bytes", attributes.get("recordComputeCostUnit"));

        // 4. Remove struct attribute
        complexTest.setAttribute(ATTR_POPULARITY_INSIGHTS, null);
        createEntity(complexTest);

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        assertNull(complexTest.getAttribute(ATTR_POPULARITY_INSIGHTS));

        // 5. Set struct attribute again with both fields
        popularityInsights = new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, 
            mapOf("recordUser", "user_3", "recordComputeCostUnit", "slot-ms"));
        complexTest.setAttribute(ATTR_POPULARITY_INSIGHTS, popularityInsights);
        createEntity(complexTest);

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        assertNotNull(complexTest.getAttribute(ATTR_POPULARITY_INSIGHTS));
        structMap = (Map<String, Object>) complexTest.getAttribute(ATTR_POPULARITY_INSIGHTS);
        assertEquals(STRUCT_TYPE_POPULARITY_INSIGHTS, structMap.get("typeName"));
        attributes = (Map<String, Object>) structMap.get("attributes");
        assertEquals("user_3", attributes.get("recordUser"));
        assertEquals("slot-ms", attributes.get("recordComputeCostUnit"));

        LOG.info("<< structTypeAttribute");
    }

    @Test
    private static void tagAttributeTest() throws Exception {
        LOG.info(">> tagAttributeTest");

        /*
         * ComplexTest.tagAttributeTest ==> SourceTagAttachment
         * SourceTagAttachment.sourceTagValue ==> array<SourceTagAttachmentValue>
         *
         * Update ComplexTest
         * 1. Set tag attribute with one value in sourceTagValue array
         * 2. Add two more values in sourceTagValue array
         * 3. Remove one value from sourceTagValue array
         * 4. Add one + Remove one value in sourceTagValue array
         * 5. Remove all values from sourceTagValue array
         * 6. Add all 3 values back to sourceTagValue array
         * */

        AtlasEntity complexTest = getAtlasEntity(TYPE_COMPLEX_TEST, "complextest_0");
        String complexTestGuid = createEntity(complexTest).getGuidAssignments().values().iterator().next();

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        // 1. Set tag attribute with one value in sourceTagValue array
        Map<String, Object> tagAttributes = new HashMap<>();
        tagAttributes.put("sourceTagName", "test_tag");
        tagAttributes.put("sourceTagQualifiedName", "test.tag");
        tagAttributes.put("sourceTagGuid", "tag_guid_1");
        tagAttributes.put("sourceTagConnectorName", "test_connector");
        tagAttributes.put("isSourceTagSynced", true);
        tagAttributes.put("sourceTagSyncTimestamp", System.currentTimeMillis());
        tagAttributes.put("sourceTagType", "custom");

        List<Map<String, Object>> tagValues = new ArrayList<>();
        Map<String, Object> tagValue1 = new HashMap<>();
        tagValue1.put("tagAttachmentKey", "has_pii");
        tagValue1.put("tagAttachmentValue", "true");
        tagValues.add(tagValue1);

        tagAttributes.put("sourceTagValue", tagValues);

        AtlasStruct tagStruct = new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes);
        complexTest.setAttribute(ATTR_TAG, tagStruct);
        createEntity(complexTest);

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        assertNotNull(complexTest.getAttribute(ATTR_TAG));
        Map<String, Object> tagMap = (Map<String, Object>) complexTest.getAttribute(ATTR_TAG);
        Map<String, Object> attributes = (Map<String, Object>) tagMap.get("attributes");
        assertEquals("test_tag", attributes.get("sourceTagName"));
        assertEquals("test.tag", attributes.get("sourceTagQualifiedName"));
        assertEquals("tag_guid_1", attributes.get("sourceTagGuid"));
        assertEquals("test_connector", attributes.get("sourceTagConnectorName"));
        assertEquals(true, attributes.get("isSourceTagSynced"));
        assertNotNull(attributes.get("sourceTagSyncTimestamp"));
        assertEquals("custom", attributes.get("sourceTagType"));

        List<Map<String, Object>> sourceTagValues = (List<Map<String, Object>>) attributes.get("sourceTagValue");
        assertEquals(1, sourceTagValues.size());
        assertEquals("has_pii", sourceTagValues.get(0).get("tagAttachmentKey"));
        assertEquals("true", sourceTagValues.get(0).get("tagAttachmentValue"));

        // 2. Add two more values in sourceTagValue array
        tagValues = new ArrayList<>();
        tagValue1 = new HashMap<>();
        tagValue1.put("tagAttachmentKey", "has_pii");
        tagValue1.put("tagAttachmentValue", "true");
        tagValues.add(tagValue1);

        Map<String, Object> tagValue2 = new HashMap<>();
        tagValue2.put("tagAttachmentKey", "type_pii");
        tagValue2.put("tagAttachmentValue", "email");
        tagValues.add(tagValue2);

        Map<String, Object> tagValue3 = new HashMap<>();
        tagValue3.put("tagAttachmentKey", "sensitivity");
        tagValue3.put("tagAttachmentValue", "high");
        tagValues.add(tagValue3);

        tagAttributes.put("sourceTagValue", tagValues);
        tagStruct = new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes);
        complexTest.setAttribute(ATTR_TAG, tagStruct);
        createEntity(complexTest);

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        tagMap = (Map<String, Object>) complexTest.getAttribute(ATTR_TAG);
        attributes = (Map<String, Object>) tagMap.get("attributes");
        sourceTagValues = (List<Map<String, Object>>) attributes.get("sourceTagValue");
        assertEquals(3, sourceTagValues.size());
        
        Map<String, String> tagValueMap = new HashMap<>();
        for (Map<String, Object> value : sourceTagValues) {
            tagValueMap.put((String)value.get("tagAttachmentKey"), (String)value.get("tagAttachmentValue"));
        }
        assertEquals("true", tagValueMap.get("has_pii"));
        assertEquals("email", tagValueMap.get("type_pii"));
        assertEquals("high", tagValueMap.get("sensitivity"));

        // 3. Remove one value from sourceTagValue array
        tagValues = new ArrayList<>();
        tagValue1 = new HashMap<>();
        tagValue1.put("tagAttachmentKey", "has_pii");
        tagValue1.put("tagAttachmentValue", "true");
        tagValues.add(tagValue1);

        tagValue3 = new HashMap<>();
        tagValue3.put("tagAttachmentKey", "sensitivity");
        tagValue3.put("tagAttachmentValue", "high");
        tagValues.add(tagValue3);

        tagAttributes.put("sourceTagValue", tagValues);
        tagStruct = new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes);
        complexTest.setAttribute(ATTR_TAG, tagStruct);
        createEntity(complexTest);

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        tagMap = (Map<String, Object>) complexTest.getAttribute(ATTR_TAG);
        attributes = (Map<String, Object>) tagMap.get("attributes");
        sourceTagValues = (List<Map<String, Object>>) attributes.get("sourceTagValue");
        assertEquals(2, sourceTagValues.size());
        
        tagValueMap = new HashMap<>();
        for (Map<String, Object> value : sourceTagValues) {
            tagValueMap.put((String)value.get("tagAttachmentKey"), (String)value.get("tagAttachmentValue"));
        }
        assertEquals("true", tagValueMap.get("has_pii"));
        assertEquals("high", tagValueMap.get("sensitivity"));

        // 4. Add one + Remove one value in sourceTagValue array
        tagValues = new ArrayList<>();
        tagValue2 = new HashMap<>();
        tagValue2.put("tagAttachmentKey", "type_pii");
        tagValue2.put("tagAttachmentValue", "email");
        tagValues.add(tagValue2);

        tagValue3 = new HashMap<>();
        tagValue3.put("tagAttachmentKey", "sensitivity");
        tagValue3.put("tagAttachmentValue", "high");
        tagValues.add(tagValue3);

        tagAttributes.put("sourceTagValue", tagValues);
        tagStruct = new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes);
        complexTest.setAttribute(ATTR_TAG, tagStruct);
        createEntity(complexTest);

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        tagMap = (Map<String, Object>) complexTest.getAttribute(ATTR_TAG);
        attributes = (Map<String, Object>) tagMap.get("attributes");
        sourceTagValues = (List<Map<String, Object>>) attributes.get("sourceTagValue");
        assertEquals(2, sourceTagValues.size());
        
        tagValueMap = new HashMap<>();
        for (Map<String, Object> value : sourceTagValues) {
            tagValueMap.put((String)value.get("tagAttachmentKey"), (String)value.get("tagAttachmentValue"));
        }
        assertEquals("email", tagValueMap.get("type_pii"));
        assertEquals("high", tagValueMap.get("sensitivity"));

        // 5. Remove all values from sourceTagValue array
        tagValues = new ArrayList<>();
        tagAttributes.put("sourceTagValue", tagValues);
        tagStruct = new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes);
        complexTest.setAttribute(ATTR_TAG, tagStruct);
        createEntity(complexTest);

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        tagMap = (Map<String, Object>) complexTest.getAttribute(ATTR_TAG);
        attributes = (Map<String, Object>) tagMap.get("attributes");
        sourceTagValues = (List<Map<String, Object>>) attributes.get("sourceTagValue");
        assertEquals(0, sourceTagValues.size());

        // 6. Add all 3 values back to sourceTagValue array
        tagValues = new ArrayList<>();
        tagValue1 = new HashMap<>();
        tagValue1.put("tagAttachmentKey", "has_pii");
        tagValue1.put("tagAttachmentValue", "true");
        tagValues.add(tagValue1);

        tagValue2 = new HashMap<>();
        tagValue2.put("tagAttachmentKey", "type_pii");
        tagValue2.put("tagAttachmentValue", "email");
        tagValues.add(tagValue2);

        tagValue3 = new HashMap<>();
        tagValue3.put("tagAttachmentKey", "sensitivity");
        tagValue3.put("tagAttachmentValue", "high");
        tagValues.add(tagValue3);

        tagAttributes.put("sourceTagValue", tagValues);
        tagStruct = new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes);
        complexTest.setAttribute(ATTR_TAG, tagStruct);
        createEntity(complexTest);

        sleep(2);
        complexTest = getEntity(complexTestGuid);

        tagMap = (Map<String, Object>) complexTest.getAttribute(ATTR_TAG);
        attributes = (Map<String, Object>) tagMap.get("attributes");
        sourceTagValues = (List<Map<String, Object>>) attributes.get("sourceTagValue");
        assertEquals(3, sourceTagValues.size());
        
        tagValueMap = new HashMap<>();
        for (Map<String, Object> value : sourceTagValues) {
            tagValueMap.put((String)value.get("tagAttachmentKey"), (String)value.get("tagAttachmentValue"));
        }
        assertEquals("true", tagValueMap.get("has_pii"));
        assertEquals("email", tagValueMap.get("type_pii"));
        assertEquals("high", tagValueMap.get("sensitivity"));

        LOG.info("<< tagAttributeTest");
    }
} 