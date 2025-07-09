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
import java.util.stream.Collectors;

import static com.tests.main.utils.TestUtil.*;
import static org.junit.Assert.*;

public class SanityArrayTypeAttributesMutations implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(SanityArrayTypeAttributesMutations.class);

    private static long SLEEP = 1000;

    public static String TYPE_ARRAY_TEST = "ArrayTest";

    public static String ATTR_OWNER_USERS = "ownerUsersTest";
    public static String ATTR_PROJECT_HIERARCHY = "projectHierarchyTest";
    public static String ATTR_REPLICATED_FROM = "replicatedFromTest";
    public static String ATTR_USER_RECORD_LIST = "sourceReadRecentUserRecordListTest";
    public static String ATTR_TAGS = "tagsTest";
    public static String ATTR_TAG_ATTACHMENTS = "tagAttachmentsTest";

    public static String STRUCT_TYPE_POPULARITY_INSIGHTS = "PopularityInsights";
    public static String STRUCT_TYPE_SOURCE_TAG_ATTACHMENT = "SourceTagAttachment";

    public static String ATTR_ENUM_ARRAY = "enumArrayTest";

    public static void main(String[] args) throws Exception {
        try {
            new SanityArrayTypeAttributesMutations().run();
            //TestRunner.runTests(SanityArrayTypeAttributesMutations.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running SanityArrayTypeAttributesMutations tests");

        long start = System.currentTimeMillis();
        try {
            createTypeDefs();
            arrayOfStrings(); // ComplexTest.stringArrayTest
            arrayOfEnums(); // ComplexTest.enumArrayTest
            arrayOfMaps(); // TableauWorkbook.projectHierarchy
            arrayOfStructs(); // Table.sourceReadRecentUserRecordList

            //arrayOfTagAttachments();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running SanityArrayTypeAttributesMutations tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private void createTypeDefs() throws Exception {
        AtlasEntityDef complexTestDef = new AtlasEntityDef();
        complexTestDef.setName(TYPE_ARRAY_TEST);
        complexTestDef.setDescription("Test entity for array type attributes");
        complexTestDef.setServiceType("test");
        complexTestDef.setTypeVersion("1.0");
        complexTestDef.setSuperTypes(Collections.singleton("Asset"));
        
        List<AtlasAttributeDef> arrayTestAttributes = new ArrayList<>();
        
        // Add string array attribute
        AtlasAttributeDef stringArrayAttr = new AtlasAttributeDef();
        stringArrayAttr.setName(ATTR_OWNER_USERS);
        stringArrayAttr.setTypeName("array<string>");
        stringArrayAttr.setIsOptional(true);
        stringArrayAttr.setIsUnique(false);
        arrayTestAttributes.add(stringArrayAttr);
        
        // Add enum array attribute
        AtlasAttributeDef enumArrayAttr = new AtlasAttributeDef();
        enumArrayAttr.setName(ATTR_ENUM_ARRAY);
        enumArrayAttr.setTypeName("array<SourceCostUnitType>");
        enumArrayAttr.setIsOptional(true);
        enumArrayAttr.setIsUnique(false);
        arrayTestAttributes.add(enumArrayAttr);

        // Map array attribute
        AtlasAttributeDef mapArrayAttr = new AtlasAttributeDef();
        mapArrayAttr.setName(ATTR_PROJECT_HIERARCHY);
        mapArrayAttr.setTypeName("array<map<string,string>>");
        mapArrayAttr.setIsOptional(true);
        mapArrayAttr.setIsUnique(false);
        mapArrayAttr.setDescription("Array of map attribute for testing");
        arrayTestAttributes.add(mapArrayAttr);

        // Server array attribute
        AtlasAttributeDef serverArrayAttr = new AtlasAttributeDef();
        serverArrayAttr.setName(ATTR_REPLICATED_FROM);
        serverArrayAttr.setTypeName("array<AtlasServer>");
        serverArrayAttr.setIsOptional(true);
        serverArrayAttr.setIsUnique(false);
        serverArrayAttr.setDescription("Array of server attribute for testing");
        arrayTestAttributes.add(serverArrayAttr);

        // Struct array attribute
        AtlasAttributeDef structArrayAttr = new AtlasAttributeDef();
        structArrayAttr.setName(ATTR_USER_RECORD_LIST);
        structArrayAttr.setTypeName("array<PopularityInsights>");
        structArrayAttr.setIsOptional(true);
        structArrayAttr.setIsUnique(false);
        structArrayAttr.setDescription("Array of struct attribute for testing");
        arrayTestAttributes.add(structArrayAttr);

        // Tag array attribute
        AtlasAttributeDef tagArrayAttr = new AtlasAttributeDef();
        tagArrayAttr.setName(ATTR_TAGS);
        tagArrayAttr.setTypeName("array<SourceTagAttachmentValue>");
        tagArrayAttr.setIsOptional(true);
        tagArrayAttr.setIsUnique(false);
        tagArrayAttr.setDescription("Array of tag attribute for testing");
        arrayTestAttributes.add(tagArrayAttr);

        // SourceTagAttachment array attribute
        AtlasAttributeDef tagAttachmentsArrayAttr = new AtlasAttributeDef();
        tagAttachmentsArrayAttr.setName(ATTR_TAG_ATTACHMENTS);
        tagAttachmentsArrayAttr.setTypeName("array<SourceTagAttachment>");
        tagAttachmentsArrayAttr.setIsOptional(true);
        tagAttachmentsArrayAttr.setIsUnique(false);
        tagAttachmentsArrayAttr.setDescription("Array of SourceTagAttachment struct attribute for testing");
        arrayTestAttributes.add(tagAttachmentsArrayAttr);

        complexTestDef.setAttributeDefs(arrayTestAttributes);
        
        // Create type definitions
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setEntityDefs(Collections.singletonList(complexTestDef));
        try {
            // Create types in Atlas
            TestUtil.createTypeDefs(typesDef);
        } catch (Exception e) {
            LOG.warn(e.getMessage());
        }
    }

    @Test
    private static void arrayOfStrings() throws Exception {
        LOG.info(">> arrayOfStrings");

        /*
         * Table.ownerUsers ==> array<string>
         *
         * Update Table
         * 1. Add one value in array
         * 2. Add two more values in array
         * 3. Remove one value from array
         * 4. Add one + Remove one value
         * 5. Remove all values from array
         * 6. Add all 3 values back to array
         * */

        AtlasEntity table = getAtlasEntity(TYPE_ARRAY_TEST, "table_0");
        String tableGuid = createEntity(table).getGuidAssignments().values().iterator().next();

        sleep(SLEEP);
        table = getEntity(tableGuid);

        // 1. Add one value in array
        table.setAttribute(ATTR_OWNER_USERS, Collections.singleton("user_1"));
        createEntity(table);

        sleep(SLEEP);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_OWNER_USERS));
        List<String> ownerUsers = (List<String>) table.getAttribute(ATTR_OWNER_USERS);
        assertEquals(1, ownerUsers.size());
        assertEquals("user_1", ownerUsers.get(0));
        verifyESAttributes(tableGuid, mapOf(ATTR_OWNER_USERS, ownerUsers));

        // 2. Add two more values in array
        ownerUsers = new ArrayList<>(3);
        ownerUsers.add("user_0");
        ownerUsers.add("user_1");
        ownerUsers.add("user_2");
        table.setAttribute(ATTR_OWNER_USERS, ownerUsers);
        createEntity(table);

        sleep(SLEEP);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_OWNER_USERS));
        ownerUsers = (List<String>) table.getAttribute(ATTR_OWNER_USERS);
        assertEquals(3, ownerUsers.size());
        assertTrue(ownerUsers.contains("user_0"));
        assertTrue(ownerUsers.contains("user_1"));
        assertTrue(ownerUsers.contains("user_2"));
        verifyESAttributes(tableGuid, mapOf(ATTR_OWNER_USERS, ownerUsers));

        // 3. Remove one value from array
        ownerUsers = new ArrayList<>(2);
        ownerUsers.add("user_0");
        ownerUsers.add("user_2");
        table.setAttribute(ATTR_OWNER_USERS, ownerUsers);
        createEntity(table);

        sleep(SLEEP);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_OWNER_USERS));
        ownerUsers = (List<String>) table.getAttribute(ATTR_OWNER_USERS);
        assertEquals(2, ownerUsers.size());
        assertTrue(ownerUsers.contains("user_0"));
        assertTrue(ownerUsers.contains("user_2"));
        verifyESAttributes(tableGuid, mapOf(ATTR_OWNER_USERS, ownerUsers));

        // 4. Add one + Remove one value
        ownerUsers = new ArrayList<>(2);
        ownerUsers.add("user_1");
        ownerUsers.add("user_2");
        table.setAttribute(ATTR_OWNER_USERS, ownerUsers);
        createEntity(table);

        sleep(SLEEP);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_OWNER_USERS));
        ownerUsers = (List<String>) table.getAttribute(ATTR_OWNER_USERS);
        assertEquals(2, ownerUsers.size());
        assertTrue(ownerUsers.contains("user_1"));
        assertTrue(ownerUsers.contains("user_2"));
        verifyESAttributes(tableGuid, mapOf(ATTR_OWNER_USERS, ownerUsers));

        // 5. Remove all values from array
        table.setAttribute(ATTR_OWNER_USERS, null);
        createEntity(table);

        sleep(SLEEP);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_OWNER_USERS));
        assertEquals(0, ((List) table.getAttribute(ATTR_OWNER_USERS)).size());
        verifyESAttributes(tableGuid, mapOf(ATTR_OWNER_USERS, null));

        // 6. Add all 3 values back to array
        ownerUsers = new ArrayList<>(3);
        ownerUsers.add("user_0");
        ownerUsers.add("user_1");
        ownerUsers.add("user_2");
        table.setAttribute(ATTR_OWNER_USERS, ownerUsers);
        createEntity(table);

        sleep(SLEEP);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_OWNER_USERS));
        ownerUsers = (List<String>) table.getAttribute(ATTR_OWNER_USERS);
        assertEquals(3, ownerUsers.size());
        assertTrue(ownerUsers.contains("user_0"));
        assertTrue(ownerUsers.contains("user_1"));
        assertTrue(ownerUsers.contains("user_2"));
        verifyESAttributes(tableGuid, mapOf(ATTR_OWNER_USERS, ownerUsers));

        LOG.info("<< arrayOfStrings");
    }

    @Test
    private static void arrayOfMaps() throws Exception {
        LOG.info(">> arrayOfMaps");

        /*
         * TableauWorkbook.projectHierarchy ==> array<map<string,string>>
         *
         * Update TableauWorkbook
         * 1. Add one value in array
         * 2. Add two more values in array
         * 3. Remove one value from array
         * 4. Add one + Remove one value
         * 5. Remove all values from array
         * 6. Add all 3 values back to array
         * */

        AtlasEntity workbook = getAtlasEntity(TYPE_ARRAY_TEST, "workbook_0");
        String workbookGuid = createEntity(workbook).getGuidAssignments().values().iterator().next();

        sleep(SLEEP);
        workbook = getEntity(workbookGuid);

        // 1. Add one value in array
        workbook.setAttribute(ATTR_PROJECT_HIERARCHY, Collections.singleton(mapOf("key_1", "value_1")));
        createEntity(workbook);

        sleep(SLEEP);
        workbook = getEntity(workbookGuid);

        assertNotNull(workbook.getAttribute(ATTR_PROJECT_HIERARCHY));
        List<Map<String, String>> projectHierarchy = (List<Map<String, String>>) workbook.getAttribute(ATTR_PROJECT_HIERARCHY);
        assertEquals(1, projectHierarchy.size());
        assertEquals("value_1", projectHierarchy.get(0).get("key_1"));

        // 2. Add two more values in array
        projectHierarchy = new ArrayList<>(3);
        projectHierarchy.add(mapOf("key_0", "value_0"));
        projectHierarchy.add(mapOf("key_1", "value_1"));
        projectHierarchy.add(mapOf("key_2", "value_2"));
        workbook.setAttribute(ATTR_PROJECT_HIERARCHY, projectHierarchy);
        createEntity(workbook);

        sleep(SLEEP);
        workbook = getEntity(workbookGuid);

        assertNotNull(workbook.getAttribute(ATTR_PROJECT_HIERARCHY));
        projectHierarchy = (List<Map<String, String>>) workbook.getAttribute(ATTR_PROJECT_HIERARCHY);
        assertEquals(3, projectHierarchy.size());
        assertEquals("value_0", projectHierarchy.get(0).get("key_0"));
        assertEquals("value_1", projectHierarchy.get(1).get("key_1"));
        assertEquals("value_2", projectHierarchy.get(2).get("key_2"));

        // 3. Remove one value from array
        projectHierarchy = new ArrayList<>(2);
        projectHierarchy.add(mapOf("key_0", "value_0"));
        projectHierarchy.add(mapOf("key_2", "value_2"));
        workbook.setAttribute(ATTR_PROJECT_HIERARCHY, projectHierarchy);
        createEntity(workbook);

        sleep(SLEEP);
        workbook = getEntity(workbookGuid);

        assertNotNull(workbook.getAttribute(ATTR_PROJECT_HIERARCHY));
        projectHierarchy = (List<Map<String, String>>) workbook.getAttribute(ATTR_PROJECT_HIERARCHY);
        assertEquals(2, projectHierarchy.size());
        assertEquals("value_0", projectHierarchy.get(0).get("key_0"));
        assertEquals("value_2", projectHierarchy.get(1).get("key_2"));

        // 4. Add one + Remove one value
        projectHierarchy = new ArrayList<>(2);
        projectHierarchy.add(mapOf("key_1", "value_1"));
        projectHierarchy.add(mapOf("key_2", "value_2"));
        workbook.setAttribute(ATTR_PROJECT_HIERARCHY, projectHierarchy);
        createEntity(workbook);

        sleep(SLEEP);
        workbook = getEntity(workbookGuid);

        assertNotNull(workbook.getAttribute(ATTR_PROJECT_HIERARCHY));
        projectHierarchy = (List<Map<String, String>>) workbook.getAttribute(ATTR_PROJECT_HIERARCHY);
        assertEquals(2, projectHierarchy.size());
        assertEquals("value_1", projectHierarchy.get(0).get("key_1"));
        assertEquals("value_2", projectHierarchy.get(1).get("key_2"));

        // 5. Remove all values from array
        workbook.setAttribute(ATTR_PROJECT_HIERARCHY, null);
        createEntity(workbook);

        sleep(SLEEP);
        workbook = getEntity(workbookGuid);

        assertNull(workbook.getAttribute(ATTR_PROJECT_HIERARCHY));

        // 6. Add all 3 values back to array
        projectHierarchy = new ArrayList<>(3);
        projectHierarchy.add(mapOf("key_0", "value_0"));
        projectHierarchy.add(mapOf("key_1", "value_1"));
        projectHierarchy.add(mapOf("key_2", "value_2"));
        workbook.setAttribute(ATTR_PROJECT_HIERARCHY, projectHierarchy);
        createEntity(workbook);

        sleep(SLEEP);
        workbook = getEntity(workbookGuid);

        assertNotNull(workbook.getAttribute(ATTR_PROJECT_HIERARCHY));
        projectHierarchy = (List<Map<String, String>>) workbook.getAttribute(ATTR_PROJECT_HIERARCHY);
        assertEquals(3, projectHierarchy.size());
        assertEquals("value_0", projectHierarchy.get(0).get("key_0"));
        assertEquals("value_1", projectHierarchy.get(1).get("key_1"));
        assertEquals("value_2", projectHierarchy.get(2).get("key_2"));

        LOG.info("<< arrayOfMaps");
    }

    @Test
    private static void arrayOfStructs() throws Exception {
        LOG.info(">> arrayOfStructs");

        /*
         * Table.sourceReadRecentUserRecordList ==> array<PopularityInsights>
         *
         * Update Table
         * 1. Add one value in array
         * 2. Add two more values in array
         * 3. Remove one value from array
         * 4. Add one + Remove one value
         * 5. Remove all values from array
         * 6. Add all 3 values back to array
         * */

        AtlasEntity table = getAtlasEntity(TYPE_ARRAY_TEST, "table_0");
        String tableGuid = createEntity(table).getGuidAssignments().values().iterator().next();

        sleep(SLEEP);
        table = getEntity(tableGuid);

        // 1. Add one value in array
        List<AtlasStruct> structs = new ArrayList<>();
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_0")));
        table.setAttribute(ATTR_USER_RECORD_LIST, structs);
        createEntity(table);

        sleep(SLEEP);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_USER_RECORD_LIST));
        List<Map> structsAsMap = (List<Map>) table.getAttribute(ATTR_USER_RECORD_LIST);
        assertEquals(1, structsAsMap.size());
        assertEquals(STRUCT_TYPE_POPULARITY_INSIGHTS, structsAsMap.get(0).get("typeName"));
        assertEquals("user_0", ((Map) structsAsMap.get(0).get("attributes")).get("recordUser"));

        // 2. Add two more values in array
        structs = new ArrayList<>();
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_0")));
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_1")));
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_2")));
        table.setAttribute(ATTR_USER_RECORD_LIST, structs);
        createEntity(table);

        sleep(SLEEP);
        table = getEntity(tableGuid);

        structsAsMap = (List<Map>) table.getAttribute(ATTR_USER_RECORD_LIST);
        assertEquals(3, structsAsMap.size());
        List<String> users = structsAsMap.stream()
            .map(x -> ((Map) x.get("attributes")).get("recordUser").toString())
            .collect(Collectors.toList());
        assertTrue(users.contains("user_0"));
        assertTrue(users.contains("user_1"));
        assertTrue(users.contains("user_2"));

        // 3. Remove one value from array
        structs = new ArrayList<>();
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_0")));
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_2")));
        table.setAttribute(ATTR_USER_RECORD_LIST, structs);
        createEntity(table);

        sleep(SLEEP);
        table = getEntity(tableGuid);

        structsAsMap = (List<Map>) table.getAttribute(ATTR_USER_RECORD_LIST);
        assertEquals(2, structsAsMap.size());
        users = structsAsMap.stream()
            .map(x -> ((Map) x.get("attributes")).get("recordUser").toString())
            .collect(Collectors.toList());
        assertTrue(users.contains("user_0"));
        assertTrue(users.contains("user_2"));

        // 4. Add one + Remove one value
        structs = new ArrayList<>();
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_1")));
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_2")));
        table.setAttribute(ATTR_USER_RECORD_LIST, structs);
        createEntity(table);

        sleep(SLEEP);
        table = getEntity(tableGuid);

        structsAsMap = (List<Map>) table.getAttribute(ATTR_USER_RECORD_LIST);
        assertEquals(2, structsAsMap.size());
        users = structsAsMap.stream()
            .map(x -> ((Map) x.get("attributes")).get("recordUser").toString())
            .collect(Collectors.toList());
        assertTrue(users.contains("user_1"));
        assertTrue(users.contains("user_2"));

        // 5. Remove all values from array
        structs = new ArrayList<>();
        table.setAttribute(ATTR_USER_RECORD_LIST, structs);
        createEntity(table);

        sleep(SLEEP);
        table = getEntity(tableGuid);

        structsAsMap = (List<Map>) table.getAttribute(ATTR_USER_RECORD_LIST);
        assertEquals(0, structsAsMap.size());

        // 6. Add all 3 values back to array
        structs = new ArrayList<>();
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_0")));
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_1")));
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_2")));
        table.setAttribute(ATTR_USER_RECORD_LIST, structs);
        createEntity(table);

        sleep(SLEEP);
        table = getEntity(tableGuid);

        structsAsMap = (List<Map>) table.getAttribute(ATTR_USER_RECORD_LIST);
        assertEquals(3, structsAsMap.size());
        users = structsAsMap.stream()
            .map(x -> ((Map) x.get("attributes")).get("recordUser").toString())
            .collect(Collectors.toList());
        assertTrue(users.contains("user_0"));
        assertTrue(users.contains("user_1"));
        assertTrue(users.contains("user_2"));

        LOG.info("<< arrayOfStructs");
    }

    @Test
    private static void arrayOfEnums() throws Exception {
        LOG.info(">> arrayOfEnums");

        /*
         * ComplexTest.enumArrayTest ==> array<SourceCostUnitType>
         * SourceCostUnitType values: Credits, bytes, slot-ms
         *
         * Update ComplexTest
         * 1. Add one value in array
         * 2. Add two more values in array
         * 3. Remove one value from array
         * 4. Add one + Remove one value
         * 5. Remove all values from array
         * 6. Add all 3 values back to array
         * */

        AtlasEntity complexTest = getAtlasEntity(TYPE_ARRAY_TEST, "complextest_0");
        String complexTestGuid = createEntity(complexTest).getGuidAssignments().values().iterator().next();

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        // 1. Add one value in array
        complexTest.setAttribute(ATTR_ENUM_ARRAY, Collections.singleton("Credits"));
        createEntity(complexTest);

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        assertNotNull(complexTest.getAttribute(ATTR_ENUM_ARRAY));
        List<String> enumArray = (List<String>) complexTest.getAttribute(ATTR_ENUM_ARRAY);
        assertEquals(1, enumArray.size());
        assertEquals("Credits", enumArray.get(0));
        verifyESAttributes(complexTestGuid, mapOf(ATTR_ENUM_ARRAY, enumArray));

        // 2. Add two more values in array
        enumArray = new ArrayList<>(3);
        enumArray.add("Credits");
        enumArray.add("bytes");
        enumArray.add("slot-ms");
        complexTest.setAttribute(ATTR_ENUM_ARRAY, enumArray);
        createEntity(complexTest);

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        assertNotNull(complexTest.getAttribute(ATTR_ENUM_ARRAY));
        enumArray = (List<String>) complexTest.getAttribute(ATTR_ENUM_ARRAY);
        assertEquals(3, enumArray.size());
        assertEquals("Credits", enumArray.get(0));
        assertEquals("bytes", enumArray.get(1));
        assertEquals("slot-ms", enumArray.get(2));
        verifyESAttributes(complexTestGuid, mapOf(ATTR_ENUM_ARRAY, enumArray));

        // 3. Remove one value from array
        enumArray = new ArrayList<>(2);
        enumArray.add("Credits");
        enumArray.add("slot-ms");
        complexTest.setAttribute(ATTR_ENUM_ARRAY, enumArray);
        createEntity(complexTest);

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        assertNotNull(complexTest.getAttribute(ATTR_ENUM_ARRAY));
        enumArray = (List<String>) complexTest.getAttribute(ATTR_ENUM_ARRAY);
        assertEquals(2, enumArray.size());
        assertEquals("Credits", enumArray.get(0));
        assertEquals("slot-ms", enumArray.get(1));
        verifyESAttributes(complexTestGuid, mapOf(ATTR_ENUM_ARRAY, enumArray));

        // 4. Add one + Remove one value
        enumArray = new ArrayList<>(2);
        enumArray.add("bytes");
        enumArray.add("slot-ms");
        complexTest.setAttribute(ATTR_ENUM_ARRAY, enumArray);
        createEntity(complexTest);

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        assertNotNull(complexTest.getAttribute(ATTR_ENUM_ARRAY));
        enumArray = (List<String>) complexTest.getAttribute(ATTR_ENUM_ARRAY);
        assertEquals(2, enumArray.size());
        assertEquals("bytes", enumArray.get(0));
        assertEquals("slot-ms", enumArray.get(1));
        verifyESAttributes(complexTestGuid, mapOf(ATTR_ENUM_ARRAY, enumArray));

        // 5. Remove all values from array
        complexTest.setAttribute(ATTR_ENUM_ARRAY, null);
        createEntity(complexTest);

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        assertNotNull(complexTest.getAttribute(ATTR_ENUM_ARRAY));
        assertEquals(0, ((List) complexTest.getAttribute(ATTR_ENUM_ARRAY)).size());

        // 6. Add all 3 values back to array
        enumArray = new ArrayList<>(3);
        enumArray.add("Credits");
        enumArray.add("bytes");
        enumArray.add("slot-ms");
        complexTest.setAttribute(ATTR_ENUM_ARRAY, enumArray);
        createEntity(complexTest);

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        assertNotNull(complexTest.getAttribute(ATTR_ENUM_ARRAY));
        enumArray = (List<String>) complexTest.getAttribute(ATTR_ENUM_ARRAY);
        assertEquals(3, enumArray.size());
        assertTrue(enumArray.contains("Credits"));
        assertTrue(enumArray.contains("bytes"));
        assertTrue(enumArray.contains("slot-ms"));
        verifyESAttributes(complexTestGuid, mapOf(ATTR_ENUM_ARRAY, enumArray));

        LOG.info("<< arrayOfEnums");
    }

    @Test
    private static void arrayOfTagAttachments() throws Exception {
        LOG.info(">> arrayOfTagAttachments");

        /*
         * ComplexTest.tagAttachmentsTest ==> array<SourceTagAttachment>
         * SourceTagAttachment has attributes:
         * - sourceTagName (string)
         * - sourceTagQualifiedName (string)
         * - sourceTagGuid (string)
         * - sourceTagConnectorName (string)
         * - isSourceTagSynced (boolean)
         * - sourceTagSyncTimestamp (long)
         * - sourceTagType (string)
         * - sourceTagValue (array<SourceTagAttachmentValue>)
         *
         * Update ComplexTest
         * 1. Add one tag attachment in array
         * 2. Add two more tag attachments in array
         * 3. Remove one tag attachment from array
         * 4. Add one + Remove one tag attachment
         * 5. Remove all tag attachments from array
         * 6. Add all 3 tag attachments back to array
         * */

        AtlasEntity complexTest = getAtlasEntity(TYPE_ARRAY_TEST, "complextest_0");
        String complexTestGuid = createEntity(complexTest).getGuidAssignments().values().iterator().next();

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        // 1. Add one tag attachment in array
        List<AtlasStruct> tagAttachments = new ArrayList<>();
        Map<String, Object> tagAttributes1 = new HashMap<>();
        tagAttributes1.put("sourceTagName", "test_tag_1");
        tagAttributes1.put("sourceTagQualifiedName", "test.tag.1");
        tagAttributes1.put("sourceTagGuid", "tag_guid_1");
        tagAttributes1.put("sourceTagConnectorName", "test_connector");
        tagAttributes1.put("isSourceTagSynced", true);
        tagAttributes1.put("sourceTagSyncTimestamp", System.currentTimeMillis());
        tagAttributes1.put("sourceTagType", "custom");

        List<Map<String, Object>> tagValues1 = new ArrayList<>();
        Map<String, Object> tagValue1 = new HashMap<>();
        tagValue1.put("tagAttachmentKey", "has_pii");
        tagValue1.put("tagAttachmentValue", "true");
        tagValues1.add(tagValue1);
        tagAttributes1.put("sourceTagValue", tagValues1);

        tagAttachments.add(new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes1));
        complexTest.setAttribute(ATTR_TAG_ATTACHMENTS, tagAttachments);
        createEntity(complexTest);

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        assertNotNull(complexTest.getAttribute(ATTR_TAG_ATTACHMENTS));
        List<Map<String, Object>> tagAttachmentsAsMap = (List<Map<String, Object>>) complexTest.getAttribute(ATTR_TAG_ATTACHMENTS);
        assertEquals(1, tagAttachmentsAsMap.size());
        Map<String, Object> attributes = (Map<String, Object>) tagAttachmentsAsMap.get(0).get("attributes");
        assertEquals("test_tag_1", attributes.get("sourceTagName"));
        assertEquals("test.tag.1", attributes.get("sourceTagQualifiedName"));
        assertEquals("tag_guid_1", attributes.get("sourceTagGuid"));
        assertEquals("test_connector", attributes.get("sourceTagConnectorName"));
        assertEquals(true, attributes.get("isSourceTagSynced"));
        assertNotNull(attributes.get("sourceTagSyncTimestamp"));
        assertEquals("custom", attributes.get("sourceTagType"));

        List<Map<String, Object>> sourceTagValues = (List<Map<String, Object>>) attributes.get("sourceTagValue");
        assertEquals(1, sourceTagValues.size());
        assertEquals("has_pii", ((Map<String, Object>) sourceTagValues.get(0).get("attributes")).get("tagAttachmentKey"));
        assertEquals("true", ((Map<String, Object>) sourceTagValues.get(0).get("attributes")).get("tagAttachmentValue"));

        // 2. Add two more tag attachments in array
        tagAttachments = new ArrayList<>();
        tagAttachments.add(new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes1));

        Map<String, Object> tagAttributes2 = new HashMap<>();
        tagAttributes2.put("sourceTagName", "test_tag_2");
        tagAttributes2.put("sourceTagQualifiedName", "test.tag.2");
        tagAttributes2.put("sourceTagGuid", "tag_guid_2");
        tagAttributes2.put("sourceTagConnectorName", "test_connector");
        tagAttributes2.put("isSourceTagSynced", true);
        tagAttributes2.put("sourceTagSyncTimestamp", System.currentTimeMillis());
        tagAttributes2.put("sourceTagType", "custom");

        List<Map<String, Object>> tagValues2 = new ArrayList<>();
        Map<String, Object> tagValue2 = new HashMap<>();
        tagValue2.put("tagAttachmentKey", "type_pii");
        tagValue2.put("tagAttachmentValue", "email");
        tagValues2.add(tagValue2);
        tagAttributes2.put("sourceTagValue", tagValues2);

        tagAttachments.add(new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes2));

        Map<String, Object> tagAttributes3 = new HashMap<>();
        tagAttributes3.put("sourceTagName", "test_tag_3");
        tagAttributes3.put("sourceTagQualifiedName", "test.tag.3");
        tagAttributes3.put("sourceTagGuid", "tag_guid_3");
        tagAttributes3.put("sourceTagConnectorName", "test_connector");
        tagAttributes3.put("isSourceTagSynced", true);
        tagAttributes3.put("sourceTagSyncTimestamp", System.currentTimeMillis());
        tagAttributes3.put("sourceTagType", "custom");

        List<Map<String, Object>> tagValues3 = new ArrayList<>();
        Map<String, Object> tagValue3 = new HashMap<>();
        tagValue3.put("tagAttachmentKey", "sensitivity");
        tagValue3.put("tagAttachmentValue", "high");
        tagValues3.add(tagValue3);
        tagAttributes3.put("sourceTagValue", tagValues3);

        tagAttachments.add(new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes3));
        complexTest.setAttribute(ATTR_TAG_ATTACHMENTS, tagAttachments);
        createEntity(complexTest);

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        tagAttachmentsAsMap = (List<Map<String, Object>>) complexTest.getAttribute(ATTR_TAG_ATTACHMENTS);
        assertEquals(3, tagAttachmentsAsMap.size());

        Map<String, Map<String, Object>> tagMap = new HashMap<>();
        for (Map<String, Object> tagAttachment : tagAttachmentsAsMap) {
            Map<String, Object> attrs = (Map<String, Object>) tagAttachment.get("attributes");
            tagMap.put((String)attrs.get("sourceTagName"), attrs);
        }

        Map<String, Object> tag1 = tagMap.get("test_tag_1");
        assertEquals("test.tag.1", tag1.get("sourceTagQualifiedName"));
        assertEquals("tag_guid_1", tag1.get("sourceTagGuid"));
        List<Map<String, Object>> values1 = (List<Map<String, Object>>) tag1.get("sourceTagValue");
        assertEquals("has_pii", ((Map<String, Object>) values1.get(0).get("attributes")).get("tagAttachmentKey"));
        assertEquals("true", ((Map<String, Object>) values1.get(0).get("attributes")).get("tagAttachmentValue"));

        Map<String, Object> tag2 = tagMap.get("test_tag_2");
        assertEquals("test.tag.2", tag2.get("sourceTagQualifiedName"));
        assertEquals("tag_guid_2", tag2.get("sourceTagGuid"));
        List<Map<String, Object>> values2 = (List<Map<String, Object>>) tag2.get("sourceTagValue");
        assertEquals("type_pii", ((Map<String, Object>) values2.get(0).get("attributes")).get("tagAttachmentKey"));
        assertEquals("email", ((Map<String, Object>) values2.get(0).get("attributes")).get("tagAttachmentValue"));

        Map<String, Object> tag3 = tagMap.get("test_tag_3");
        assertEquals("test.tag.3", tag3.get("sourceTagQualifiedName"));
        assertEquals("tag_guid_3", tag3.get("sourceTagGuid"));
        List<Map<String, Object>> values3 = (List<Map<String, Object>>) tag3.get("sourceTagValue");
        assertEquals("sensitivity", ((Map<String, Object>) values3.get(0).get("attributes")).get("tagAttachmentKey"));
        assertEquals("high", ((Map<String, Object>) values3.get(0).get("attributes")).get("tagAttachmentValue"));

        // 3. Remove one tag attachment from array
        tagAttachments = new ArrayList<>();
        tagAttachments.add(new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes1));
        tagAttachments.add(new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes3));
        complexTest.setAttribute(ATTR_TAG_ATTACHMENTS, tagAttachments);
        createEntity(complexTest);

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        tagAttachmentsAsMap = (List<Map<String, Object>>) complexTest.getAttribute(ATTR_TAG_ATTACHMENTS);
        assertEquals(2, tagAttachmentsAsMap.size());

        tagMap = new HashMap<>();
        for (Map<String, Object> tagAttachment : tagAttachmentsAsMap) {
            Map<String, Object> attrs = (Map<String, Object>) tagAttachment.get("attributes");
            tagMap.put((String)attrs.get("sourceTagName"), attrs);
        }

        assertTrue(tagMap.containsKey("test_tag_1"));
        assertTrue(tagMap.containsKey("test_tag_3"));
        assertFalse(tagMap.containsKey("test_tag_2"));

        // 4. Add one + Remove one tag attachment
        tagAttachments = new ArrayList<>();
        tagAttachments.add(new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes2));
        tagAttachments.add(new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes3));
        complexTest.setAttribute(ATTR_TAG_ATTACHMENTS, tagAttachments);
        createEntity(complexTest);

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        tagAttachmentsAsMap = (List<Map<String, Object>>) complexTest.getAttribute(ATTR_TAG_ATTACHMENTS);
        assertEquals(2, tagAttachmentsAsMap.size());

        tagMap = new HashMap<>();
        for (Map<String, Object> tagAttachment : tagAttachmentsAsMap) {
            Map<String, Object> attrs = (Map<String, Object>) tagAttachment.get("attributes");
            tagMap.put((String)attrs.get("sourceTagName"), attrs);
        }

        assertTrue(tagMap.containsKey("test_tag_2"));
        assertTrue(tagMap.containsKey("test_tag_3"));
        assertFalse(tagMap.containsKey("test_tag_1"));

        // 5. Remove all tag attachments from array
        tagAttachments = new ArrayList<>();
        complexTest.setAttribute(ATTR_TAG_ATTACHMENTS, tagAttachments);
        createEntity(complexTest);

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        tagAttachmentsAsMap = (List<Map<String, Object>>) complexTest.getAttribute(ATTR_TAG_ATTACHMENTS);
        assertEquals(0, tagAttachmentsAsMap.size());

        // 6. Add all 3 tag attachments back to array
        tagAttachments = new ArrayList<>();
        tagAttachments.add(new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes1));
        tagAttachments.add(new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes2));
        tagAttachments.add(new AtlasStruct(STRUCT_TYPE_SOURCE_TAG_ATTACHMENT, tagAttributes3));
        complexTest.setAttribute(ATTR_TAG_ATTACHMENTS, tagAttachments);
        createEntity(complexTest);

        sleep(SLEEP);
        complexTest = getEntity(complexTestGuid);

        tagAttachmentsAsMap = (List<Map<String, Object>>) complexTest.getAttribute(ATTR_TAG_ATTACHMENTS);
        assertEquals(3, tagAttachmentsAsMap.size());

        tagMap = new HashMap<>();
        for (Map<String, Object> tagAttachment : tagAttachmentsAsMap) {
            Map<String, Object> attrs = (Map<String, Object>) tagAttachment.get("attributes");
            tagMap.put((String)attrs.get("sourceTagName"), attrs);
        }

        assertTrue(tagMap.containsKey("test_tag_1"));
        assertTrue(tagMap.containsKey("test_tag_2"));
        assertTrue(tagMap.containsKey("test_tag_3"));

        LOG.info("<< arrayOfTagAttachments");
    }
} 