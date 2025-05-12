package com.tests.main.sanity;

import com.tests.main.Test;
import com.tests.main.TestRunner;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.TestUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasStructDef.AtlasAttributeDef;
import org.apache.atlas.model.typedef.AtlasEntityDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.tests.main.utils.TestUtil.*;
import static org.junit.Assert.*;

public class SanityMapTypeAttributesMutations implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(SanityMapTypeAttributesMutations.class);

    public static String TYPE_MAP_TEST = "MapTest";
    public static String ATTR_STRING_MAP = "stringMapTest";
    public static String ATTR_LONG_MAP = "longMapTest";
    public static String ATTR_INT_MAP = "intMapTest";
    public static String ATTR_DOUBLE_MAP = "doubleMapTest";
    public static String ATTR_FLOAT_MAP = "floatMapTest";

    public static void main(String[] args) throws Exception {
        try {
            new SanityMapTypeAttributesMutations().run();
            //TestRunner.runTests(SanityMapTypeAttributesMutations.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running SanityMapTypeAttributesMutations tests");

        long start = System.currentTimeMillis();
        try {
            createTypeDefs();
            mapOfStrings(); // MapTest.stringMapTest
            mapOfLongs(); // MapTest.longMapTest
            mapOfInts(); // MapTest.intMapTest
            mapOfDoubles(); // MapTest.doubleMapTest
            mapOfFloats(); // MapTest.floatMapTest
        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running SanityMapTypeAttributesMutations tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private void createTypeDefs() throws Exception {
        // Create entity definition for MapTest
        AtlasEntityDef mapTestDef = new AtlasEntityDef();
        mapTestDef.setName(TYPE_MAP_TEST);
        mapTestDef.setDescription("Test entity for map type attributes");
        mapTestDef.setServiceType("test");
        mapTestDef.setTypeVersion("1.0");
        mapTestDef.setSuperTypes(Collections.singleton("Asset"));
        
        // Add attributes
        List<AtlasAttributeDef> mapTestAttributes = new ArrayList<>();
        
        // String map attribute
        AtlasAttributeDef stringMapAttr = new AtlasAttributeDef();
        stringMapAttr.setName(ATTR_STRING_MAP);
        stringMapAttr.setTypeName("map<string,string>");
        stringMapAttr.setIsOptional(true);
        stringMapAttr.setIsUnique(false);
        stringMapAttr.setDescription("Map of string to string attribute for testing");
        mapTestAttributes.add(stringMapAttr);
        
        // Long map attribute
        AtlasAttributeDef longMapAttr = new AtlasAttributeDef();
        longMapAttr.setName(ATTR_LONG_MAP);
        longMapAttr.setTypeName("map<string,long>");
        longMapAttr.setIsOptional(true);
        longMapAttr.setIsUnique(false);
        longMapAttr.setDescription("Map of string to long attribute for testing");
        mapTestAttributes.add(longMapAttr);

        // Int map attribute
        AtlasAttributeDef intMapAttr = new AtlasAttributeDef();
        intMapAttr.setName(ATTR_INT_MAP);
        intMapAttr.setTypeName("map<string,int>");
        intMapAttr.setIsOptional(true);
        intMapAttr.setIsUnique(false);
        intMapAttr.setDescription("Map of string to int attribute for testing");
        mapTestAttributes.add(intMapAttr);

        // Double map attribute
        AtlasAttributeDef doubleMapAttr = new AtlasAttributeDef();
        doubleMapAttr.setName(ATTR_DOUBLE_MAP);
        doubleMapAttr.setTypeName("map<string,double>");
        doubleMapAttr.setIsOptional(true);
        doubleMapAttr.setIsUnique(false);
        doubleMapAttr.setDescription("Map of string to double attribute for testing");
        mapTestAttributes.add(doubleMapAttr);

        // Float map attribute
        AtlasAttributeDef floatMapAttr = new AtlasAttributeDef();
        floatMapAttr.setName(ATTR_FLOAT_MAP);
        floatMapAttr.setTypeName("map<string,float>");
        floatMapAttr.setIsOptional(true);
        floatMapAttr.setIsUnique(false);
        floatMapAttr.setDescription("Map of string to float attribute for testing");
        mapTestAttributes.add(floatMapAttr);
        
        mapTestDef.setAttributeDefs(mapTestAttributes);
        
        // Create type definition
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setEntityDefs(Collections.singletonList(mapTestDef));
        
        // Create type in Atlas
        TestUtil.createTypeDefs(typesDef);
    }

    @Test
    private static void mapOfStrings() throws Exception {
        LOG.info(">> mapOfStrings");

        /*
         * MapTest.stringMapTest ==> map<string,string>
         *
         * Update MapTest
         * 1. Add one key-value pair
         * 2. Add two more key-value pairs
         * 3. Remove one key-value pair
         * 4. Add one + Remove one key-value pair
         * 5. Remove all key-value pairs
         * 6. Add all 3 key-value pairs back
         * */

        AtlasEntity mapTest = getAtlasEntity(TYPE_MAP_TEST, "maptest_0");
        String mapTestGuid = createEntity(mapTest).getGuidAssignments().values().iterator().next();

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        // 1. Add one key-value pair
        mapTest.setAttribute(ATTR_STRING_MAP, mapOf("key_1", "value_1"));
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_STRING_MAP));
        Map<String, String> stringMap = (Map<String, String>) mapTest.getAttribute(ATTR_STRING_MAP);
        assertEquals(1, stringMap.size());
        assertEquals("value_1", stringMap.get("key_1"));

        // 2. Add two more key-value pairs
        stringMap = new HashMap<>();
        stringMap.put("key_0", "value_0");
        stringMap.put("key_1", "value_1");
        stringMap.put("key_2", "value_2");
        mapTest.setAttribute(ATTR_STRING_MAP, stringMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_STRING_MAP));
        stringMap = (Map<String, String>) mapTest.getAttribute(ATTR_STRING_MAP);
        assertEquals(3, stringMap.size());
        assertEquals("value_0", stringMap.get("key_0"));
        assertEquals("value_1", stringMap.get("key_1"));
        assertEquals("value_2", stringMap.get("key_2"));

        // 3. Remove one key-value pair
        stringMap = new HashMap<>();
        stringMap.put("key_0", "value_0");
        stringMap.put("key_2", "value_2");
        mapTest.setAttribute(ATTR_STRING_MAP, stringMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_STRING_MAP));
        stringMap = (Map<String, String>) mapTest.getAttribute(ATTR_STRING_MAP);
        assertEquals(2, stringMap.size());
        assertEquals("value_0", stringMap.get("key_0"));
        assertEquals("value_2", stringMap.get("key_2"));

        // 4. Add one + Remove one key-value pair
        stringMap = new HashMap<>();
        stringMap.put("key_1", "value_1");
        stringMap.put("key_2", "value_2");
        mapTest.setAttribute(ATTR_STRING_MAP, stringMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_STRING_MAP));
        stringMap = (Map<String, String>) mapTest.getAttribute(ATTR_STRING_MAP);
        assertEquals(2, stringMap.size());
        assertEquals("value_1", stringMap.get("key_1"));
        assertEquals("value_2", stringMap.get("key_2"));

        // 5. Remove all key-value pairs
        mapTest.setAttribute(ATTR_STRING_MAP, null);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNull(mapTest.getAttribute(ATTR_STRING_MAP));

        // 6. Add all 3 key-value pairs back
        stringMap = new HashMap<>();
        stringMap.put("key_0", "value_0");
        stringMap.put("key_1", "value_1");
        stringMap.put("key_2", "value_2");
        mapTest.setAttribute(ATTR_STRING_MAP, stringMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_STRING_MAP));
        stringMap = (Map<String, String>) mapTest.getAttribute(ATTR_STRING_MAP);
        assertEquals(3, stringMap.size());
        assertEquals("value_0", stringMap.get("key_0"));
        assertEquals("value_1", stringMap.get("key_1"));
        assertEquals("value_2", stringMap.get("key_2"));

        LOG.info("<< mapOfStrings");
    }

    @Test
    private static void mapOfLongs() throws Exception {
        LOG.info(">> mapOfLongs");

        /*
         * MapTest.longMapTest ==> map<string,long>
         *
         * Update MapTest
         * 1. Add one key-value pair
         * 2. Add two more key-value pairs
         * 3. Remove one key-value pair
         * 4. Add one + Remove one key-value pair
         * 5. Remove all key-value pairs
         * 6. Add all 3 key-value pairs back
         * */

        AtlasEntity mapTest = getAtlasEntity(TYPE_MAP_TEST, "maptest_0");
        String mapTestGuid = createEntity(mapTest).getGuidAssignments().values().iterator().next();

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        // 1. Add one key-value pair
        Map<String, Long> longMap = new HashMap<>();
        longMap.put("key_1", 1L);
        mapTest.setAttribute(ATTR_LONG_MAP, longMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_LONG_MAP));
        Map<String, Object> rawMap = (Map<String, Object>) mapTest.getAttribute(ATTR_LONG_MAP);
        assertEquals(1, rawMap.size());
        assertEquals(1L, ((Number)rawMap.get("key_1")).longValue());

        // 2. Add two more key-value pairs
        longMap = new HashMap<>();
        longMap.put("key_0", 0L);
        longMap.put("key_1", 1L);
        longMap.put("key_2", 2L);
        mapTest.setAttribute(ATTR_LONG_MAP, longMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_LONG_MAP));
        rawMap = (Map<String, Object>) mapTest.getAttribute(ATTR_LONG_MAP);
        assertEquals(3, rawMap.size());
        assertEquals(0L, ((Number)rawMap.get("key_0")).longValue());
        assertEquals(1L, ((Number)rawMap.get("key_1")).longValue());
        assertEquals(2L, ((Number)rawMap.get("key_2")).longValue());

        // 3. Remove one key-value pair
        longMap = new HashMap<>();
        longMap.put("key_0", 0L);
        longMap.put("key_2", 2L);
        mapTest.setAttribute(ATTR_LONG_MAP, longMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_LONG_MAP));
        rawMap = (Map<String, Object>) mapTest.getAttribute(ATTR_LONG_MAP);
        assertEquals(2, rawMap.size());
        assertEquals(0L, ((Number)rawMap.get("key_0")).longValue());
        assertEquals(2L, ((Number)rawMap.get("key_2")).longValue());

        // 4. Add one + Remove one key-value pair
        longMap = new HashMap<>();
        longMap.put("key_1", 1L);
        longMap.put("key_2", 2L);
        mapTest.setAttribute(ATTR_LONG_MAP, longMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_LONG_MAP));
        rawMap = (Map<String, Object>) mapTest.getAttribute(ATTR_LONG_MAP);
        assertEquals(2, rawMap.size());
        assertEquals(1L, ((Number)rawMap.get("key_1")).longValue());
        assertEquals(2L, ((Number)rawMap.get("key_2")).longValue());

        // 5. Remove all key-value pairs
        mapTest.setAttribute(ATTR_LONG_MAP, null);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNull(mapTest.getAttribute(ATTR_LONG_MAP));

        // 6. Add all 3 key-value pairs back
        longMap = new HashMap<>();
        longMap.put("key_0", 0L);
        longMap.put("key_1", 1L);
        longMap.put("key_2", 2L);
        mapTest.setAttribute(ATTR_LONG_MAP, longMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_LONG_MAP));
        rawMap = (Map<String, Object>) mapTest.getAttribute(ATTR_LONG_MAP);
        assertEquals(3, rawMap.size());
        assertEquals(0L, ((Number)rawMap.get("key_0")).longValue());
        assertEquals(1L, ((Number)rawMap.get("key_1")).longValue());
        assertEquals(2L, ((Number)rawMap.get("key_2")).longValue());

        LOG.info("<< mapOfLongs");
    }

    @Test
    private static void mapOfInts() throws Exception {
        LOG.info(">> mapOfInts");

        /*
         * MapTest.intMapTest ==> map<string,int>
         *
         * Update MapTest
         * 1. Add one key-value pair
         * 2. Add two more key-value pairs
         * 3. Remove one key-value pair
         * 4. Add one + Remove one key-value pair
         * 5. Remove all key-value pairs
         * 6. Add all 3 key-value pairs back
         * */

        AtlasEntity mapTest = getAtlasEntity(TYPE_MAP_TEST, "maptest_0");
        String mapTestGuid = createEntity(mapTest).getGuidAssignments().values().iterator().next();

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        // 1. Add one key-value pair
        Map<String, Integer> intMap = new HashMap<>();
        intMap.put("key_1", 1);
        mapTest.setAttribute(ATTR_INT_MAP, intMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_INT_MAP));
        intMap = (Map<String, Integer>) mapTest.getAttribute(ATTR_INT_MAP);
        assertEquals(1, intMap.size());
        assertEquals(Integer.valueOf(1), intMap.get("key_1"));

        // 2. Add two more key-value pairs
        intMap = new HashMap<>();
        intMap.put("key_0", 0);
        intMap.put("key_1", 1);
        intMap.put("key_2", 2);
        mapTest.setAttribute(ATTR_INT_MAP, intMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_INT_MAP));
        intMap = (Map<String, Integer>) mapTest.getAttribute(ATTR_INT_MAP);
        assertEquals(3, intMap.size());
        assertEquals(Integer.valueOf(0), intMap.get("key_0"));
        assertEquals(Integer.valueOf(1), intMap.get("key_1"));
        assertEquals(Integer.valueOf(2), intMap.get("key_2"));

        // 3. Remove one key-value pair
        intMap = new HashMap<>();
        intMap.put("key_0", 0);
        intMap.put("key_2", 2);
        mapTest.setAttribute(ATTR_INT_MAP, intMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_INT_MAP));
        intMap = (Map<String, Integer>) mapTest.getAttribute(ATTR_INT_MAP);
        assertEquals(2, intMap.size());
        assertEquals(Integer.valueOf(0), intMap.get("key_0"));
        assertEquals(Integer.valueOf(2), intMap.get("key_2"));

        // 4. Add one + Remove one key-value pair
        intMap = new HashMap<>();
        intMap.put("key_1", 1);
        intMap.put("key_2", 2);
        mapTest.setAttribute(ATTR_INT_MAP, intMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_INT_MAP));
        intMap = (Map<String, Integer>) mapTest.getAttribute(ATTR_INT_MAP);
        assertEquals(2, intMap.size());
        assertEquals(Integer.valueOf(1), intMap.get("key_1"));
        assertEquals(Integer.valueOf(2), intMap.get("key_2"));

        // 5. Remove all key-value pairs
        mapTest.setAttribute(ATTR_INT_MAP, null);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNull(mapTest.getAttribute(ATTR_INT_MAP));

        // 6. Add all 3 key-value pairs back
        intMap = new HashMap<>();
        intMap.put("key_0", 0);
        intMap.put("key_1", 1);
        intMap.put("key_2", 2);
        mapTest.setAttribute(ATTR_INT_MAP, intMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_INT_MAP));
        intMap = (Map<String, Integer>) mapTest.getAttribute(ATTR_INT_MAP);
        assertEquals(3, intMap.size());
        assertEquals(Integer.valueOf(0), intMap.get("key_0"));
        assertEquals(Integer.valueOf(1), intMap.get("key_1"));
        assertEquals(Integer.valueOf(2), intMap.get("key_2"));

        LOG.info("<< mapOfInts");
    }

    @Test
    private static void mapOfDoubles() throws Exception {
        LOG.info(">> mapOfDoubles");

        /*
         * MapTest.doubleMapTest ==> map<string,double>
         *
         * Update MapTest
         * 1. Add one key-value pair
         * 2. Add two more key-value pairs
         * 3. Remove one key-value pair
         * 4. Add one + Remove one key-value pair
         * 5. Remove all key-value pairs
         * 6. Add all 3 key-value pairs back
         * */

        AtlasEntity mapTest = getAtlasEntity(TYPE_MAP_TEST, "maptest_0");
        String mapTestGuid = createEntity(mapTest).getGuidAssignments().values().iterator().next();

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        // 1. Add one key-value pair
        Map<String, Double> doubleMap = new HashMap<>();
        doubleMap.put("key_1", 1.1);
        mapTest.setAttribute(ATTR_DOUBLE_MAP, doubleMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_DOUBLE_MAP));
        doubleMap = (Map<String, Double>) mapTest.getAttribute(ATTR_DOUBLE_MAP);
        assertEquals(1, doubleMap.size());
        assertEquals(Double.valueOf(1.1), doubleMap.get("key_1"));

        // 2. Add two more key-value pairs
        doubleMap = new HashMap<>();
        doubleMap.put("key_0", 0.0);
        doubleMap.put("key_1", 1.1);
        doubleMap.put("key_2", 2.2);
        mapTest.setAttribute(ATTR_DOUBLE_MAP, doubleMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_DOUBLE_MAP));
        doubleMap = (Map<String, Double>) mapTest.getAttribute(ATTR_DOUBLE_MAP);
        assertEquals(3, doubleMap.size());
        assertEquals(Double.valueOf(0.0), doubleMap.get("key_0"));
        assertEquals(Double.valueOf(1.1), doubleMap.get("key_1"));
        assertEquals(Double.valueOf(2.2), doubleMap.get("key_2"));

        // 3. Remove one key-value pair
        doubleMap = new HashMap<>();
        doubleMap.put("key_0", 0.0);
        doubleMap.put("key_2", 2.2);
        mapTest.setAttribute(ATTR_DOUBLE_MAP, doubleMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_DOUBLE_MAP));
        doubleMap = (Map<String, Double>) mapTest.getAttribute(ATTR_DOUBLE_MAP);
        assertEquals(2, doubleMap.size());
        assertEquals(Double.valueOf(0.0), doubleMap.get("key_0"));
        assertEquals(Double.valueOf(2.2), doubleMap.get("key_2"));

        // 4. Add one + Remove one key-value pair
        doubleMap = new HashMap<>();
        doubleMap.put("key_1", 1.1);
        doubleMap.put("key_2", 2.2);
        mapTest.setAttribute(ATTR_DOUBLE_MAP, doubleMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_DOUBLE_MAP));
        doubleMap = (Map<String, Double>) mapTest.getAttribute(ATTR_DOUBLE_MAP);
        assertEquals(2, doubleMap.size());
        assertEquals(Double.valueOf(1.1), doubleMap.get("key_1"));
        assertEquals(Double.valueOf(2.2), doubleMap.get("key_2"));

        // 5. Remove all key-value pairs
        mapTest.setAttribute(ATTR_DOUBLE_MAP, null);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNull(mapTest.getAttribute(ATTR_DOUBLE_MAP));

        // 6. Add all 3 key-value pairs back
        doubleMap = new HashMap<>();
        doubleMap.put("key_0", 0.0);
        doubleMap.put("key_1", 1.1);
        doubleMap.put("key_2", 2.2);
        mapTest.setAttribute(ATTR_DOUBLE_MAP, doubleMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_DOUBLE_MAP));
        doubleMap = (Map<String, Double>) mapTest.getAttribute(ATTR_DOUBLE_MAP);
        assertEquals(3, doubleMap.size());
        assertEquals(Double.valueOf(0.0), doubleMap.get("key_0"));
        assertEquals(Double.valueOf(1.1), doubleMap.get("key_1"));
        assertEquals(Double.valueOf(2.2), doubleMap.get("key_2"));

        LOG.info("<< mapOfDoubles");
    }

    @Test
    private static void mapOfFloats() throws Exception {
        LOG.info(">> mapOfFloats");

        /*
         * MapTest.floatMapTest ==> map<string,float>
         *
         * Update MapTest
         * 1. Add one key-value pair
         * 2. Add two more key-value pairs
         * 3. Remove one key-value pair
         * 4. Add one + Remove one key-value pair
         * 5. Remove all key-value pairs
         * 6. Add all 3 key-value pairs back
         * */

        AtlasEntity mapTest = getAtlasEntity(TYPE_MAP_TEST, "maptest_0");
        String mapTestGuid = createEntity(mapTest).getGuidAssignments().values().iterator().next();

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        // 1. Add one key-value pair
        Map<String, Float> floatMap = new HashMap<>();
        floatMap.put("key_1", 1.1f);
        mapTest.setAttribute(ATTR_FLOAT_MAP, floatMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_FLOAT_MAP));
        Map<String, Object> rawMap = (Map<String, Object>) mapTest.getAttribute(ATTR_FLOAT_MAP);
        assertEquals(1, rawMap.size());
        assertEquals(1.1f, ((Number)rawMap.get("key_1")).floatValue(), 0.0001f);

        // 2. Add two more key-value pairs
        floatMap = new HashMap<>();
        floatMap.put("key_0", 0.0f);
        floatMap.put("key_1", 1.1f);
        floatMap.put("key_2", 2.2f);
        mapTest.setAttribute(ATTR_FLOAT_MAP, floatMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_FLOAT_MAP));
        rawMap = (Map<String, Object>) mapTest.getAttribute(ATTR_FLOAT_MAP);
        assertEquals(3, rawMap.size());
        assertEquals(0.0f, ((Number)rawMap.get("key_0")).floatValue(), 0.0001f);
        assertEquals(1.1f, ((Number)rawMap.get("key_1")).floatValue(), 0.0001f);
        assertEquals(2.2f, ((Number)rawMap.get("key_2")).floatValue(), 0.0001f);

        // 3. Remove one key-value pair
        floatMap = new HashMap<>();
        floatMap.put("key_0", 0.0f);
        floatMap.put("key_2", 2.2f);
        mapTest.setAttribute(ATTR_FLOAT_MAP, floatMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_FLOAT_MAP));
        rawMap = (Map<String, Object>) mapTest.getAttribute(ATTR_FLOAT_MAP);
        assertEquals(2, rawMap.size());
        assertEquals(0.0f, ((Number)rawMap.get("key_0")).floatValue(), 0.0001f);
        assertEquals(2.2f, ((Number)rawMap.get("key_2")).floatValue(), 0.0001f);

        // 4. Add one + Remove one key-value pair
        floatMap = new HashMap<>();
        floatMap.put("key_1", 1.1f);
        floatMap.put("key_2", 2.2f);
        mapTest.setAttribute(ATTR_FLOAT_MAP, floatMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_FLOAT_MAP));
        rawMap = (Map<String, Object>) mapTest.getAttribute(ATTR_FLOAT_MAP);
        assertEquals(2, rawMap.size());
        assertEquals(1.1f, ((Number)rawMap.get("key_1")).floatValue(), 0.0001f);
        assertEquals(2.2f, ((Number)rawMap.get("key_2")).floatValue(), 0.0001f);

        // 5. Remove all key-value pairs
        mapTest.setAttribute(ATTR_FLOAT_MAP, null);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNull(mapTest.getAttribute(ATTR_FLOAT_MAP));

        // 6. Add all 3 key-value pairs back
        floatMap = new HashMap<>();
        floatMap.put("key_0", 0.0f);
        floatMap.put("key_1", 1.1f);
        floatMap.put("key_2", 2.2f);
        mapTest.setAttribute(ATTR_FLOAT_MAP, floatMap);
        createEntity(mapTest);

        sleep(2);
        mapTest = getEntity(mapTestGuid);

        assertNotNull(mapTest.getAttribute(ATTR_FLOAT_MAP));
        rawMap = (Map<String, Object>) mapTest.getAttribute(ATTR_FLOAT_MAP);
        assertEquals(3, rawMap.size());
        assertEquals(0.0f, ((Number)rawMap.get("key_0")).floatValue(), 0.0001f);
        assertEquals(1.1f, ((Number)rawMap.get("key_1")).floatValue(), 0.0001f);
        assertEquals(2.2f, ((Number)rawMap.get("key_2")).floatValue(), 0.0001f);

        LOG.info("<< mapOfFloats");
    }
} 