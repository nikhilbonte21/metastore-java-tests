package com.tests.main.sanity.bulk;

import com.tests.main.Test;
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

public class SanityBasicTypeAttributesMutations implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(SanityBasicTypeAttributesMutations.class);

    // Type definitions
    public static final String TYPE_TEST_BASIC = "BasicType";
    
    // Attribute names
    public static final String ATTR_STRING = "stringAttr";
    public static final String ATTR_ENUM = "enumAttr";
    public static final String ATTR_BOOLEAN = "booleanAttr";
    public static final String ATTR_LONG = "longAttr";
    public static final String ATTR_FLOAT = "floatAttr";
    public static final String ATTR_DOUBLE = "doubleAttr";
    public static final String ATTR_INT = "intAttr";
    public static final String ATTR_DATE = "dateAttr";

    // Default values
    private static final long DEFAULT_LONG = 0L;
    private static final float DEFAULT_FLOAT = 0.0f;
    private static final double DEFAULT_DOUBLE = 0.0;
    private static final int DEFAULT_INT = 0;

    public static void main(String[] args) throws Exception {
        try {
            new SanityBasicTypeAttributesMutations().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running SanityBasicTypeAttributesMutations tests");

        long start = System.currentTimeMillis();
        try {
            createTypeDefs();
            testStringAttribute();
            testEnumAttribute();
            testBooleanAttribute();
            testLongAttribute();
            testFloatAttribute();
            testDoubleAttribute();
            testIntAttribute();
            testDateAttribute();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running SanityBasicTypeAttributesMutations tests, took {} seconds", 
                    (System.currentTimeMillis() - start) / 1000);
        }
    }

    private void createTypeDefs() throws Exception {
        // Create entity definition for TestBasicType
        AtlasEntityDef entityDef = new AtlasEntityDef();
        entityDef.setName(TYPE_TEST_BASIC);
        entityDef.setDescription("Test entity for basic type attributes");
        entityDef.setServiceType("test");
        entityDef.setTypeVersion("1.0");
        entityDef.setSuperTypes(Collections.singleton("Asset"));
        
        // Add attributes
        List<AtlasAttributeDef> attributes = new ArrayList<>();
        
        // String attribute
        AtlasAttributeDef stringAttr = new AtlasAttributeDef();
        stringAttr.setName(ATTR_STRING);
        stringAttr.setTypeName("string");
        stringAttr.setIsOptional(true);
        stringAttr.setIsUnique(false);
        stringAttr.setDescription("String attribute for testing");
        attributes.add(stringAttr);

        // Enum attribute
        AtlasAttributeDef enumAttr = new AtlasAttributeDef();
        enumAttr.setName(ATTR_ENUM);
        enumAttr.setTypeName("SourceCostUnitType");
        enumAttr.setIsOptional(true);
        enumAttr.setIsUnique(false);
        enumAttr.setDescription("String attribute for testing");
        attributes.add(enumAttr);
        
        // Boolean attribute
        AtlasAttributeDef booleanAttr = new AtlasAttributeDef();
        booleanAttr.setName(ATTR_BOOLEAN);
        booleanAttr.setTypeName("boolean");
        booleanAttr.setIsOptional(true);
        booleanAttr.setIsUnique(false);
        booleanAttr.setDescription("Boolean attribute for testing");
        attributes.add(booleanAttr);
        
        // Long attribute
        AtlasAttributeDef longAttr = new AtlasAttributeDef();
        longAttr.setName(ATTR_LONG);
        longAttr.setTypeName("long");
        longAttr.setIsOptional(true);
        longAttr.setIsUnique(false);
        longAttr.setDescription("Long attribute for testing");
        longAttr.setDefaultValue(String.valueOf(DEFAULT_LONG));
        attributes.add(longAttr);
        
        // Float attribute
        AtlasAttributeDef floatAttr = new AtlasAttributeDef();
        floatAttr.setName(ATTR_FLOAT);
        floatAttr.setTypeName("float");
        floatAttr.setIsOptional(true);
        floatAttr.setIsUnique(false);
        floatAttr.setDescription("Float attribute for testing");
        floatAttr.setDefaultValue(String.valueOf(DEFAULT_FLOAT));
        attributes.add(floatAttr);
        
        // Double attribute
        AtlasAttributeDef doubleAttr = new AtlasAttributeDef();
        doubleAttr.setName(ATTR_DOUBLE);
        doubleAttr.setTypeName("double");
        doubleAttr.setIsOptional(true);
        doubleAttr.setIsUnique(false);
        doubleAttr.setDescription("Double attribute for testing");
        doubleAttr.setDefaultValue(String.valueOf(DEFAULT_DOUBLE));
        attributes.add(doubleAttr);
        
        // Int attribute
        AtlasAttributeDef intAttr = new AtlasAttributeDef();
        intAttr.setName(ATTR_INT);
        intAttr.setTypeName("int");
        intAttr.setIsOptional(true);
        intAttr.setIsUnique(false);
        intAttr.setDescription("Int attribute for testing");
        intAttr.setDefaultValue(String.valueOf(DEFAULT_INT));
        attributes.add(intAttr);
        
        // Date attribute
        AtlasAttributeDef dateAttr = new AtlasAttributeDef();
        dateAttr.setName(ATTR_DATE);
        dateAttr.setTypeName("date");
        dateAttr.setIsOptional(true);
        dateAttr.setIsUnique(false);
        dateAttr.setDescription("Date attribute for testing");
        attributes.add(dateAttr);
        
        entityDef.setAttributeDefs(attributes);
        
        // Create type definition
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setEntityDefs(Collections.singletonList(entityDef));
        
        // Create type in Atlas
        try {
            TestUtil.createTypeDefs(typesDef);
        } catch (Exception e) {
            LOG.warn("Type {} already exists", TYPE_TEST_BASIC);
        }
    }

    @Test
    private void testStringAttribute() throws Exception {
        LOG.info(">> testStringAttribute");

        // Create test entity
        AtlasEntity entity = getAtlasEntity(TYPE_TEST_BASIC, "test_entity_1");
        String entityGuid = createEntity(entity).getGuidAssignments().values().iterator().next();

        sleep(2);
        entity = getEntity(entityGuid);

        // Test 1: Set string value
        String testValue = "test_string_value";
        entity.setAttribute(ATTR_STRING, testValue);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(testValue, entity.getAttribute(ATTR_STRING));
        verifyESAttributes(entityGuid, mapOf(ATTR_STRING, testValue));

        // Test 2: Update string value
        String updatedValue = "updated_string_value";
        entity.setAttribute(ATTR_STRING, updatedValue);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(updatedValue, entity.getAttribute(ATTR_STRING));
        verifyESAttributes(entityGuid, mapOf(ATTR_STRING, updatedValue));

        // Test 3: Set null value
        entity.setAttribute(ATTR_STRING, null);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertNull(entity.getAttribute(ATTR_STRING));
        verifyESAttributes(entityGuid, mapOf(ATTR_STRING, null));

        LOG.info(">> testStringAttribute completed");
    }

    @Test
    private void testEnumAttribute() throws Exception {
        LOG.info(">> testEnumAttribute");

        // Create test entity
        AtlasEntity entity = getAtlasEntity(TYPE_TEST_BASIC, "test_entity_enum");
        String entityGuid = createEntity(entity).getGuidAssignments().values().iterator().next();

        sleep(2);
        entity = getEntity(entityGuid);

        // Test 1: Set string value
        String testValue = "Credits";
        entity.setAttribute(ATTR_ENUM, testValue);
        createEntity(entity);
        sleep(2);
        verifyESAttributes(entityGuid, mapOf(ATTR_ENUM, testValue));

        entity = getEntity(entityGuid);
        assertEquals(testValue, entity.getAttribute(ATTR_ENUM));

        // Test 2: Update string value
        String updatedValue = "bytes";
        entity.setAttribute(ATTR_ENUM, updatedValue);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(updatedValue, entity.getAttribute(ATTR_ENUM));
        verifyESAttributes(entityGuid, mapOf(ATTR_ENUM, updatedValue));

        // Test 3: Set null value
        entity.setAttribute(ATTR_ENUM, null);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertNull(entity.getAttribute(ATTR_ENUM));
        verifyESAttributes(entityGuid, mapOf(ATTR_ENUM, null));

        LOG.info(">> testEnumAttribute completed");
    }

    @Test
    private void testBooleanAttribute() throws Exception {
        LOG.info(">> testBooleanAttribute");

        // Create test entity
        AtlasEntity entity = getAtlasEntity(TYPE_TEST_BASIC, "test_entity_2");
        String entityGuid = createEntity(entity).getGuidAssignments().values().iterator().next();

        sleep(2);
        entity = getEntity(entityGuid);

        // Test 1: Set true value
        entity.setAttribute(ATTR_BOOLEAN, true);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertTrue((Boolean) entity.getAttribute(ATTR_BOOLEAN));
        verifyESAttributes(entityGuid, mapOf(ATTR_BOOLEAN, true));

        // Test 2: Set false value
        entity.setAttribute(ATTR_BOOLEAN, false);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertFalse((Boolean) entity.getAttribute(ATTR_BOOLEAN));
        verifyESAttributes(entityGuid, mapOf(ATTR_BOOLEAN, false));

        // Test 3: Set null value
        entity.setAttribute(ATTR_BOOLEAN, null);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertFalse((Boolean) entity.getAttribute(ATTR_BOOLEAN));
        verifyESAttributes(entityGuid, mapOf(ATTR_BOOLEAN, false));

        LOG.info(">> testBooleanAttribute completed");
    }

    @Test
    private void testLongAttribute() throws Exception {
        LOG.info(">> testLongAttribute");

        // Create test entity
        AtlasEntity entity = getAtlasEntity(TYPE_TEST_BASIC, "test_entity_3");
        String entityGuid = createEntity(entity).getGuidAssignments().values().iterator().next();

        sleep(2);
        entity = getEntity(entityGuid);

        // Test 0: Verify default value
        assertEquals(DEFAULT_LONG, ((Number) entity.getAttribute(ATTR_LONG)).longValue());
        verifyESAttributes(entityGuid, mapOf(ATTR_LONG, DEFAULT_LONG));

        // Test 1: Set positive long value
        long testValue = 123456789L;
        entity.setAttribute(ATTR_LONG, testValue);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(testValue, ((Number) entity.getAttribute(ATTR_LONG)).longValue());
        verifyESAttributes(entityGuid, mapOf(ATTR_LONG, testValue));

        // Test 2: Set negative long value
        long negativeValue = -987654321L;
        entity.setAttribute(ATTR_LONG, negativeValue);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(negativeValue, ((Number) entity.getAttribute(ATTR_LONG)).longValue());
        verifyESAttributes(entityGuid, mapOf(ATTR_LONG, negativeValue));

        // Test 3: Set null value
        entity.setAttribute(ATTR_LONG, null);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(DEFAULT_LONG, ((Number) entity.getAttribute(ATTR_LONG)).longValue());
        verifyESAttributes(entityGuid, mapOf(ATTR_LONG, DEFAULT_LONG));

        LOG.info(">> testLongAttribute completed");
    }

    @Test
    private void testFloatAttribute() throws Exception {
        LOG.info(">> testFloatAttribute");

        // Create test entity
        AtlasEntity entity = getAtlasEntity(TYPE_TEST_BASIC, "test_entity_4");
        String entityGuid = createEntity(entity).getGuidAssignments().values().iterator().next();

        sleep(2);
        entity = getEntity(entityGuid);

        // Test 0: Verify default value
        assertEquals(DEFAULT_FLOAT, ((Number) entity.getAttribute(ATTR_FLOAT)).floatValue(), 0.0001f);
        verifyESAttributes(entityGuid, mapOf(ATTR_FLOAT, DEFAULT_FLOAT));

        // Test 1: Set positive float value
        float testValue = 123.456f;
        entity.setAttribute(ATTR_FLOAT, testValue);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(testValue, ((Number) entity.getAttribute(ATTR_FLOAT)).floatValue(), 0.0001f);
        verifyESAttributes(entityGuid, mapOf(ATTR_FLOAT, testValue));

        // Test 2: Set negative float value
        float negativeValue = -987.654f;
        entity.setAttribute(ATTR_FLOAT, negativeValue);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(negativeValue, ((Number) entity.getAttribute(ATTR_FLOAT)).floatValue(), 0.0001f);
        verifyESAttributes(entityGuid, mapOf(ATTR_FLOAT, negativeValue));

        // Test 3: Set null value
        entity.setAttribute(ATTR_FLOAT, null);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(DEFAULT_FLOAT, ((Number) entity.getAttribute(ATTR_FLOAT)).floatValue(), 0.0001f);
        verifyESAttributes(entityGuid, mapOf(ATTR_FLOAT, DEFAULT_FLOAT));

        LOG.info(">> testFloatAttribute completed");
    }

    @Test
    private void testDoubleAttribute() throws Exception {
        LOG.info(">> testDoubleAttribute");

        // Create test entity
        AtlasEntity entity = getAtlasEntity(TYPE_TEST_BASIC, "test_entity_6");
        String entityGuid = createEntity(entity).getGuidAssignments().values().iterator().next();

        sleep(2);
        entity = getEntity(entityGuid);

        // Test 0: Verify default value
        assertEquals(DEFAULT_DOUBLE, ((Number) entity.getAttribute(ATTR_DOUBLE)).doubleValue(), 0.0001);
        verifyESAttributes(entityGuid, mapOf(ATTR_DOUBLE, DEFAULT_DOUBLE));

        // Test 1: Set positive double value
        double testValue = 123.456789;
        entity.setAttribute(ATTR_DOUBLE, testValue);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(testValue, ((Number) entity.getAttribute(ATTR_DOUBLE)).doubleValue(), 0.0001);
        verifyESAttributes(entityGuid, mapOf(ATTR_DOUBLE, testValue));

        // Test 2: Set negative double value
        double negativeValue = -987.654321;
        entity.setAttribute(ATTR_DOUBLE, negativeValue);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(negativeValue, ((Number) entity.getAttribute(ATTR_DOUBLE)).doubleValue(), 0.0001);
        verifyESAttributes(entityGuid, mapOf(ATTR_DOUBLE, negativeValue));

        // Test 3: Set null value
        entity.setAttribute(ATTR_DOUBLE, null);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(DEFAULT_DOUBLE, ((Number) entity.getAttribute(ATTR_DOUBLE)).doubleValue(), 0.0001);
        verifyESAttributes(entityGuid, mapOf(ATTR_DOUBLE, DEFAULT_DOUBLE));

        LOG.info(">> testDoubleAttribute completed");
    }

    @Test
    private void testIntAttribute() throws Exception {
        LOG.info(">> testIntAttribute");

        // Create test entity
        AtlasEntity entity = getAtlasEntity(TYPE_TEST_BASIC, "test_entity_7");
        String entityGuid = createEntity(entity).getGuidAssignments().values().iterator().next();

        sleep(2);
        entity = getEntity(entityGuid);

        // Test 0: Verify default value
        assertEquals(DEFAULT_INT, ((Number) entity.getAttribute(ATTR_INT)).intValue());
        verifyESAttributes(entityGuid, mapOf(ATTR_INT, DEFAULT_INT));

        // Test 1: Set positive int value
        int testValue = 123456;
        entity.setAttribute(ATTR_INT, testValue);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(testValue, ((Number) entity.getAttribute(ATTR_INT)).intValue());
        verifyESAttributes(entityGuid, mapOf(ATTR_INT, testValue));

        // Test 2: Set negative int value
        int negativeValue = -987654;
        entity.setAttribute(ATTR_INT, negativeValue);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(negativeValue, ((Number) entity.getAttribute(ATTR_INT)).intValue());
        verifyESAttributes(entityGuid, mapOf(ATTR_INT, negativeValue));

        // Test 3: Set null value
        entity.setAttribute(ATTR_INT, null);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(DEFAULT_INT, ((Number) entity.getAttribute(ATTR_INT)).intValue());
        verifyESAttributes(entityGuid, mapOf(ATTR_INT, DEFAULT_INT));

        LOG.info(">> testIntAttribute completed");
    }

    @Test
    private void testDateAttribute() throws Exception {
        LOG.info(">> testDateAttribute");

        // Create test entity
        AtlasEntity entity = getAtlasEntity(TYPE_TEST_BASIC, "test_entity_5");
        String entityGuid = createEntity(entity).getGuidAssignments().values().iterator().next();

        sleep(2);
        entity = getEntity(entityGuid);

        // Test 1: Set current date
        Date currentDate = new Date();
        entity.setAttribute(ATTR_DATE, currentDate);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        Long retrievedMillis = (Long) entity.getAttribute(ATTR_DATE);
        assertEquals(currentDate.getTime(), retrievedMillis.longValue());
        verifyESAttributes(entityGuid, mapOf(ATTR_DATE, currentDate.getTime()));

        // Test 2: Set specific date
        Calendar calendar = Calendar.getInstance();
        calendar.set(2024, Calendar.JANUARY, 1, 12, 0, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        Date specificDate = calendar.getTime();
        
        entity.setAttribute(ATTR_DATE, specificDate);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        retrievedMillis = (Long) entity.getAttribute(ATTR_DATE);
        assertEquals(specificDate.getTime(), retrievedMillis.longValue());
        verifyESAttributes(entityGuid, mapOf(ATTR_DATE, specificDate.getTime()));

        // Test 3: Set null value
        entity.setAttribute(ATTR_DATE, null);
        createEntity(entity);
        sleep(2);

        entity = getEntity(entityGuid);
        assertEquals(0, entity.getAttribute(ATTR_DATE));
        verifyESAttributes(entityGuid, mapOf(ATTR_DATE, 0));

        LOG.info(">> testDateAttribute completed");
    }
} 