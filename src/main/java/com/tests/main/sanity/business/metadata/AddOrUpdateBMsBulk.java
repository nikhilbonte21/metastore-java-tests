package com.tests.main.sanity.business.metadata;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.TestUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.Optional;

import static com.tests.main.utils.TestUtil.addOrUpdateCMAttrBulk;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.deleteTypeDefByName;
import static com.tests.main.utils.TestUtil.getBMDefs;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.mapOf;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.verifyESAttributes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AddOrUpdateBMsBulk implements TestsMain {
    /// guid/{guid}/businessmetadata

    private static long SLEEP = 1000;
    
    private static final Logger LOG = LoggerFactory.getLogger(AddOrUpdateBMsBulk.class);

    static String TYPE_CM_TEST = "TestCustomMetadata_1";
    static String ATTR_STRING_ARRAY = "testStringArray";
    static String ATTR_STRING = "testString";

    public static void main(String[] args) throws Exception {
        try {
            new AddOrUpdateBMsBulk().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
            //deleteTypeDefByName(TYPE_CM_TEST);
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running business metadata tests");
        
        // First create the business metadata definition
        createTypeDef();
        
        // Then run the test for table with business metadata
        testTableWithBusinessMetadata();
    }

    private void createTypeDef() throws Exception {
        AtlasBusinessMetadataDef CMTestDef = new AtlasBusinessMetadataDef();

        CMTestDef.setName(TYPE_CM_TEST);
        CMTestDef.setDisplayName(TYPE_CM_TEST);
        CMTestDef.setServiceType("test");
        CMTestDef.setTypeVersion("1.0");

        List<AtlasStructDef.AtlasAttributeDef> arrayTestAttributes = new ArrayList<>();

        // Add enum array attribute
        AtlasStructDef.AtlasAttributeDef enumArrayAttr = new AtlasStructDef.AtlasAttributeDef();
        enumArrayAttr.setName(ATTR_STRING);
        enumArrayAttr.setDisplayName(ATTR_STRING);
        enumArrayAttr.setTypeName("string");
        enumArrayAttr.setIsOptional(true);
        enumArrayAttr.setIsUnique(false);
        enumArrayAttr.setOption("applicableEntityTypes", "[\"Column\",\"Table\",\"View\",\"Process\",\"AtlasGlossary\",\"AtlasGlossaryTerm\"]");
        enumArrayAttr.setOption("maxStrLength", "50");
        arrayTestAttributes.add(enumArrayAttr);

        // Add string array attribute
        AtlasStructDef.AtlasAttributeDef stringAttr = new AtlasStructDef.AtlasAttributeDef();
        stringAttr.setName(ATTR_STRING_ARRAY);
        stringAttr.setDisplayName(ATTR_STRING_ARRAY);
        stringAttr.setTypeName("array<string>");
        stringAttr.setIsOptional(true);
        stringAttr.setIsUnique(false);
        stringAttr.setOption("applicableEntityTypes", "[\"Column\",\"Table\",\"View\",\"Process\",\"AtlasGlossary\",\"AtlasGlossaryTerm\"]");
        stringAttr.setOption("maxStrLength", "50");
        stringAttr.setCardinality(AtlasStructDef.AtlasAttributeDef.Cardinality.SET);

        arrayTestAttributes.add(stringAttr);

        CMTestDef.setAttributeDefs(arrayTestAttributes);

        // Create type definitions
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setBusinessMetadataDefs(Collections.singletonList(CMTestDef));
        try {
            // Create types in Atlas
            typesDef = TestUtil.createTypeDefs(typesDef);
            CMTestDef = typesDef.getBusinessMetadataDefs().get(0);
            LOG.info("Created new typeDef");
        } catch (Exception e) {
            if (e.getMessage().contains("409")) {
                Optional<AtlasBusinessMetadataDef> opt = getBMDefs().getBusinessMetadataDefs().stream()
                        .filter(x -> TYPE_CM_TEST.equals(x.getDisplayName()))
                        .findFirst();

                if (!opt.isPresent()) {
                    throw e;
                }
                CMTestDef = opt.get();
            }
        }

        TYPE_CM_TEST = CMTestDef.getName();
        for (AtlasStructDef.AtlasAttributeDef attributeDef: CMTestDef.getAttributeDefs()) {
            if (attributeDef.getDisplayName().equals(ATTR_STRING))  {
                ATTR_STRING = attributeDef.getName();
            } else if (attributeDef.getDisplayName().equals(ATTR_STRING_ARRAY))  {
                ATTR_STRING_ARRAY = attributeDef.getName();
            }
        }

        LOG.info("\n");
        LOG.info("====================================");
        LOG.info("TYPE_CM_TEST : {}", TYPE_CM_TEST);
        LOG.info("    ATTR_STRING       : {}", ATTR_STRING);
        LOG.info("    ATTR_STRING_ARRAY : {}", ATTR_STRING_ARRAY);
        LOG.info("====================================\n");
    }

    private void testTableWithBusinessMetadata() throws Exception {
        LOG.info("Testing table with business metadata attributes");
        
        // Create a test table
        AtlasEntity table = TestUtil.getAtlasEntity(TestUtil.TYPE_TABLE, "test_table");
        table = TestUtil.createAndGetEntity(table);
        String tableGuid = table.getGuid();

        sleep(SLEEP);

        // Add business metadata attributes to the table
        Map<String, Object> expectedESAttributes = mapOf(ATTR_STRING, "initial_string_value", ATTR_STRING_ARRAY, Arrays.asList("value1", "value2"));
        addOrUpdateCMAttrBulk(tableGuid, mapOf(TYPE_CM_TEST, expectedESAttributes));

        sleep(SLEEP);

        table = getEntity(tableGuid);

        // Verify the business metadata in the asset response
        Map<String, Object> actualBMAttributes = table.getBusinessAttributes().get(TYPE_CM_TEST);
        assertNotNull("Business metadata should be present", actualBMAttributes);
        assertEquals("String attribute should match", "initial_string_value", actualBMAttributes.get(ATTR_STRING));
        assertTrue("Array attribute should contain values",
            ((List<?>) actualBMAttributes.get(ATTR_STRING_ARRAY)).containsAll(Arrays.asList("value1", "value2")));

        // Verify updated values in Elasticsearch
        verifyESAttributes(tableGuid, expectedESAttributes);


        // Update the business metadata attributes
        expectedESAttributes = mapOf(ATTR_STRING, "updated_string_value", ATTR_STRING_ARRAY, Arrays.asList("value3", "value4"));
        addOrUpdateCMAttrBulk(tableGuid, mapOf(TYPE_CM_TEST, expectedESAttributes));
        sleep(SLEEP);

        table = getEntity(tableGuid);

        // Verify the updated business metadata in the asset response
        actualBMAttributes = table.getBusinessAttributes().get(TYPE_CM_TEST);
        assertNotNull("Updated business metadata should be present", actualBMAttributes);
        assertEquals("Updated string attribute should match", "updated_string_value", actualBMAttributes.get(ATTR_STRING));
        assertTrue("Updated array attribute should contain values",
            ((List<?>) actualBMAttributes.get(ATTR_STRING_ARRAY)).containsAll(Arrays.asList("value3", "value4")));

        // Verify updated values in Elasticsearch
        verifyESAttributes(tableGuid, expectedESAttributes);


        // Set null the both business metadata attributes
        expectedESAttributes = mapOf(ATTR_STRING, null, ATTR_STRING_ARRAY, null);
        addOrUpdateCMAttrBulk(tableGuid, mapOf(TYPE_CM_TEST, expectedESAttributes));
        sleep(SLEEP);

        table = getEntity(tableGuid);

        // Verify the updated business metadata in the asset response
        actualBMAttributes = table.getBusinessAttributes().get(TYPE_CM_TEST);
        assertNotNull("Updated business metadata should be present", actualBMAttributes);
        assertNull("Updated string attribute should match", actualBMAttributes.get(ATTR_STRING));
        assertTrue("Updated array attribute should contain values", ((List<?>) actualBMAttributes.get(ATTR_STRING_ARRAY)).isEmpty());

        // Verify updated values in Elasticsearch
        verifyESAttributes(tableGuid, expectedESAttributes);
    }
}
