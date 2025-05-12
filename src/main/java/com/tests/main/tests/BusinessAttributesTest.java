package com.tests.main.tests;


import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.Utils;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.tests.main.utils.TestUtil.*;
import static org.junit.Assert.*;


public class BusinessAttributesTest implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(BusinessAttributesTest.class);

    private static BusinessMetadata bm_0;
    private static BusinessMetadata bm_1;
    private static AtlasEntity table_0;

    public static void main(String[] args) throws Exception {
        try {
            new BusinessAttributesTest().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running BusinessAttributesTest tests");

        long start = System.currentTimeMillis();
        try {
            setup();

            empty();

        } catch (Exception e) {
            throw e;
        } finally {
            deleteBMTypeDefs();
            LOG.info("Completed running BusinessAttributesTest tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void setup() throws Exception {
        LOG.info(">> setup");

        String randomStr = getRandomName();

        AtlasBusinessMetadataDef _bm_0 = getBusinessMetadataDef("bm_0" + randomStr);
        _bm_0.addAttribute(getBMAttrDef("attr_0", "string", "Table"));
        _bm_0.addAttribute(getBMAttrDef("attr_1", "int", "Table"));

        AtlasBusinessMetadataDef _bm_1 = getBusinessMetadataDef("bm_1" + randomStr);
        _bm_1.addAttribute(getBMAttrDef("attr_2", "string", "Table"));
        _bm_1.addAttribute(getBMAttrDef("attr_3", "int", "Table"));

        bm_0 = new BusinessMetadata(createBusinessMetadataDefs(_bm_0));
        bm_1 = new BusinessMetadata(createBusinessMetadataDefs(_bm_1));

        AtlasEntity entity = getAtlasEntityExt("Table", getRandomName()).getEntity();
        String guid = createEntity(entity).getGuidAssignments().values().iterator().next();
        sleep();

        table_0 = getEntity(guid);

        resetBM();

        AtlasEntity table = getEntity(table_0.getGuid());
        Map<String, Map<String, Object>> bms = table.getBusinessAttributes();

        assertEquals("string_1", bms.get(bm_0.getName()).get(bm_0.getStringAttr().getName()));
        assertEquals(1, bms.get(bm_0.getName()).get(bm_0.getIntAttr().getName()));
        assertEquals("string_2", bms.get(bm_1.getName()).get(bm_1.getStringAttr().getName()));
        assertEquals(2, bms.get(bm_1.getName()).get(bm_1.getIntAttr().getName()));

        LOG.info(">> setup");
    }
    
    private static void empty() throws Exception {
        LOG.info(">> notPassed");
        resetBM();

        Map<String, Map<String, Object>> bmsToUpdate = new HashMap<>();

        AtlasEntity entity = getAtlasEntityExt("Table", getRandomName()).getEntity();
        LOG.info(Utils.toJson(entity));

        //table_0.setBusinessAttribute(bmsToUpdate);

        AtlasEntity table = getEntity(table_0.getGuid());

        LOG.info(">> notPassed");
    }

    private static void resetBM() throws Exception {
        Map<String, Map<String, Object>> businessAttributes = new HashMap<>();

        businessAttributes.put(bm_0.getName(), mapOf(
                bm_0.getStringAttr().getName(), "string_1",
                bm_0.getIntAttr().getName(), 1)
        );

        businessAttributes.put(bm_1.getName(), mapOf(
                bm_1.getStringAttr().getName(), "string_2",
                bm_1.getIntAttr().getName(), 2)
        );

        table_0.setBusinessAttributes(businessAttributes);

        createEntity(table_0);
        sleep();
    }

    private static void deleteBMTypeDefs() throws Exception {
        deleteEntities(Collections.singletonList(table_0.getGuid()));
        sleep();
        deleteTypeByName(bm_0.getName());
        deleteTypeByName(bm_1.getName());
    }

    private static void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}