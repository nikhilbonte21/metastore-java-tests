package com.tests.main.sanity.meanings.attributes;

import com.tests.main.sanity.tag.propagation.PropagationSwitchRestrictConfs;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tests.main.sanity.tag.propagation.PropagationUtils.getTagTypeDefs;
import static com.tests.main.utils.TestUtil.ANCHOR;
import static com.tests.main.utils.TestUtil.ES_MEANINGS;
import static com.tests.main.utils.TestUtil.ES_MEANING_NAMES;
import static com.tests.main.utils.TestUtil.ES_MEANING_TEXT;
import static com.tests.main.utils.TestUtil.REL_MEANINGS;
import static com.tests.main.utils.TestUtil.TYPE_GLOSSARY;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.TYPE_TERM;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getName;
import static com.tests.main.utils.TestUtil.getObjectId;
import static com.tests.main.utils.TestUtil.getObjectIdsAsList;
import static com.tests.main.utils.TestUtil.getQualifiedName;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;
import static com.tests.main.utils.TestUtil.verifyESAttributes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ViaRelationshipAttributes implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(ViaRelationshipAttributes.class);

    private static long SLEEP = 2000;


    private static List<String> tagTypeNames;

    static {
        tagTypeNames = getTagTypeDefs(1);
    }

    public static void main(String[] args) throws Exception {
        try {
            new ViaRelationshipAttributes().run();
            //TestRunner.runTests(ViaRelationshipAttributes.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running ViaRelationshipAttributes tests");

        long start = System.currentTimeMillis();
        try {

            linkAndUnlinkTermsToTable();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running ViaRelationshipAttributes tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void linkAndUnlinkTermsToTable() throws Exception {
        LOG.info(">> linkAndUnlinkTermsToTable");

        Map<String, Object> expectedEsAttrs = new HashMap<>();

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "test_glossary" + getRandomName());

        Map<String, String> guidAssignments = createEntitiesBulk(table, glossary).getGuidAssignments();
        String table_guid = guidAssignments.get(table.getGuid());
        String glossary_guid = guidAssignments.get(glossary.getGuid());

        sleep(SLEEP);

        AtlasEntity term0 = getAtlasEntity(TYPE_TERM, "test_term_0" + getRandomName());
        AtlasEntity term1 = getAtlasEntity(TYPE_TERM, "test_term_1" + getRandomName());
        AtlasEntity term2 = getAtlasEntity(TYPE_TERM, "test_term_2" + getRandomName());

        term0.setRelationshipAttribute(ANCHOR, getObjectId(glossary_guid, TYPE_GLOSSARY));
        term1.setRelationshipAttribute(ANCHOR, getObjectId(glossary_guid, TYPE_GLOSSARY));
        term2.setRelationshipAttribute(ANCHOR, getObjectId(glossary_guid, TYPE_GLOSSARY));

        guidAssignments = createEntitiesBulk(term0, term1, term2).getGuidAssignments();
        String term0_guid = guidAssignments.get(term0.getGuid());
        String term1_guid = guidAssignments.get(term1.getGuid());
        String term2_guid = guidAssignments.get(term2.getGuid());

        term0 = getEntity(term0_guid);
        term1 = getEntity(term1_guid);
        term2 = getEntity(term2_guid);

        String term0_qn = getQualifiedName(term0);
        String term1_qn = getQualifiedName(term1);
        String term2_qn = getQualifiedName(term2);

        String term0_name = getName(term0);
        String term1_name = getName(term1);
        String term2_name = getName(term2);

        // Link term0
        table.setRelationshipAttribute(REL_MEANINGS, getObjectIdsAsList(TYPE_TERM, term0_guid));
        updateEntity(table);
        sleep(SLEEP);

        table = getEntity(table_guid);

        assertNotNull(table.getRelationshipAttribute(REL_MEANINGS));
        List<HashMap> meanings = (List<HashMap>) table.getRelationshipAttribute(REL_MEANINGS);
        assertEquals(1, meanings.size());
        assertEquals("ACTIVE", meanings.get(0).get("relationshipStatus"));

        expectedEsAttrs.put(ES_MEANINGS, Collections.singletonList(term0_qn));
        expectedEsAttrs.put(ES_MEANING_NAMES, Collections.singletonList(term0_name));
        expectedEsAttrs.put(ES_MEANING_TEXT, term0_name);
        verifyESAttributes(table_guid, expectedEsAttrs);


        // ADD term1
        table.setRelationshipAttribute(REL_MEANINGS, getObjectIdsAsList(TYPE_TERM, term0_guid, term1_guid));
        updateEntity(table);
        sleep(SLEEP);

        table = getEntity(table_guid);

        assertNotNull(table.getRelationshipAttribute(REL_MEANINGS));
        meanings = (List<HashMap>) table.getRelationshipAttribute(REL_MEANINGS);
        assertEquals(2, meanings.size());
        assertEquals("ACTIVE", meanings.get(0).get("relationshipStatus"));
        assertEquals("ACTIVE", meanings.get(1).get("relationshipStatus"));

        expectedEsAttrs.put(ES_MEANINGS, Arrays.asList(term0_qn, term1_qn));
        expectedEsAttrs.put(ES_MEANING_NAMES, Arrays.asList(term0_name, term1_name));
        expectedEsAttrs.put(ES_MEANING_TEXT, term0_name.concat(",").concat(term1_name));
        verifyESAttributes(table_guid, expectedEsAttrs);


        // ADD term2
        table.setRelationshipAttribute(REL_MEANINGS, getObjectIdsAsList(TYPE_TERM, term0_guid, term1_guid, term2_guid));
        updateEntity(table);
        sleep(SLEEP);

        table = getEntity(table_guid);

        assertNotNull(table.getRelationshipAttribute(REL_MEANINGS));
        meanings = (List<HashMap>) table.getRelationshipAttribute(REL_MEANINGS);
        assertEquals(3, meanings.size());
        assertEquals("ACTIVE", meanings.get(0).get("relationshipStatus"));
        assertEquals("ACTIVE", meanings.get(1).get("relationshipStatus"));
        assertEquals("ACTIVE", meanings.get(2).get("relationshipStatus"));

        expectedEsAttrs.put(ES_MEANINGS, Arrays.asList(term0_qn, term1_qn, term2_qn));
        expectedEsAttrs.put(ES_MEANING_NAMES, Arrays.asList(term0_name, term1_name, term2_name));
        expectedEsAttrs.put(ES_MEANING_TEXT, term0_name.concat(",").concat(term1_name).concat(",").concat(term2_name));
        verifyESAttributes(table_guid, expectedEsAttrs);


        // REMOVE term0
        table.setRelationshipAttribute(REL_MEANINGS, getObjectIdsAsList(TYPE_TERM, term1_guid, term2_guid));
        updateEntity(table);
        sleep(SLEEP);

        table = getEntity(table_guid);

        assertNotNull(table.getRelationshipAttribute(REL_MEANINGS));
        meanings = (List<HashMap>) table.getRelationshipAttribute(REL_MEANINGS);
        assertEquals(2, meanings.size());
        assertEquals("ACTIVE", meanings.get(0).get("relationshipStatus"));
        assertEquals("ACTIVE", meanings.get(1).get("relationshipStatus"));

        expectedEsAttrs.put(ES_MEANINGS, Arrays.asList(term1_qn, term2_qn));
        expectedEsAttrs.put(ES_MEANING_NAMES, Arrays.asList(term1_name, term2_name));
        expectedEsAttrs.put(ES_MEANING_TEXT, term1_name.concat(",").concat(term2_name));
        verifyESAttributes(table_guid, expectedEsAttrs);


        LOG.info(">> linkAndUnlinkTermsToTable");
    }
}
