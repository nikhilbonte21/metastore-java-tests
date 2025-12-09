package com.tests.main.sanity.audits;

import com.tests.main.Test;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.audit.EntityAuditEventV2;
import org.apache.atlas.model.instance.AtlasEntity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.tests.main.utils.TestUtil.ANCHOR;
import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.REL_ASSIGNED_ENTITIES;
import static com.tests.main.utils.TestUtil.REL_MEANINGS;
import static com.tests.main.utils.TestUtil.TYPE_DATABASE;
import static com.tests.main.utils.TestUtil.TYPE_GLOSSARY;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.TYPE_TERM;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getEntityAudit;
import static com.tests.main.utils.TestUtil.getObjectId;
import static com.tests.main.utils.TestUtil.getObjectIdsAsList;
import static com.tests.main.utils.TestUtil.listOf;
import static com.tests.main.utils.TestUtil.mapOf;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntity;
import static com.tests.main.utils.TestUtil.verifyESAttributes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.apache.atlas.model.audit.EntityAuditEventV2.EntityAuditActionV2.*;


public class SanityESAuditsUpdate implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(SanityESAuditsUpdate.class);

    private static final long SLEEP = 2000;

    public static void main(String[] args) throws Exception {
        try {
            new SanityESAuditsUpdate().run();
            //TestRunner.runTests(SanityESAuditsUpdate.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running SanityESAuditsUpdate tests");

        long start = System.currentTimeMillis();
        try {
            //https://atlanhq.atlassian.net/wiki/spaces/~61277197fc5509007110a8d6/pages/728760421/MLH-173+DEV+tests

            /*  createDatabase
                createTerm
                updateDatabaseToAddMeanings
            */
            testOne();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running SanityESAuditsUpdate tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    @Test
    private static void testOne() throws Exception {
        LOG.info(">> testOne");

        /*
          create Database
          create Glossary
          create Term
          update Database To Add Meanings
        */

        long timeInMillis = System.currentTimeMillis();

        AtlasEntity database = getAtlasEntity(TYPE_DATABASE, "database_0");
        AtlasEntity glossary = getAtlasEntity(TYPE_GLOSSARY, "glossary_0");
        AtlasEntity term = getAtlasEntity(TYPE_TERM, "term_0");

        String databaseGuid = createEntity(database).getGuidAssignments().values().iterator().next();
        String glossaryGuid = createEntity(glossary).getGuidAssignments().values().iterator().next();

        sleep(SLEEP);

        List<EntityAuditEventV2> events = getEntityAudit(databaseGuid, timeInMillis);
        assertEquals(1, events.size());
        assertEquals(ENTITY_CREATE, events.get(0).getAction());

        events = getEntityAudit(glossaryGuid, timeInMillis);
        assertEquals(1, events.size());
        assertEquals(ENTITY_CREATE, events.get(0).getAction());

        timeInMillis = System.currentTimeMillis();

        term.setRelationshipAttribute(ANCHOR, getObjectId(glossaryGuid, TYPE_GLOSSARY));
        String termGuid = createEntity(term).getGuidAssignments().values().iterator().next();
        sleep(SLEEP);

        events = getEntityAudit(glossaryGuid, timeInMillis);
        assertEquals(1, events.size());
        assertEquals(ENTITY_UPDATE, events.get(0).getAction());

        events = getEntityAudit(termGuid, timeInMillis);
        assertEquals(2, events.size());
        assertEquals(ENTITY_CREATE, events.get(1).getAction());

        assertEquals(ENTITY_UPDATE, events.get(0).getAction());
        assertNotNull(events.get(0).getDetail());
        Map<String, Object> relAttributes = (Map<String, Object>) events.get(0).getDetail().get("relationshipAttributes");
        assertNotNull(relAttributes);
        Map<String, Object> anchor = (Map<String, Object>) relAttributes.get("anchor");
        assertNotNull(anchor);
        assertEquals(glossaryGuid, anchor.get("guid"));
        assertEquals(TYPE_GLOSSARY, anchor.get("typeName"));


        timeInMillis = System.currentTimeMillis();

        database = getEntity(databaseGuid);
        AtlasEntity minimalDatabase = new AtlasEntity();
        minimalDatabase.setTypeName(TYPE_DATABASE);
        minimalDatabase.setAttribute(NAME, database.getAttribute(NAME));
        minimalDatabase.setAttribute(QUALIFIED_NAME, database.getAttribute(QUALIFIED_NAME));

        minimalDatabase.setAppendRelationshipAttribute(REL_MEANINGS, getObjectIdsAsList(TYPE_TERM, termGuid));
        updateEntity(minimalDatabase);
        sleep(SLEEP);

        database = getEntity(databaseGuid);
        term = getEntity(termGuid);

        assertNotNull(database.getRelationshipAttribute(REL_MEANINGS));
        assertEquals(1, ((List) database.getRelationshipAttribute(REL_MEANINGS)).size());

        assertNotNull(term.getRelationshipAttribute(REL_ASSIGNED_ENTITIES));
        assertEquals(1, ((List) term.getRelationshipAttribute(REL_ASSIGNED_ENTITIES)).size());

        events = getEntityAudit(databaseGuid, timeInMillis);
        assertEquals(1, events.size());
        assertEquals(ENTITY_UPDATE, events.get(0).getAction());
        assertNotNull(events.get(0).getDetail());
        relAttributes = (Map<String, Object>) events.get(0).getDetail().get("relationshipAttributes");
        assertNotNull(relAttributes);
        List<Map<String, Object>> meanings = (List<Map<String, Object>>) relAttributes.get(REL_MEANINGS);
        assertNotNull(meanings);
        assertEquals(1, meanings.size());
        assertEquals(termGuid, meanings.get(0).get("guid"));
        assertEquals(TYPE_TERM, meanings.get(0).get("typeName"));

        LOG.info(">> testOne");
    }
}
