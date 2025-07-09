package com.tests.main.tests.custom.relationship;

import com.tests.main.CustomException;

import com.tests.main.tests.glossary.tests.TestsMain;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import com.tests.main.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.atlas.model.instance.AtlasStruct;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.createRelationship;
import static com.tests.main.utils.TestUtil.createRelationships;
import static com.tests.main.utils.TestUtil.deleteEntitySoft;
import static com.tests.main.utils.TestUtil.deleteRelationshipByGuid;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.indexSearch;
import static com.tests.main.utils.TestUtil.mapOf;
import static com.tests.main.utils.TestUtil.runAsAdmin;
import static com.tests.main.utils.TestUtil.runAsGod;
import static com.tests.main.utils.TestUtil.runAsGuest;
import static com.tests.main.utils.TestUtil.runAsMember;
import static com.tests.main.utils.TestUtil.sleep;
import static org.junit.Assert.*;


public class CustomRelationships implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(CustomRelationships.class);

    private static AtlasEntity CONNECTION;

    private static final int RELATIONSHIP_THRESHOLD = 100;
    private static final String TABLE_QN_PREFIX = "bulk_table_" + getRandomName() + "_";

    private static final String REL_NAME_FROM = "userDefRelationshipFrom";
    private static final String REL_NAME_TO = "userDefRelationshipTo";


    private static final String KEY_REL_ATTRS = "relationshipAttributes";

    private static final String ATTR_TO_TYPE = "toTypeLabel";
    private static final String ATTR_FROM_TYPE = "fromTypeLabel";
    private static final String UD_RELATIONSHIP_TYPE_NAME = "UserDefRelationship";

    private static Map<String, Object> REL_ATTRS = new HashMap<String, Object>(){{
        put(ATTR_TO_TYPE, "classifiedBy");
        put(ATTR_FROM_TYPE, "isA");
    }};

    public static void main(String[] args) throws Exception {
        try {
            new CustomRelationships().run();
        } finally {
            runAsGod();
            cleanUpAll();
            //ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running CustomRelationships tests");

        long start = System.currentTimeMillis();
        try {
            runAsGod();
            createTables();

            createAndGet();
            createAndGetEmptyValue();

            //hitLimitWithBulkUpdateOverride();
            //Also covers HARD delete edge case when removing relationship

            /*hitLimitWithBulkUpdateOverrideInverse();

            hitLimitAppendRelationship();

            hitLimitAppendRelationshipInverse();

            hitLimitWithRelationshipAPI();

            deleteRelationshipBulkOverride();

            deleteRelationshipDeleteAsset();

            deleteRelationshipAssetInverse();

            deleteRelationshipWithRelationshipAPI();*/

            //Access control
            createConnection();

            createWithAdminUser();
            createWithMemberUser();
            createWithGuestUser();

            createRelationshipAccessControl();


        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running CustomRelationships tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void createAndGetEmptyValue() throws Exception {
        LOG.info(">> createAndGetEmptyValue");

        // send both null implicit
        // add only one relationship ATTR_TO_TYPE
        // add only one relationship ATTR_FROM_TYPE
        // send both explicit empty
        // send both null explicit
        // send without relationshipType


        Map<String, Object> REL_ATTRS_ = new HashMap<>();

        AtlasEntity table0 = getAtlasEntity(TYPE_TABLE, "table_0");
        String table0Guid = createEntity(table0).getCreatedEntities().get(0).getGuid();

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1");
        AtlasRelatedObjectId customRel = new AtlasRelatedObjectId();
        //AtlasObjectId customRel = new AtlasObjectId();
        customRel.setTypeName(TYPE_TABLE);
        customRel.setGuid(table0Guid);
        customRel.setRelationshipAttributes(new AtlasStruct(UD_RELATIONSHIP_TYPE_NAME, REL_ATTRS_));

        table1.setRelationshipAttribute(REL_NAME_FROM, Collections.singleton(customRel));

        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(2);

        table0 = getEntity(table0Guid);

        assertNotNull(table0.getRelationshipAttribute(REL_NAME_TO));

        List<Map<String, Object>> relations =  (List<Map<String, Object>>) table0.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(1, relations.size());
        assertNotNull(relations.get(0).get(KEY_REL_ATTRS));

        Map<String, Object> relAttributes = (Map<String, Object>) ((Map<String, Object>)relations.get(0).get(KEY_REL_ATTRS)).get("attributes");
        assertNotNull(relAttributes);
        assertNull(relAttributes.get(ATTR_TO_TYPE));
        assertNull(relAttributes.get(ATTR_FROM_TYPE));

        //// add only one relationship ATTR_TO_TYPE

        REL_ATTRS_.put(ATTR_TO_TYPE, "a");

        AtlasEntity table2 = getAtlasEntity(TYPE_TABLE, "table_2");
        customRel = new AtlasRelatedObjectId();
        //AtlasObjectId customRel = new AtlasObjectId();
        customRel.setTypeName(TYPE_TABLE);
        customRel.setGuid(table0Guid);
        customRel.setRelationshipAttributes(new AtlasStruct(UD_RELATIONSHIP_TYPE_NAME, REL_ATTRS_));

        table2.setRelationshipAttribute(REL_NAME_FROM, Collections.singleton(customRel));

        String table2Guid = createEntity(table2).getCreatedEntities().get(0).getGuid();
        sleep(2);

        table2 = getEntity(table2Guid);

        assertNotNull(table2.getRelationshipAttribute(REL_NAME_TO));

        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());
        assertNotNull(relations.get(0).get(KEY_REL_ATTRS));

        relAttributes = (Map<String, Object>) ((Map<String, Object>)relations.get(0).get(KEY_REL_ATTRS)).get("attributes");
        assertNotNull(relAttributes);
        assertEquals(relAttributes.get(ATTR_TO_TYPE), "A");
        assertNull(relAttributes.get(ATTR_FROM_TYPE));


        //// add only one relationship ATTR_FROM_TYPE

        REL_ATTRS_.put(ATTR_TO_TYPE, null);
        REL_ATTRS_.put(ATTR_FROM_TYPE, "b");

        AtlasEntity table3 = getAtlasEntity(TYPE_TABLE, "table_3");
        customRel = new AtlasRelatedObjectId();
        //AtlasObjectId customRel = new AtlasObjectId();
        customRel.setTypeName(TYPE_TABLE);
        customRel.setGuid(table0Guid);
        customRel.setRelationshipAttributes(new AtlasStruct(UD_RELATIONSHIP_TYPE_NAME, REL_ATTRS_));

        table3.setRelationshipAttribute(REL_NAME_FROM, Collections.singleton(customRel));

        String table3Guid = createEntity(table3).getCreatedEntities().get(0).getGuid();
        sleep(2);

        table3 = getEntity(table3Guid);

        assertNotNull(table3.getRelationshipAttribute(REL_NAME_TO));

        relations =  (List<Map<String, Object>>) table3.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());
        assertNotNull(relations.get(0).get(KEY_REL_ATTRS));

        relAttributes = (Map<String, Object>) ((Map<String, Object>) relations.get(0).get(KEY_REL_ATTRS)).get("attributes");
        assertNotNull(relAttributes);
        assertEquals(relAttributes.get(ATTR_FROM_TYPE), "B");
        assertNull(relAttributes.get(ATTR_TO_TYPE));


        // send both explicit empty

        REL_ATTRS_ = new HashMap<>();
        REL_ATTRS_.put(ATTR_TO_TYPE, "");
        REL_ATTRS_.put(ATTR_FROM_TYPE, "");

        AtlasEntity table4 = getAtlasEntity(TYPE_TABLE, "table_4");
        customRel = new AtlasRelatedObjectId();
        //AtlasObjectId customRel = new AtlasObjectId();
        customRel.setTypeName(TYPE_TABLE);
        customRel.setGuid(table0Guid);
        customRel.setRelationshipAttributes(new AtlasStruct(UD_RELATIONSHIP_TYPE_NAME, REL_ATTRS_));

        table4.setRelationshipAttribute(REL_NAME_FROM, Collections.singleton(customRel));

        String table4Guid = createEntity(table4).getCreatedEntities().get(0).getGuid();
        sleep(2);

        table4 = getEntity(table4Guid);

        assertNotNull(table4.getRelationshipAttribute(REL_NAME_TO));

        relations =  (List<Map<String, Object>>) table4.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());
        assertNotNull(relations.get(0).get(KEY_REL_ATTRS));

        relAttributes = (Map<String, Object>) ((Map<String, Object>)relations.get(0).get(KEY_REL_ATTRS)).get("attributes");
        assertNotNull(relAttributes);
        assertEquals(relAttributes.get(ATTR_TO_TYPE), "");
        assertEquals(relAttributes.get(ATTR_FROM_TYPE), "");


        // send both null explicit

        REL_ATTRS_ = new HashMap<>();
        REL_ATTRS_.put(ATTR_TO_TYPE, null);
        REL_ATTRS_.put(ATTR_FROM_TYPE, null);

        AtlasEntity table5 = getAtlasEntity(TYPE_TABLE, "table_5");
        customRel = new AtlasRelatedObjectId();
        //AtlasObjectId customRel = new AtlasObjectId();
        customRel.setTypeName(TYPE_TABLE);
        customRel.setGuid(table0Guid);
        customRel.setRelationshipAttributes(new AtlasStruct(UD_RELATIONSHIP_TYPE_NAME, REL_ATTRS_));

        table5.setRelationshipAttribute(REL_NAME_FROM, Collections.singleton(customRel));

        String table5Guid = createEntity(table5).getCreatedEntities().get(0).getGuid();
        sleep(2);

        table5 = getEntity(table5Guid);

        assertNotNull(table5.getRelationshipAttribute(REL_NAME_TO));

        relations =  (List<Map<String, Object>>) table5.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());
        assertNotNull(relations.get(0).get(KEY_REL_ATTRS));

        relAttributes = (Map<String, Object>) ((Map<String, Object>)relations.get(0).get(KEY_REL_ATTRS)).get("attributes");
        assertNotNull(relAttributes);
        assertNull(relAttributes.get(ATTR_TO_TYPE));
        assertNull(relAttributes.get(ATTR_FROM_TYPE));

        // send without relationshipType

        REL_ATTRS_ = new HashMap<>();
        REL_ATTRS_.put(ATTR_TO_TYPE, "a");
        REL_ATTRS_.put(ATTR_FROM_TYPE, "b");

        AtlasEntity table6 = getAtlasEntity(TYPE_TABLE, "table_6");
        customRel = new AtlasRelatedObjectId();
        //AtlasObjectId customRel = new AtlasObjectId();
        customRel.setTypeName(TYPE_TABLE);
        customRel.setGuid(table0Guid);
        customRel.setRelationshipAttributes(new AtlasStruct("", REL_ATTRS_));

        table6.setRelationshipAttribute(REL_NAME_FROM, Collections.singleton(customRel));

        String table6Guid = createEntity(table6).getCreatedEntities().get(0).getGuid();
        sleep(2);

        table6 = getEntity(table6Guid);

        assertNotNull(table6.getRelationshipAttribute(REL_NAME_TO));

        relations =  (List<Map<String, Object>>) table6.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());
        assertNotNull(relations.get(0).get(KEY_REL_ATTRS));

        relAttributes = (Map<String, Object>) ((Map<String, Object>)relations.get(0).get(KEY_REL_ATTRS)).get("attributes");
        assertNotNull(relAttributes);
        assertEquals(relAttributes.get(ATTR_TO_TYPE), "A");
        assertEquals(relAttributes.get(ATTR_FROM_TYPE), "B");

        LOG.info(">> createAndGetEmptyValue");
    }

    private static void createAndGet() throws Exception {
        LOG.info(">> createAndGet");

        AtlasEntity table0 = getAtlasEntity(TYPE_TABLE, "table_0");
        String table0Guid = createEntity(table0).getCreatedEntities().get(0).getGuid();

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1");
        AtlasRelatedObjectId customRel = new AtlasRelatedObjectId();
        //AtlasObjectId customRel = new AtlasObjectId();
        customRel.setTypeName(TYPE_TABLE);
        customRel.setGuid(table0Guid);
        customRel.setRelationshipAttributes(new AtlasStruct(UD_RELATIONSHIP_TYPE_NAME, REL_ATTRS));

        table1.setRelationshipAttribute(REL_NAME_FROM, Collections.singleton(customRel));

        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(2);

        assertWithIndexSearch(table0Guid, table1Guid);
        assertWithGET(table0Guid, table1Guid);


        LOG.info(">> createAndGet");
    }

    private static void createTables() throws Exception {
        LOG.info(">> createTables");

        AtlasEntity.AtlasEntitiesWithExtInfo extInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        int i = 0;
        while (i < RELATIONSHIP_THRESHOLD) {
            AtlasEntity table = getAtlasEntity(TYPE_TABLE, TABLE_QN_PREFIX + i);
            table.setAttribute(QUALIFIED_NAME, TABLE_QN_PREFIX + i);

            extInfo.addEntity(table);
            i++;
        }

        assertEquals(RELATIONSHIP_THRESHOLD, createEntitiesBulk(extInfo).getCreatedEntities().size());

        LOG.info("<< createTables");
    }

    private static void createConnection() throws Exception {
        LOG.info(">> createConnection");

        AtlasEntity connection = getAtlasEntity("Connection", "connection_0" + getRandomName());
        connection.setAttribute(QUALIFIED_NAME, "default/redshift/" + getRandomName());
        connection.setAttribute("adminUsers", Arrays.asList("nikhil.bonte", "nikhil.member", "nikhil.guest"));
        String connectionGuid = createEntity(connection).getCreatedEntities().get(0).getGuid();
        sleep(60);
        CONNECTION = getEntity(connectionGuid);

        LOG.info("<< createConnection");
    }

    private static void hitLimitWithBulkUpdateOverride() throws Exception {
        LOG.info(">> hitLimitWithBulkUpdateOverride");
        //Also covers HARD delete edge case when removing relationship

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        List<AtlasRelatedObjectId> customRelations = new ArrayList<>();

        int i = 0;
        while (i < RELATIONSHIP_THRESHOLD) {
            customRelations.add(getUDRelationshipWithAttributes(TYPE_TABLE, TABLE_QN_PREFIX + i));
            i++;
        }
        table1.setRelationshipAttribute(REL_NAME_FROM, customRelations);

        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(2);

        table1 = getEntity(table1Guid);

        List<Map<String, Object>> relations;

        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(RELATIONSHIP_THRESHOLD, relations.size());


        AtlasEntity table101 = getAtlasEntity(TYPE_TABLE, "on-demand_table_0" + getRandomName());
        String table101Guid = createEntity(table101).getCreatedEntities().get(0).getGuid();
        sleep(2);

        customRelations.add(getUDRelationshipWithAttributes(TYPE_TABLE, table101.getAttribute(QUALIFIED_NAME).toString()
        ));

        assertEquals(101, customRelations.size());

        table1.setRelationshipAttribute(REL_NAME_FROM, customRelations);

        boolean failed = false;
        try {
            createEntity(table1);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains("Operation not supported: Custom relationships size is more than 100, current is 101 for " + table1.getAttribute(NAME)));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }


        customRelations.remove(0);
        assertEquals(100, customRelations.size());
        table1.setRelationshipAttribute(REL_NAME_FROM, customRelations);

        sleep(2);
        createEntity(table1);
        sleep(5);

        table1 = getEntity(table1Guid);

        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(RELATIONSHIP_THRESHOLD, relations.size());

        LOG.info(">> hitLimitWithBulkUpdateOverride");
    }

    private static void hitLimitWithBulkUpdateOverrideInverse() throws Exception {
        LOG.info(">> hitLimitWithBulkUpdateOverrideInverse");
        //Also covers HARD delete edge case when removing relationship

        // table1     - from  - 100
        // on_demand_table_0 < to > table1    - 1
        // resulting table1.from to exceed 101

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        List<AtlasRelatedObjectId> customRelations = new ArrayList<>();

        int i = 0;
        while (i < RELATIONSHIP_THRESHOLD) {
            customRelations.add(getUDRelationshipWithAttributes(TYPE_TABLE, TABLE_QN_PREFIX + i));
            i++;
        }
        table1.setRelationshipAttribute(REL_NAME_FROM, customRelations);

        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(20);
        LOG.info("table1Guid {}", table1Guid);

        table1 = getEntity(table1Guid);

        AtlasEntity table101 = getAtlasEntity(TYPE_TABLE, "on_demand_table_0" + getRandomName());
        //table101.setAttribute(QUALIFIED_NAME, "on_demand_table_0");
        table101.setRelationshipAttribute(REL_NAME_TO, Arrays.asList(getUDRelationshipWithAttributes(TYPE_TABLE, (String) table1.getAttribute(QUALIFIED_NAME)
        )));

        EntityMutationResponse response;
        boolean failed = false;
        try {
            response = createEntity(table101);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains("Operation not supported: Custom relationships size is more than 100, current is 101 for " + table1.getAttribute(NAME)));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        //------------------

        AtlasEntity table2 = getAtlasEntity(TYPE_TABLE, "table_2" + getRandomName());
        table2.setRelationshipAttribute(REL_NAME_TO, customRelations);

        String table2Guid = createEntity(table2).getCreatedEntities().get(0).getGuid();
        sleep(2);
        LOG.info("table2Guid {}", table2Guid);

        table2 = getEntity(table2Guid);

        AtlasEntity table102 = getAtlasEntity(TYPE_TABLE, "on_demand_table_102" + getRandomName());
        table102.setAttribute(QUALIFIED_NAME, "on_demand_table_102");
        table102.setRelationshipAttribute(REL_NAME_FROM, Arrays.asList(getUDRelationshipWithAttributes(TYPE_TABLE, (String) table2.getAttribute(QUALIFIED_NAME)
        )));

        failed = false;
        try {
            createEntity(table102);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains("Operation not supported: Custom relationships size is more than 100, current is 101 for " + table2.getAttribute(NAME)));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        LOG.info(">> hitLimitWithBulkUpdateOverrideInverse");
    }

    private static void hitLimitAppendRelationship() throws Exception {
        LOG.info(">> hitLimitAppendRelationship");

        //append
        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        List<AtlasRelatedObjectId> customRelations = new ArrayList<>();

        int i = 0;
        while (i < RELATIONSHIP_THRESHOLD) {
            customRelations.add(getUDRelationshipWithAttributes(TYPE_TABLE, TABLE_QN_PREFIX + i));
            i++;
        }
        table1.setRelationshipAttribute(REL_NAME_FROM, customRelations);

        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(2);

        table1 = getEntity(table1Guid);


        AtlasEntity table101 = getAtlasEntity(TYPE_TABLE, "on-demand_table_101" + getRandomName());
        createEntity(table101).getCreatedEntities().get(0).getGuid();
        sleep(1);

        AtlasEntity table1_copy = new AtlasEntity(TYPE_TABLE);
        table1_copy.setAttribute(NAME, table1.getAttribute(NAME));
        table1_copy.setAttribute(QUALIFIED_NAME, table1.getAttribute(QUALIFIED_NAME));
        table1_copy.setAppendRelationshipAttribute(REL_NAME_FROM, Arrays.asList(getUDRelationshipWithAttributes(TYPE_TABLE, table101.getAttribute(QUALIFIED_NAME).toString()
        )));

        boolean failed = false;
        try {
            createEntity(table1_copy);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains("Operation not supported: Custom relationships size is more than 100, current is 101 for " + table1.getAttribute(NAME)));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        sleep(1);
        table1 = getEntity(table1Guid);

        List<Map<String, Object>> relations;
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(RELATIONSHIP_THRESHOLD, relations.size());

        LOG.info(">> hitLimitAppendRelationship");
    }

    private static void hitLimitAppendRelationshipInverse() throws Exception {
        LOG.info(">> hitLimitAppendRelationshipInverse");

        List<AtlasRelatedObjectId> customRelations = new ArrayList<>();
        int i = 0;
        while (i < RELATIONSHIP_THRESHOLD) {
            customRelations.add(getUDRelationshipWithAttributes(TYPE_TABLE, TABLE_QN_PREFIX + i));
            i++;
        }

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        table1.setRelationshipAttribute(REL_NAME_FROM, customRelations);
        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table1 = getEntity(table1Guid);


        AtlasEntity table101 = getAtlasEntity(TYPE_TABLE, "on-demand_table_101" + getRandomName());
        table101.setAttribute(QUALIFIED_NAME, "on-demand_table_101" + getRandomName());
        table101.setAppendRelationshipAttribute(REL_NAME_TO, Arrays.asList(getUDRelationshipWithAttributes(TYPE_TABLE, table1.getAttribute(QUALIFIED_NAME).toString()
        )));

        boolean failed = false;
        try {
            createEntity(table101);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains("Operation not supported: Custom relationships size is more than 100, current is 101 for " + table1.getAttribute(NAME)));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        //-----------------

        AtlasEntity table2 = getAtlasEntity(TYPE_TABLE, "table2" + getRandomName());
        table2.setRelationshipAttribute(REL_NAME_TO, customRelations);
        String table2Guid = createEntity(table2).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table2 = getEntity(table2Guid);


        AtlasEntity table102 = getAtlasEntity(TYPE_TABLE, "on-demand_table_102" + getRandomName());
        table102.setAttribute(QUALIFIED_NAME, "on-demand_table_102" + getRandomName());
        table102.setAppendRelationshipAttribute(REL_NAME_FROM, Arrays.asList(getUDRelationshipWithAttributes(TYPE_TABLE, table2.getAttribute(QUALIFIED_NAME).toString()
        )));
        sleep(1);

        failed = false;
        try {
            createEntity(table102);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains("Operation not supported: Custom relationships size is more than 100, current is 101 for " + table2.getAttribute(NAME)));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        LOG.info(">> hitLimitAppendRelationshipInverse");
    }

    private static void hitLimitWithRelationshipAPI() throws Exception {
        LOG.info(">> hitLimitWithRelationshipAPI");

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table1 = getEntity(table1Guid);

        List<AtlasRelationship> relationships = new ArrayList<>();
        int i = 0;
        while (i < RELATIONSHIP_THRESHOLD) {
            AtlasRelationship relationship = new AtlasRelationship();
            relationship.setTypeName(UD_RELATIONSHIP_TYPE_NAME);
            relationship.setEnd1(new AtlasObjectId(TYPE_TABLE, mapOf(QUALIFIED_NAME, table1.getAttribute(QUALIFIED_NAME))));
            relationship.setEnd2(new AtlasObjectId(TYPE_TABLE, mapOf(QUALIFIED_NAME, TABLE_QN_PREFIX + i)));
            relationship.setAttributes(REL_ATTRS);

            relationships.add(relationship);
            i++;
        }

        assertEquals(100, createRelationships(relationships).size());

        AtlasEntity table2 = getAtlasEntity(TYPE_TABLE, "on-demand_table_2" + getRandomName());
        table2.setAttribute(QUALIFIED_NAME, "on-demand_table_2" + getRandomName());
        String table2Guid = createEntity(table2).getCreatedEntities().get(0).getGuid();
        sleep(2);

        AtlasRelationship relationship = new AtlasRelationship();
        relationship.setTypeName(UD_RELATIONSHIP_TYPE_NAME);
        relationship.setEnd1(new AtlasObjectId(TYPE_TABLE, mapOf(QUALIFIED_NAME, table1.getAttribute(QUALIFIED_NAME))));
        relationship.setEnd2(new AtlasObjectId(TYPE_TABLE, mapOf(QUALIFIED_NAME, table2.getAttribute(QUALIFIED_NAME))));
        relationship.setAttributes(REL_ATTRS);

        boolean failed = false;
        try {
            createRelationship(relationship);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains("Operation not supported: Custom relationships size is more than 100, current is 101 for " + table1.getAttribute(QUALIFIED_NAME)));
            failed = true;
        } finally {
            if (!failed) {
                throw new CustomException("This test should have failed");
            }
        }

        //------------------

        AtlasEntity table3 = getAtlasEntity(TYPE_TABLE, "table_3" + getRandomName());
        String table3Guid = createEntity(table3).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table3 = getEntity(table3Guid);

        relationships = new ArrayList<>();
        i = 0;
        while (i < RELATIONSHIP_THRESHOLD) {
            relationship = new AtlasRelationship();
            relationship.setTypeName(UD_RELATIONSHIP_TYPE_NAME);
            relationship.setEnd1(new AtlasObjectId(TYPE_TABLE, mapOf(QUALIFIED_NAME, TABLE_QN_PREFIX + i)));
            relationship.setEnd2(new AtlasObjectId(TYPE_TABLE, mapOf(QUALIFIED_NAME, table3.getAttribute(QUALIFIED_NAME))));
            relationship.setAttributes(REL_ATTRS);

            relationships.add(relationship);
            i++;
        }

        assertEquals(100, createRelationships(relationships).size());

        relationship = new AtlasRelationship();
        relationship.setTypeName(UD_RELATIONSHIP_TYPE_NAME);
        relationship.setEnd1(new AtlasObjectId(TYPE_TABLE, mapOf(QUALIFIED_NAME, table2.getAttribute(QUALIFIED_NAME))));
        relationship.setEnd2(new AtlasObjectId(TYPE_TABLE, mapOf(QUALIFIED_NAME, table3.getAttribute(QUALIFIED_NAME))));
        relationship.setAttributes(REL_ATTRS);

        failed = false;
        try {
            createRelationship(relationship);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains("Operation not supported: Custom relationships size is more than 100, current is 101 for " + table3.getAttribute(QUALIFIED_NAME)));
            failed = true;
        } finally {
            if (!failed) {
                throw new CustomException("This test should have failed");
            }
        }

        LOG.info(">> hitLimitWithRelationshipAPI");
    }

    private static void deleteRelationshipBulkOverride() throws Exception {
        LOG.info(">> deleteRelationshipBulkOverride");

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        table1.setRelationshipAttribute(REL_NAME_FROM, Arrays.asList(
                getUDRelationshipWithAttributes(TYPE_TABLE, TABLE_QN_PREFIX + 1),
                getUDRelationshipWithAttributes(TYPE_TABLE, TABLE_QN_PREFIX + 2)
        ));
        table1.setRelationshipAttribute(REL_NAME_TO, Arrays.asList(
                getUDRelationshipWithAttributes(TYPE_TABLE, TABLE_QN_PREFIX + 1),
                getUDRelationshipWithAttributes(TYPE_TABLE, TABLE_QN_PREFIX + 3)
        ));
        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table1 = getEntity(table1Guid);

        //---------------
        table1.setRelationshipAttribute(REL_NAME_FROM, null);
        table1.setRelationshipAttribute(REL_NAME_TO, null);
        createEntity(table1);
        sleep(2);
        table1 = getEntity(table1Guid);

        List<Map<String, Object>> relations;
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());


        //---------------

        table1.setRelationshipAttribute(REL_NAME_FROM, Arrays.asList(
                getUDRelationshipWithAttributes(TYPE_TABLE, TABLE_QN_PREFIX + 1),
                getUDRelationshipWithAttributes(TYPE_TABLE, TABLE_QN_PREFIX + 2)
        ));
        table1.setRelationshipAttribute(REL_NAME_TO, Arrays.asList(
                getUDRelationshipWithAttributes(TYPE_TABLE, TABLE_QN_PREFIX + 1),
                getUDRelationshipWithAttributes(TYPE_TABLE, TABLE_QN_PREFIX + 3)
        ));
        createEntity(table1);
        sleep(2);
        table1 = getEntity(table1Guid);

        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(2, relations.size());
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(2, relations.size());

        //---------------
        table1.setRelationshipAttribute(REL_NAME_FROM, new ArrayList<>());
        table1.setRelationshipAttribute(REL_NAME_TO, new ArrayList<>());
        createEntity(table1);
        sleep(2);
        table1 = getEntity(table1Guid);

        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());


        //---------------


        LOG.info("<< deleteRelationshipBulkOverride");
    }

    private static void deleteRelationshipDeleteAsset() throws Exception {
        LOG.info(">> deleteRelationshipDeleteAsset");

        List<Map<String, Object>> relations;

        AtlasEntity table0 = getAtlasEntity(TYPE_TABLE, "table_0" + getRandomName());
        AtlasEntity table2 = getAtlasEntity(TYPE_TABLE, "table_2" + getRandomName());
        String table0Guid = createEntity(table0).getCreatedEntities().get(0).getGuid();
        String table2Guid = createEntity(table2).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table0 = getEntity(table0Guid);
        table2 = getEntity(table2Guid);

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        table1.setRelationshipAttribute(REL_NAME_FROM, Arrays.asList(
                getUDRelationshipWithAttributes(TYPE_TABLE, table0.getAttribute(QUALIFIED_NAME).toString())
        ));
        table1.setRelationshipAttribute(REL_NAME_TO, Arrays.asList(
                getUDRelationshipWithAttributes(TYPE_TABLE, table2.getAttribute(QUALIFIED_NAME).toString())
        ));
        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(2);


        //---------------
        deleteEntitySoft(table1Guid);
        sleep(2);
        table1 = getEntity(table1Guid);
        table0 = getEntity(table0Guid);
        table2 = getEntity(table2Guid);

        assertEquals("DELETED", table1.getStatus().name());
        assertEquals("ACTIVE", table0.getStatus().name());
        assertEquals("ACTIVE", table2.getStatus().name());

        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(1, relations.size());
        assertEquals("ACTIVE", relations.get(0).get("relationshipStatus"));
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table0.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table0.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());
        assertEquals("ACTIVE", relations.get(0).get("relationshipStatus"));


        //---------------
        deleteEntitySoft(table0Guid);
        sleep(2);
        table1 = getEntity(table1Guid);
        table0 = getEntity(table0Guid);
        table2 = getEntity(table2Guid);

        assertEquals("DELETED", table0.getStatus().name());
        assertEquals("DELETED", table1.getStatus().name());
        assertEquals("ACTIVE", table2.getStatus().name());

        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(1, relations.size());
        assertEquals("ACTIVE", relations.get(0).get("relationshipStatus"));
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table0.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table0.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());
        assertEquals("ACTIVE", relations.get(0).get("relationshipStatus"));

        //---------------
        deleteEntitySoft(table2Guid);
        sleep(2);
        table1 = getEntity(table1Guid);
        table0 = getEntity(table0Guid);
        table2 = getEntity(table2Guid);

        assertEquals("DELETED", table0.getStatus().name());
        assertEquals("DELETED", table1.getStatus().name());
        assertEquals("DELETED", table2.getStatus().name());

        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table0.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table0.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        LOG.info("<< deleteRelationshipDeleteAsset");
    }

    private static void deleteRelationshipAssetInverse() throws Exception {
        LOG.info(">> deleteRelationshipAssetInverse");

        List<Map<String, Object>> relations;

        AtlasEntity table0 = getAtlasEntity(TYPE_TABLE, "table_0" + getRandomName());
        AtlasEntity table2 = getAtlasEntity(TYPE_TABLE, "table_2" + getRandomName());
        String table0Guid = createEntity(table0).getCreatedEntities().get(0).getGuid();
        String table2Guid = createEntity(table2).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table0 = getEntity(table0Guid);
        table2 = getEntity(table2Guid);

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        table1.setRelationshipAttribute(REL_NAME_FROM, Arrays.asList(
                getUDRelationshipWithAttributes(TYPE_TABLE, table0.getAttribute(QUALIFIED_NAME).toString())
        ));
        table1.setRelationshipAttribute(REL_NAME_TO, Arrays.asList(
                getUDRelationshipWithAttributes(TYPE_TABLE, table2.getAttribute(QUALIFIED_NAME).toString())
        ));
        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(2);


        //---------------
        deleteEntitySoft(table0Guid);
        sleep(2);
        table1 = getEntity(table1Guid);
        table0 = getEntity(table0Guid);
        table2 = getEntity(table2Guid);

        assertEquals("DELETED", table0.getStatus().name());
        assertEquals("ACTIVE", table1.getStatus().name());
        assertEquals("ACTIVE", table2.getStatus().name());

        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(1, relations.size());
        assertEquals("ACTIVE", relations.get(0).get("relationshipStatus"));
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());
        assertEquals("ACTIVE", relations.get(0).get("relationshipStatus"));

        relations =  (List<Map<String, Object>>) table0.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(1, relations.size());
        relations =  (List<Map<String, Object>>) table0.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());
        assertEquals("ACTIVE", relations.get(0).get("relationshipStatus"));

        //---------------
        deleteEntitySoft(table2Guid);
        sleep(2);
        table1 = getEntity(table1Guid);
        table0 = getEntity(table0Guid);
        table2 = getEntity(table2Guid);

        assertEquals("DELETED", table0.getStatus().name());
        assertEquals("ACTIVE", table1.getStatus().name());
        assertEquals("DELETED", table2.getStatus().name());

        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());

        relations =  (List<Map<String, Object>>) table0.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(1, relations.size());
        relations =  (List<Map<String, Object>>) table0.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        LOG.info(">> deleteRelationshipAssetInverse");
    }

    private static void deleteRelationshipWithRelationshipAPI() throws Exception {
        LOG.info(">> deleteRelationshipWithRelationshipAPI");
        List<Map<String, Object>> relations;

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        AtlasEntity table2 = getAtlasEntity(TYPE_TABLE, "table_2" + getRandomName());
        AtlasEntity table3 = getAtlasEntity(TYPE_TABLE, "table_3" + getRandomName());
        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        String table2Guid = createEntity(table2).getCreatedEntities().get(0).getGuid();
        String table3Guid = createEntity(table3).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table1 = getEntity(table1Guid);
        table2 = getEntity(table2Guid);
        table3 = getEntity(table3Guid);

        AtlasRelationship relationship_0 = new AtlasRelationship();
        relationship_0.setTypeName(UD_RELATIONSHIP_TYPE_NAME);
        relationship_0.setEnd1(new AtlasObjectId(TYPE_TABLE, mapOf(QUALIFIED_NAME, table1.getAttribute(QUALIFIED_NAME))));
        relationship_0.setEnd2(new AtlasObjectId(TYPE_TABLE, mapOf(QUALIFIED_NAME, table2.getAttribute(QUALIFIED_NAME))));
        relationship_0.setAttributes(REL_ATTRS);

        AtlasRelationship relationship_1 = new AtlasRelationship();
        relationship_1.setTypeName(UD_RELATIONSHIP_TYPE_NAME);
        relationship_1.setEnd1(new AtlasObjectId(TYPE_TABLE, mapOf(QUALIFIED_NAME, table1.getAttribute(QUALIFIED_NAME))));
        relationship_1.setEnd2(new AtlasObjectId(TYPE_TABLE, mapOf(QUALIFIED_NAME, table3.getAttribute(QUALIFIED_NAME))));
        relationship_1.setAttributes(REL_ATTRS);

        String relationship_0Guid = createRelationship(relationship_0).getGuid();
        String relationship_1Guid = createRelationship(relationship_1).getGuid();
        sleep(2);

        //-------------
        deleteRelationshipByGuid(relationship_0Guid);
        sleep(2);
        table1 = getEntity(table1Guid);
        table2 = getEntity(table2Guid);
        table3 = getEntity(table3Guid);

        assertEquals("ACTIVE", table1.getStatus().name());
        assertEquals("ACTIVE", table2.getStatus().name());
        assertEquals("ACTIVE", table3.getStatus().name());

        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(1, relations.size());
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table3.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table3.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());

        //-------------
        deleteRelationshipByGuid(relationship_1Guid);
        sleep(2);
        table1 = getEntity(table1Guid);
        table2 = getEntity(table2Guid);
        table3 = getEntity(table3Guid);

        assertEquals("ACTIVE", table1.getStatus().name());
        assertEquals("ACTIVE", table2.getStatus().name());
        assertEquals("ACTIVE", table3.getStatus().name());

        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table3.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table3.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        LOG.info(">> deleteRelationshipWithRelationshipAPI");
    }

    private static void createWithAdminUser() throws Exception {
        LOG.info(">> createWithAdminUser");

        runAsGod();

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        table1.setAttribute(QUALIFIED_NAME, CONNECTION.getAttribute(QUALIFIED_NAME) + "/table_1" + getRandomName());
        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table1 = getEntity(table1Guid);


        AtlasEntity table2 = getAtlasEntity(TYPE_TABLE, "table_2" + getRandomName());
        table2.setAttribute(QUALIFIED_NAME, CONNECTION.getAttribute(QUALIFIED_NAME) + "/table_2" + getRandomName());
        table2.setRelationshipAttribute(REL_NAME_FROM, Arrays.asList(
                getUDRelationshipWithAttributes(TYPE_TABLE, table1.getAttribute(QUALIFIED_NAME).toString()))
        );

        runAsAdmin();

        String table2Guid = createEntity(table2).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table2 = getEntity(table2Guid);
        table1 = getEntity(table1Guid);

        List<Map<String, Object>> relations;
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(1, relations.size());
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());

        LOG.info(">> createWithAdminUser");
    }

    private static void createWithMemberUser() throws Exception {
        LOG.info(">> createWithMemberUser");
        runAsGod();

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        table1.setAttribute(QUALIFIED_NAME, CONNECTION.getAttribute(QUALIFIED_NAME) + "/table_1" + getRandomName());
        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table1 = getEntity(table1Guid);


        AtlasEntity table2 = getAtlasEntity(TYPE_TABLE, "table_2" + getRandomName());
        table2.setAttribute(QUALIFIED_NAME, CONNECTION.getAttribute(QUALIFIED_NAME) + "/table_2" + getRandomName());
        table2.setRelationshipAttribute(REL_NAME_FROM, Arrays.asList(
                getUDRelationshipWithAttributes(TYPE_TABLE, table1.getAttribute(QUALIFIED_NAME).toString()))
        );

        runAsMember();

        String table2Guid = createEntity(table2).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table2 = getEntity(table2Guid);
        table1 = getEntity(table1Guid);

        List<Map<String, Object>> relations;
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(1, relations.size());
        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(0, relations.size());

        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(0, relations.size());
        relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());
        LOG.info(">> createWithMemberUser");
    }

    private static void createWithGuestUser() throws Exception {
        LOG.info(">> createWithGuestUser");
        runAsGod();

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        table1.setAttribute(QUALIFIED_NAME, CONNECTION.getAttribute(QUALIFIED_NAME) + "/table_1" + getRandomName());
        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table1 = getEntity(table1Guid);


        AtlasEntity table2 = getAtlasEntity(TYPE_TABLE, "table_2" + getRandomName());
        table2.setAttribute(QUALIFIED_NAME, CONNECTION.getAttribute(QUALIFIED_NAME) + "/table_2" + getRandomName());
        table2.setRelationshipAttribute(REL_NAME_FROM, Arrays.asList(
                getUDRelationshipWithAttributes(TYPE_TABLE, table1.getAttribute(QUALIFIED_NAME).toString()))
        );

        runAsGuest();

        boolean failed = false;
        try {
            createEntity(table2);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 403);
            assertTrue(exception.getMessage().contains("403"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        LOG.info(">> createWithGuestUser");
    }

    private static void createRelationshipAccessControl() throws Exception {
        LOG.info(">> createRelationshipAccessControl");

        runAsGod();

        AtlasEntity table1 = getAtlasEntity(TYPE_TABLE, "table_1" + getRandomName());
        AtlasEntity table2 = getAtlasEntity(TYPE_TABLE, "table_2" + getRandomName());

        String table1Guid = createEntity(table1).getCreatedEntities().get(0).getGuid();
        String table2Guid = createEntity(table2).getCreatedEntities().get(0).getGuid();
        sleep(2);
        table1 = getEntity(table1Guid);
        table2 = getEntity(table2Guid);

        AtlasRelationship relationship = new AtlasRelationship(UD_RELATIONSHIP_TYPE_NAME);
        relationship.setEnd1(new AtlasObjectId(table1Guid, TYPE_TABLE));
        relationship.setEnd2(new AtlasObjectId(table2Guid, TYPE_TABLE));
        relationship.setAttributes(REL_ATTRS);

        runAsGuest();
        checkRelationshipPermission(relationship, false);

        runAsMember();
        checkRelationshipPermission(relationship, false);

        runAsAdmin();
        checkRelationshipPermission(relationship, true);

        LOG.info("<< createRelationshipAccessControl");
    }

    private static void checkRelationshipPermission(AtlasRelationship relationship, boolean allowed) throws Exception {
        if (allowed) {
            createRelationship(relationship);
            sleep(2);
            runAsGod();
            AtlasEntity table1 = getEntity(relationship.getEnd1().getGuid());
            AtlasEntity table2 = getEntity(relationship.getEnd2().getGuid());

            List<Map<String, Object>> relations;
            relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_TO);
            assertEquals(1, relations.size());
            relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
            assertEquals(0, relations.size());

            relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_TO);
            assertEquals(0, relations.size());
            relations =  (List<Map<String, Object>>) table2.getRelationshipAttribute(REL_NAME_FROM);
            assertEquals(1, relations.size());
        } else {
            boolean failed = false;
            try {
                createRelationship(relationship);
            } catch (Exception exception) {
                //assertEquals(exception.getStatus().getStatusCode(), 403);
                assertTrue(exception.getMessage().contains("403"));
                failed = true;
            } finally {
                if (!failed) {
                    throw new Exception("This test should have failed");
                }
            }
        }
    }

    private static void assertWithIndexSearch(String table0Guid, String table1Guid) throws Exception {
        IndexSearchParams indexSearchParams = new IndexSearchParams();
        Map<String, Object> dsl = mapOf("query", mapOf("match", mapOf("__guid", table0Guid)));
        dsl.put("size", 1);

        indexSearchParams.setDsl(dsl);
        indexSearchParams.setAttributes(new HashSet<>(Arrays.asList(REL_NAME_FROM, REL_NAME_TO)));
        indexSearchParams.setRelationAttributes(new HashSet<>(Arrays.asList(NAME)));

        //indexSearchParams.setRequestMetadata(null);
        AtlasSearchResult result = indexSearch(indexSearchParams);

        assertNotNull(result);
        assertNotNull(result.getEntities());
        assertEquals(1, result.getEntities().size());

        AtlasEntityHeader entityHeader = result.getEntities().get(0);
        assertNotNull(entityHeader);
        assertTrue(CollectionUtils.isEmpty( (List) entityHeader.getAttribute(REL_NAME_FROM)));
        assertNotNull(entityHeader.getAttribute(REL_NAME_TO));

        Map<String, Object> attributes =  (Map<String, Object>)((List<Map<String, Object>>) entityHeader.getAttribute(REL_NAME_TO)).get(0).get("attributes");
        assertNotNull(attributes);

        List<Map<String, Object>> relationshipAttributes =  (List<Map<String, Object>>) attributes.get("relationshipAttributes");
        assertNull(relationshipAttributes);

        //--------------

        indexSearchParams.setIncludeRelationshipAttributes(true);
        result = indexSearch(indexSearchParams);

        entityHeader = result.getEntities().get(0);
        assertNotNull(entityHeader);
        assertTrue(CollectionUtils.isEmpty( (List) entityHeader.getAttribute(REL_NAME_FROM)));
        assertNotNull(entityHeader.getAttribute(REL_NAME_TO));

        attributes =  (Map<String, Object>)((List<Map<String, Object>>) entityHeader.getAttribute(REL_NAME_TO)).get(0).get("attributes");
        assertNotNull(attributes);

        Map<String, Object> attributesOnRelationship =  (Map<String, Object>) attributes.get("relationshipAttributes");
        assertNotNull(attributesOnRelationship);
        assertEquals(2, attributesOnRelationship.size());
        assertEquals(attributesOnRelationship.get(ATTR_TO_TYPE), "classifiedBy");
        assertEquals(attributesOnRelationship.get(ATTR_FROM_TYPE), "isA");


        //-------------

        dsl = mapOf("query", mapOf("match", mapOf("__guid", table1Guid)));
        indexSearchParams.setDsl(dsl);
        indexSearchParams.setAttributes(new HashSet<>(Arrays.asList(REL_NAME_FROM, REL_NAME_TO)));
        indexSearchParams.setRelationAttributes(new HashSet<>(Arrays.asList(NAME)));

        result = indexSearch(indexSearchParams);

        entityHeader = result.getEntities().get(0);
        assertNotNull(entityHeader);
        assertTrue(CollectionUtils.isEmpty( (List) entityHeader.getAttribute(REL_NAME_TO)));
        assertNotNull(entityHeader.getAttribute(REL_NAME_FROM));

        attributes =  (Map<String, Object>)((List<Map<String, Object>>) entityHeader.getAttribute(REL_NAME_FROM)).get(0).get("attributes");
        assertNotNull(attributes);

        attributesOnRelationship =  (Map<String, Object>) attributes.get("relationshipAttributes");
        assertNotNull(attributesOnRelationship);
        assertEquals(2, attributesOnRelationship.size());
        assertEquals(attributesOnRelationship.get(ATTR_TO_TYPE), "classifiedBy");
        assertEquals(attributesOnRelationship.get(ATTR_FROM_TYPE), "isA");
    }

    private static void assertWithGET(String table0Guid, String table1Guid) throws Exception {
        AtlasEntity table0 = getEntity(table0Guid);
        AtlasEntity table1 = getEntity(table1Guid);

        assertNotNull(table0.getRelationshipAttribute(REL_NAME_TO));

        List<Map<String, Object>> relations =  (List<Map<String, Object>>) table0.getRelationshipAttribute(REL_NAME_TO);
        assertEquals(1, relations.size());
        assertNotNull(relations.get(0).get(KEY_REL_ATTRS));

        Map<String, Object> relAttributes = (Map<String, Object>) ((Map<String, Object>)relations.get(0).get(KEY_REL_ATTRS)).get("attributes");
        assertNotNull(relAttributes);
        assertEquals(relAttributes.get(ATTR_TO_TYPE), "Classifiedby");
        assertEquals(relAttributes.get(ATTR_FROM_TYPE), "Isa");

        ////////

        assertNotNull(table1.getRelationshipAttribute(REL_NAME_FROM));

        relations =  (List<Map<String, Object>>) table1.getRelationshipAttribute(REL_NAME_FROM);
        assertEquals(1, relations.size());
        assertNotNull(relations.get(0).get(KEY_REL_ATTRS));

        relAttributes = (Map<String, Object>) ((Map<String, Object>)relations.get(0).get(KEY_REL_ATTRS)).get("attributes");
        assertNotNull(relAttributes);
        assertEquals(relAttributes.get(ATTR_TO_TYPE), "Classifiedby");
        assertEquals(relAttributes.get(ATTR_FROM_TYPE), "Isa");
    }

    private static AtlasRelatedObjectId getUDRelationshipWithAttributes(String assetType, String assetQn) throws Exception {
        AtlasRelatedObjectId relation = new AtlasRelatedObjectId();

        relation.setTypeName(assetType);
        relation.setUniqueAttributes(mapOf(QUALIFIED_NAME, assetQn));
        relation.setRelationshipAttributes(new AtlasStruct(UD_RELATIONSHIP_TYPE_NAME, REL_ATTRS));

        return relation;
    }
}