package com.tests.main.kafka;

import com.tests.main.AtlasKafkaConsumer;
import com.tests.main.KafkaMessage;
import com.tests.main.Test;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.AssertionUtils;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tests.main.client.okhttp3.OKClient.isBeta;
import static com.tests.main.utils.TestUtil.TYPE_COLUMN;
import static com.tests.main.utils.TestUtil.TYPE_PROCESS;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getEntityAudit;
import static com.tests.main.utils.TestUtil.getObjectId;
import static com.tests.main.utils.TestUtil.getObjectIdsAsList;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateEntitiesBulk;
import static com.tests.main.utils.TestUtil.updateEntity;
import static org.apache.atlas.model.notification.EntityNotification.EntityNotificationV2.OperationType.*;
import static org.junit.Assert.*;

public class InverseNotificationsMutatedDetails implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(InverseNotificationsMutatedDetails.class);
    
    private static long SLEEP_FOR = 3; //seconds

    public static void main(String[] args) throws Exception {
        try {
            new InverseNotificationsMutatedDetails().run();
            //TestRunner.runTests(InverseNotificationsMutatedDetails.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running InverseNotificationsMutatedDetails tests");

        long start = System.currentTimeMillis();
        try {

            // CREATE - SINGLE

            createColumnToLinkTable();
            createTableToLinkColumn();


            // Update - SINGLE

            updateColumnToLinkTable();
            updateTableToLinkColumn();


            // CREATE - Multiple

            createColumnsToLinkTable();
            createColumnsToLinkTableSingleRequest();
            createTableToLinkColumnsSingleRequest();

            // Update - Multiple

            updateColumnsToLinkTable();
            updateColumnsToLinkTableSingleRequest();
            updateTableToLinkColumnsSingleRequest();


            // Update - n * n
            updateProcessesToLinkTablesSingleRequest();

            // n * 1 change asset at 1
            updateColumnChangeTable();



        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running InverseNotificationsMutatedDetails tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    @Test
    private static void createColumnToLinkTable() throws Exception {
        LOG.info(">> createColumnToLinkTable");

         /*
         * Create Table
         * Create Column with relationship.table
         *
         * ---------------- ----------------
         *
         * Update Column to remove relationship.table
         *
         * */

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");
        String tableGuid = createEntity(table_0).getGuidAssignments().values().iterator().next();

        sleep(SLEEP_FOR);
        System.out.printf("tableGuid %s", tableGuid);

        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "column_0");
        column_0.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        String columnGuid = createEntity(column_0).getGuidAssignments().values().iterator().next();


        sleep(SLEEP_FOR);

        EntityAuditSearchResult audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(1, audit.getEntityAudits().size());

        Map details = audit.getEntityAudits().get(0).getDetail();
        List columns = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(columnGuid, ((HashMap) columns.get(0)).get("guid"));

        // Update Column to remove relationship.table

        column_0 = getEntity(columnGuid);
        column_0.setAttribute("table", null);
        column_0.setRelationshipAttribute("table", null);
        updateEntity(column_0);

        sleep(SLEEP_FOR);
        column_0 = getEntity(columnGuid);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(2, audit.getEntityAudits().size());
        details = audit.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));

        columns = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(columnGuid, ((HashMap) columns.get(0)).get("guid"));

        Map tableAsRel = (Map) column_0.getRelationshipAttribute("table");
        assertEquals("DELETED", tableAsRel.get("relationshipStatus"));

        LOG.info("<< createColumnToLinkTable");
    }

    @Test
    private static void createTableToLinkColumn() throws Exception {
        LOG.info(">> createTableToLinkColumn");

        /*
         * Create Column
         * Create Table with relationship.columns
         *
         * ---------------- ----------------
         *
         * Update Table to remove relationship.columns
         *
         * */

        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "column_0");
        String columnGuid = createEntity(column_0).getGuidAssignments().values().iterator().next();

        sleep(SLEEP_FOR);

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");
        table_0.setRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, columnGuid));
        String tableGuid = createEntity(table_0).getGuidAssignments().values().iterator().next();

        sleep(SLEEP_FOR);


        EntityAuditSearchResult audit = getEntityAudit(columnGuid);

        assertNotNull(audit);
        assertEquals(1, audit.getEntityAudits().size());

        Map details = audit.getEntityAudits().get(0).getDetail();
        Map tableInAudit = (Map) ((Map) details.get("addedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));

        //Update Table to remove relationship.columns
        table_0 = getEntity(tableGuid);
        table_0.removeAttribute("columns");
        table_0.setRelationshipAttribute("columns", null);
        updateEntity(table_0);

        sleep(SLEEP_FOR);
        table_0 = getEntity(tableGuid);

        audit = getEntityAudit(columnGuid);

        assertNotNull(audit);
        assertEquals(2, audit.getEntityAudits().size());
        details = audit.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));

        tableInAudit = (Map) ((Map) details.get("removedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));

        List colAsRel = (List) table_0.getRelationshipAttribute("columns");
        assertEquals(1, colAsRel.size());
        assertEquals("DELETED", ((Map)colAsRel.get(0)).get("relationshipStatus"));

        LOG.info("<< createTableToLinkColumn");
    }

    @Test
    private static void updateColumnToLinkTable() throws Exception {
        LOG.info(">> updateColumnToLinkTable");

        /*
         * Create Table
         * Create Column without relationship
         * Update Column with relationship.table
         *
         * ---------------- ----------------
         *
         * Update Column to remove relationship.table
         *
         * */

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");
        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "column_0");

        EntityMutationResponse response = createEntitiesBulk(table_0, column_0);

        String tableGuid = response.getGuidAssignments().get(table_0.getGuid());
        String columnGuid = response.getGuidAssignments().get(column_0.getGuid());

        sleep(SLEEP_FOR);
        column_0 = getEntity(columnGuid);

        // Update Column
        column_0.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        updateEntity(column_0);
        sleep(SLEEP_FOR);

        EntityAuditSearchResult audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(1, audit.getEntityAudits().size());

        Map details = audit.getEntityAudits().get(0).getDetail();
        List columns = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(columnGuid, ((HashMap) columns.get(0)).get("guid"));

        // Update Column to remove relationship.table

        column_0 = getEntity(columnGuid);
        column_0.setAttribute("table", null);
        column_0.setRelationshipAttribute("table", null);
        updateEntity(column_0);

        sleep(SLEEP_FOR);
        column_0 = getEntity(columnGuid);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(2, audit.getEntityAudits().size());
        details = audit.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));

        columns = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(columnGuid, ((HashMap) columns.get(0)).get("guid"));

        Map tableAsRel = (Map) column_0.getRelationshipAttribute("table");
        assertEquals("DELETED", tableAsRel.get("relationshipStatus"));

        LOG.info("<< updateColumnToLinkTable");
    }

    @Test
    private static void updateTableToLinkColumn() throws Exception {
        LOG.info(">> updateTableToLinkColumn");

        /*
         * Create Column
         * Create Table without relationship
         * Update Table with relationship.columns
         *
         * ---------------- ----------------
         *
         * Update Table to remove relationship.columns
         *
         * */

        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "column_0");
        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");

        EntityMutationResponse response = createEntitiesBulk(table_0, column_0);

        String tableGuid = response.getGuidAssignments().get(table_0.getGuid());
        String columnGuid = response.getGuidAssignments().get(column_0.getGuid());

        sleep(SLEEP_FOR);
        table_0 = getEntity(tableGuid);

        // Update Table with relationship.columns
        table_0.setRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, columnGuid));
        updateEntity(table_0);
        sleep(SLEEP_FOR);


        EntityAuditSearchResult audit = getEntityAudit(columnGuid);

        assertNotNull(audit);
        assertEquals(1, audit.getEntityAudits().size());

        Map details = audit.getEntityAudits().get(0).getDetail();
        Map tableInAudit = (Map) ((Map) details.get("addedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));

        //Update Table to remove relationship.columns
        table_0 = getEntity(tableGuid);
        table_0.removeAttribute("columns");
        table_0.setRelationshipAttribute("columns", null);
        updateEntity(table_0);

        sleep(SLEEP_FOR);
        table_0 = getEntity(tableGuid);

        audit = getEntityAudit(columnGuid);

        assertNotNull(audit);
        assertEquals(2, audit.getEntityAudits().size());
        details = audit.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));

        tableInAudit = (Map) ((Map) details.get("removedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));

        List colAsRel = (List) table_0.getRelationshipAttribute("columns");
        assertEquals(1, colAsRel.size());
        assertEquals("DELETED", ((Map)colAsRel.get(0)).get("relationshipStatus"));

        LOG.info("<< updateTableToLinkColumn");
    }

    @Test
    private static void createColumnsToLinkTable() throws Exception {
        LOG.info(">> createColumnsToLinkTable");

        /*
         * Create Table
         * Create Columns with relationship.table in separate requests
         *
         * ---------------- ----------------
         *
         * Update Column to remove relationship.table
         *
         * */

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");
        String tableGuid = createEntity(table_0).getGuidAssignments().values().iterator().next();

        sleep(SLEEP_FOR);

        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "column_0");
        AtlasEntity column_1 = getAtlasEntity(TYPE_COLUMN, "column_1");
        AtlasEntity column_2 = getAtlasEntity(TYPE_COLUMN, "column_2");
        column_0.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        column_1.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        column_2.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));

        // ----

        String column_0_Guid = createEntity(column_0).getGuidAssignments().values().iterator().next();
        sleep(SLEEP_FOR);

        EntityAuditSearchResult audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(1, audit.getEntityAudits().size());

        Map details = audit.getEntityAudits().get(0).getDetail();
        List columns = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(column_0_Guid, ((HashMap) columns.get(0)).get("guid"));


        // ----

        String column_1_Guid = createEntity(column_1).getGuidAssignments().values().iterator().next();
        sleep(SLEEP_FOR);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(2, audit.getEntityAudits().size());

        details = audit.getEntityAudits().get(0).getDetail();
        columns = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(column_1_Guid, ((HashMap) columns.get(0)).get("guid"));

        // ----

        String column_2_Guid = createEntity(column_2).getGuidAssignments().values().iterator().next();
        sleep(SLEEP_FOR);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(3, audit.getEntityAudits().size());

        details = audit.getEntityAudits().get(0).getDetail();
        columns = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(column_2_Guid, ((HashMap) columns.get(0)).get("guid"));



        // Update Column to remove relationship.table

        column_0 = getEntity(column_0_Guid);
        column_0.setAttribute("table", null);
        column_0.setRelationshipAttribute("table", null);
        updateEntity(column_0);

        sleep(SLEEP_FOR);
        column_0 = getEntity(column_0_Guid);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(4, audit.getEntityAudits().size());
        details = audit.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));

        columns = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(column_0_Guid, ((HashMap) columns.get(0)).get("guid"));

        Map tableAsRel = (Map) column_0.getRelationshipAttribute("table");
        assertEquals("DELETED", tableAsRel.get("relationshipStatus"));

        // ----

        column_1 = getEntity(column_1_Guid);
        column_1.setAttribute("table", null);
        column_1.setRelationshipAttribute("table", null);
        updateEntity(column_1);

        sleep(SLEEP_FOR);
        column_1 = getEntity(column_1_Guid);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(5, audit.getEntityAudits().size());
        details = audit.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));

        columns = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(column_1_Guid, ((HashMap) columns.get(0)).get("guid"));

        tableAsRel = (Map) column_1.getRelationshipAttribute("table");
        assertEquals("DELETED", tableAsRel.get("relationshipStatus"));

        // ----

        column_2 = getEntity(column_2_Guid);
        column_2.setAttribute("table", null);
        column_2.setRelationshipAttribute("table", null);
        updateEntity(column_2);

        sleep(SLEEP_FOR);
        column_2 = getEntity(column_2_Guid);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(6, audit.getEntityAudits().size());
        details = audit.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));

        columns = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(column_2_Guid, ((HashMap) columns.get(0)).get("guid"));

        tableAsRel = (Map) column_2.getRelationshipAttribute("table");
        assertEquals("DELETED", tableAsRel.get("relationshipStatus"));

        LOG.info("<< createColumnsToLinkTable");
    }

    @Test
    private static void createColumnsToLinkTableSingleRequest() throws Exception {
        LOG.info(">> createColumnsToLinkTableSingleRequest");

        /*
         * Create Table
         * Create Columns with relationship.table
         *
         * ---------------- ----------------
         *
         * Update Column to remove relationship.table
         *
         * */

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");
        String tableGuid = createEntity(table_0).getGuidAssignments().values().iterator().next();

        sleep(SLEEP_FOR);

        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "column_0");
        AtlasEntity column_1 = getAtlasEntity(TYPE_COLUMN, "column_1");
        AtlasEntity column_2 = getAtlasEntity(TYPE_COLUMN, "column_2");
        column_0.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        column_1.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        column_2.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));

        EntityMutationResponse response = createEntitiesBulk(column_0, column_1, column_2);

        String column_0_guid = response.getGuidAssignments().get(column_0.getGuid());
        String column_1_guid = response.getGuidAssignments().get(column_1.getGuid());
        String column_2_guid = response.getGuidAssignments().get(column_2.getGuid());

        sleep(SLEEP_FOR);

        EntityAuditSearchResult audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(1, audit.getEntityAudits().size());

        Map details = audit.getEntityAudits().get(0).getDetail();
        List<HashMap> columns = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("columns");
        assertEquals(3, columns.size());

        Set<String> allGuids = columns.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(column_0_guid, column_1_guid, column_2_guid)));

        // Update Column to remove relationship.table

        column_0 = getEntity(column_0_guid);
        column_1 = getEntity(column_1_guid);
        column_2 = getEntity(column_2_guid);

        column_0.setAttribute("table", null);
        column_1.setAttribute("table", null);
        column_2.setAttribute("table", null);
        column_0.setRelationshipAttribute("table", null);
        column_1.setRelationshipAttribute("table", null);
        column_2.setRelationshipAttribute("table", null);

        updateEntitiesBulk(column_0, column_1, column_2);

        sleep(SLEEP_FOR);
        column_0 = getEntity(column_0_guid);
        column_1 = getEntity(column_1_guid);
        column_2 = getEntity(column_2_guid);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(2, audit.getEntityAudits().size());
        details = audit.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));

        columns = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("columns");
        assertEquals(3, columns.size());

        allGuids = columns.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(column_0_guid, column_1_guid, column_2_guid)));

        Map tableAsRel;
        tableAsRel = (Map) column_0.getRelationshipAttribute("table");
        assertEquals("DELETED", tableAsRel.get("relationshipStatus"));
        tableAsRel = (Map) column_1.getRelationshipAttribute("table");
        assertEquals("DELETED", tableAsRel.get("relationshipStatus"));
        tableAsRel = (Map) column_2.getRelationshipAttribute("table");
        assertEquals("DELETED", tableAsRel.get("relationshipStatus"));

        LOG.info("<< createColumnsToLinkTableSingleRequest");
    }

    @Test
    private static void createTableToLinkColumnsSingleRequest() throws Exception {
        LOG.info(">> createTableToLinkColumnSingleRequest");

        /*
         * Create Column
         * Create Table with relationship.columns
         *
         * ---------------- ----------------
         *
         * Update Table to remove relationship.columns
         *
         * */

        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "column_0");
        AtlasEntity column_1 = getAtlasEntity(TYPE_COLUMN, "column_1");
        AtlasEntity column_2 = getAtlasEntity(TYPE_COLUMN, "column_2");
        EntityMutationResponse response = createEntitiesBulk(column_0, column_1, column_2);

        String column_0_guid = response.getGuidAssignments().get(column_0.getGuid());
        String column_1_guid = response.getGuidAssignments().get(column_1.getGuid());
        String column_2_guid = response.getGuidAssignments().get(column_2.getGuid());

        sleep(SLEEP_FOR);

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");
        table_0.setRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, column_0_guid, column_1_guid, column_2_guid));
        String tableGuid = createEntity(table_0).getGuidAssignments().values().iterator().next();

        sleep(SLEEP_FOR);


        EntityAuditSearchResult audit_0 = getEntityAudit(column_0_guid);
        EntityAuditSearchResult audit_1 = getEntityAudit(column_1_guid);
        EntityAuditSearchResult audit_2 = getEntityAudit(column_2_guid);


        assertEquals(1, audit_0.getEntityAudits().size());
        assertEquals(1, audit_1.getEntityAudits().size());
        assertEquals(1, audit_2.getEntityAudits().size());

        Map details, tableInAudit;
        details = audit_0.getEntityAudits().get(0).getDetail();
        tableInAudit = (Map) ((Map) details.get("addedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));

        details = audit_1.getEntityAudits().get(0).getDetail();
        tableInAudit = (Map) ((Map) details.get("addedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));

        details = audit_2.getEntityAudits().get(0).getDetail();
        tableInAudit = (Map) ((Map) details.get("addedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));


        //Update Table to remove relationship.columns

        table_0 = getEntity(tableGuid);
        table_0.removeAttribute("columns");
        table_0.setRelationshipAttribute("columns", null);
        updateEntity(table_0);

        sleep(SLEEP_FOR);
        table_0 = getEntity(tableGuid);

        audit_0 = getEntityAudit(column_0_guid);
        audit_1 = getEntityAudit(column_1_guid);
        audit_2 = getEntityAudit(column_2_guid);

        assertEquals(2, audit_0.getEntityAudits().size());
        details = audit_0.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        tableInAudit = (Map) ((Map) details.get("removedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));

        assertEquals(2, audit_1.getEntityAudits().size());
        details = audit_0.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        tableInAudit = (Map) ((Map) details.get("removedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));

        assertEquals(2, audit_2.getEntityAudits().size());
        details = audit_0.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        tableInAudit = (Map) ((Map) details.get("removedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));


        List<Map> colsAsRel = (List) table_0.getRelationshipAttribute("columns");
        assertEquals(3, colsAsRel.size());
        assertEquals(3, colsAsRel.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());

        LOG.info("<< createTableToLinkColumnSingleRequest");
    }

    @Test
    private static void updateColumnsToLinkTable() throws Exception {
        LOG.info(">> updateColumnsToLinkTable");

        /*
         * Create Table
         * Create Columns without relationship.table
         * Create Columns with relationship.table in separate requests
         *
         * ---------------- ----------------
         *
         * Update Columns to remove relationship.table
         *
         * */

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");
        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "column_0");
        AtlasEntity column_1 = getAtlasEntity(TYPE_COLUMN, "column_1");
        AtlasEntity column_2 = getAtlasEntity(TYPE_COLUMN, "column_2");

        EntityMutationResponse response = createEntitiesBulk(table_0, column_0, column_1, column_2);

        String tableGuid = response.getGuidAssignments().get(table_0.getGuid());
        String column_0_Guid = response.getGuidAssignments().get(column_0.getGuid());
        String column_1_Guid = response.getGuidAssignments().get(column_1.getGuid());
        String column_2_Guid = response.getGuidAssignments().get(column_2.getGuid());
        sleep(SLEEP_FOR);

        column_0 = getEntity(column_0_Guid);
        column_1 = getEntity(column_1_Guid);
        column_2 = getEntity(column_2_Guid);


        // ----

        column_0.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        updateEntity(column_0);
        sleep(SLEEP_FOR);

        EntityAuditSearchResult audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(1, audit.getEntityAudits().size());

        Map details = audit.getEntityAudits().get(0).getDetail();
        List columns = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(column_0_Guid, ((HashMap) columns.get(0)).get("guid"));


        // ----

        column_1.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        updateEntity(column_1);
        sleep(SLEEP_FOR);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(2, audit.getEntityAudits().size());

        details = audit.getEntityAudits().get(0).getDetail();
        columns = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(column_1_Guid, ((HashMap) columns.get(0)).get("guid"));

        // ----

        column_2.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        updateEntity(column_2);
        sleep(SLEEP_FOR);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(3, audit.getEntityAudits().size());

        details = audit.getEntityAudits().get(0).getDetail();
        columns = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(column_2_Guid, ((HashMap) columns.get(0)).get("guid"));



        // Update Columns to remove relationship.table

        column_0 = getEntity(column_0_Guid);
        column_0.setAttribute("table", null);
        column_0.setRelationshipAttribute("table", null);
        updateEntity(column_0);

        sleep(SLEEP_FOR);
        column_0 = getEntity(column_0_Guid);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(4, audit.getEntityAudits().size());
        details = audit.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));

        columns = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(column_0_Guid, ((HashMap) columns.get(0)).get("guid"));

        Map tableAsRel = (Map) column_0.getRelationshipAttribute("table");
        assertEquals("DELETED", tableAsRel.get("relationshipStatus"));

        // ----

        column_1 = getEntity(column_1_Guid);
        column_1.setAttribute("table", null);
        column_1.setRelationshipAttribute("table", null);
        updateEntity(column_1);

        sleep(SLEEP_FOR);
        column_1 = getEntity(column_1_Guid);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(5, audit.getEntityAudits().size());
        details = audit.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));

        columns = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(column_1_Guid, ((HashMap) columns.get(0)).get("guid"));

        tableAsRel = (Map) column_1.getRelationshipAttribute("table");
        assertEquals("DELETED", tableAsRel.get("relationshipStatus"));

        // ----

        column_2 = getEntity(column_2_Guid);
        column_2.setAttribute("table", null);
        column_2.setRelationshipAttribute("table", null);
        updateEntity(column_2);

        sleep(SLEEP_FOR);
        column_2 = getEntity(column_2_Guid);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(6, audit.getEntityAudits().size());
        details = audit.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));

        columns = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(column_2_Guid, ((HashMap) columns.get(0)).get("guid"));

        tableAsRel = (Map) column_2.getRelationshipAttribute("table");
        assertEquals("DELETED", tableAsRel.get("relationshipStatus"));

        LOG.info("<< updateColumnsToLinkTable");
    }

    @Test
    private static void updateColumnsToLinkTableSingleRequest() throws Exception {
        LOG.info(">> updateColumnsToLinkTableSingleRequest");

        /*
         * Create Table
         * Create Columns without relationship
         * Update Columns with relationship.table in single request
         *
         * ---------------- ----------------
         *
         * Update Column to remove relationship.table
         *
         * */

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");
        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "column_0");
        AtlasEntity column_1 = getAtlasEntity(TYPE_COLUMN, "column_1");
        AtlasEntity column_2 = getAtlasEntity(TYPE_COLUMN, "column_2");

        EntityMutationResponse response = createEntitiesBulk(table_0, column_0, column_1, column_2);

        String tableGuid = response.getGuidAssignments().get(table_0.getGuid());
        String column_0_guid = response.getGuidAssignments().get(column_0.getGuid());
        String column_1_guid = response.getGuidAssignments().get(column_1.getGuid());
        String column_2_guid = response.getGuidAssignments().get(column_2.getGuid());

        sleep(SLEEP_FOR);
        column_0 = getEntity(column_0_guid);
        column_1 = getEntity(column_1_guid);
        column_2 = getEntity(column_2_guid);

        // ----
        column_0.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        column_1.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));
        column_2.setRelationshipAttribute("table", getObjectId(tableGuid, TYPE_TABLE));

        updateEntitiesBulk(column_0, column_1, column_2);
        sleep(SLEEP_FOR);

        EntityAuditSearchResult audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(1, audit.getEntityAudits().size());

        Map details = audit.getEntityAudits().get(0).getDetail();
        List<HashMap> columns = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("columns");
        assertEquals(3, columns.size());

        Set<String> allGuids = columns.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(column_0_guid, column_1_guid, column_2_guid)));

        // Update Columns to remove relationship.table

        column_0 = getEntity(column_0_guid);
        column_1 = getEntity(column_1_guid);
        column_2 = getEntity(column_2_guid);

        column_0.removeAttribute("table");
        column_1.removeAttribute("table");
        column_2.removeAttribute("table");
        column_0.setRelationshipAttribute("table", null);
        column_1.setRelationshipAttribute("table", null);
        column_2.setRelationshipAttribute("table", null);

        updateEntitiesBulk(column_0, column_1, column_2);
        sleep(SLEEP_FOR);

        table_0 = getEntity(tableGuid);

        audit = getEntityAudit(tableGuid);

        assertNotNull(audit);
        assertEquals(2, audit.getEntityAudits().size());
        details = audit.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));

        columns = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("columns");
        assertEquals(3, columns.size());

        allGuids = columns.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(column_0_guid, column_1_guid, column_2_guid)));

        List<Map> colsAsRel = (List) table_0.getRelationshipAttribute("columns");
        assertEquals(3, colsAsRel.size());
        assertEquals(3, colsAsRel.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());

        LOG.info("<< updateColumnsToLinkTableSingleRequest");
    }

    @Test
    private static void updateTableToLinkColumnsSingleRequest() throws Exception {
        LOG.info(">> updateTableToLinkColumnsSingleRequest");

        /*
         * Create Columns
         * Create Table without relationship
         * Update Table with relationship.columns
         *
         * ---------------- ----------------
         *
         * Update Table to remove relationship.columns
         *
         * */

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");
        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "column_0");
        AtlasEntity column_1 = getAtlasEntity(TYPE_COLUMN, "column_1");
        AtlasEntity column_2 = getAtlasEntity(TYPE_COLUMN, "column_2");

        EntityMutationResponse response = createEntitiesBulk(table_0, column_0, column_1, column_2);

        String tableGuid = response.getGuidAssignments().get(table_0.getGuid());
        String column_0_guid = response.getGuidAssignments().get(column_0.getGuid());
        String column_1_guid = response.getGuidAssignments().get(column_1.getGuid());
        String column_2_guid = response.getGuidAssignments().get(column_2.getGuid());

        sleep(SLEEP_FOR);

        // ------
        table_0 = getEntity(tableGuid);
        table_0.setRelationshipAttribute("columns", getObjectIdsAsList(TYPE_COLUMN, column_0_guid, column_1_guid, column_2_guid));
        updateEntity(table_0);
        sleep(SLEEP_FOR);


        EntityAuditSearchResult audit_0 = getEntityAudit(column_0_guid);
        EntityAuditSearchResult audit_1 = getEntityAudit(column_1_guid);
        EntityAuditSearchResult audit_2 = getEntityAudit(column_2_guid);

        assertNotNull(audit_0);
        assertNotNull(audit_1);
        assertNotNull(audit_2);

        assertEquals(1, audit_0.getEntityAudits().size());
        assertEquals(1, audit_1.getEntityAudits().size());
        assertEquals(1, audit_2.getEntityAudits().size());

        Map details = audit_0.getEntityAudits().get(0).getDetail();
        Map tableInAudit = (Map) ((Map) details.get("addedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));

        details = audit_1.getEntityAudits().get(0).getDetail();
        tableInAudit = (Map) ((Map) details.get("addedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));

        details = audit_2.getEntityAudits().get(0).getDetail();
        tableInAudit = (Map) ((Map) details.get("addedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));


        //Update Table to remove relationship.columns

        table_0 = getEntity(tableGuid);
        table_0.removeAttribute("columns");
        table_0.setRelationshipAttribute("columns", null);
        updateEntity(table_0);

        sleep(SLEEP_FOR);
        table_0 = getEntity(tableGuid);

        audit_0 = getEntityAudit(column_0_guid);
        audit_1 = getEntityAudit(column_1_guid);
        audit_2 = getEntityAudit(column_2_guid);

        assertEquals(2, audit_0.getEntityAudits().size());
        details = audit_0.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        tableInAudit = (Map) ((Map) details.get("removedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));

        assertEquals(2, audit_1.getEntityAudits().size());
        details = audit_0.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        tableInAudit = (Map) ((Map) details.get("removedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));

        assertEquals(2, audit_2.getEntityAudits().size());
        details = audit_0.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        tableInAudit = (Map) ((Map) details.get("removedRelationshipAttributes")).get("table");
        assertEquals(tableGuid, tableInAudit.get("guid"));


        List<Map> colsAsRel = (List) table_0.getRelationshipAttribute("columns");
        assertEquals(3, colsAsRel.size());
        assertEquals(3, colsAsRel.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());

        LOG.info("<< updateTableToLinkColumnsSingleRequest");
    }

    @Test
    private static void updateProcessesToLinkTablesSingleRequest() throws Exception {
        LOG.info(">> updateProcessesToLinkTablesSingleRequest");

        /*
         * Create Processes
         * Create Tables
         * Update Processes to add relationship.inputs
         *
         * ---------------- ----------------
         *
         * Update Processes to remove relationship.inputs
         *
         * ---------------- ----------------
         *
         * Update Processes to remove remaining 2 relationship.inputs
         *
         * */

        AtlasEntity process_0 = getAtlasEntity(TYPE_PROCESS, "process_0");
        AtlasEntity process_1 = getAtlasEntity(TYPE_PROCESS, "process_0");

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");
        AtlasEntity table_1 = getAtlasEntity(TYPE_TABLE, "table_1");
        AtlasEntity table_2 = getAtlasEntity(TYPE_TABLE, "table_2");
        AtlasEntity table_3 = getAtlasEntity(TYPE_TABLE, "table_3");

        EntityMutationResponse response = createEntitiesBulk(table_0, table_1, table_2, table_3, process_0, process_1);

        String table_0_guid = response.getGuidAssignments().get(table_0.getGuid());
        String table_1_guid = response.getGuidAssignments().get(table_1.getGuid());
        String table_2_guid = response.getGuidAssignments().get(table_2.getGuid());
        String table_3_guid = response.getGuidAssignments().get(table_3.getGuid());
        String process_0_Guid = response.getGuidAssignments().get(process_0.getGuid());
        String process_1_Guid = response.getGuidAssignments().get(process_1.getGuid());

        sleep(SLEEP_FOR);

        // Update Processes to add relationship.inputs

        process_0.setRelationshipAttribute("inputs", getObjectIdsAsList(TYPE_TABLE, table_0_guid, table_1_guid, table_2_guid, table_3_guid));
        process_1.setRelationshipAttribute("inputs", getObjectIdsAsList(TYPE_TABLE, table_0_guid, table_1_guid, table_2_guid, table_3_guid));
        updateEntitiesBulk(process_0, process_1);
        sleep(SLEEP_FOR);

        Set<String> allGuids; List<Map> mutatedRelations;
        Map<String, KafkaMessage> messages;

        if (!isBeta) {
            messages = AtlasKafkaConsumer.pollMessages(6);
            assertEquals(6, messages.values().stream().filter(x -> x.getOperationType().equals(ENTITY_UPDATE)).count());

            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(process_0_Guid))
                    .forRelationName("inputs")
                    .relationType(AssertionUtils.RelationType.RELATION)
                    .expecteRelationTypeName(TYPE_TABLE)
                    .expectRelationGuids(table_0_guid, table_1_guid, table_2_guid, table_3_guid)
                    .validate();

            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(process_1_Guid))
                    .forRelationName("inputs")
                    .relationType(AssertionUtils.RelationType.RELATION)
                    .expecteRelationTypeName(TYPE_TABLE)
                    .expectRelationGuids(table_0_guid, table_1_guid, table_2_guid, table_3_guid)
                    .validate();

            //table_0
            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_0_guid))
                    .forRelationName("inputToProcesses")
                    .relationType(AssertionUtils.RelationType.RELATION_ADDED)
                    .expecteRelationTypeName(TYPE_PROCESS)
                    .expectRelationGuids(process_0_Guid, process_1_Guid)
                    .validate();

            //table_1
            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_1_guid))
                    .forRelationName("inputToProcesses")
                    .relationType(AssertionUtils.RelationType.RELATION_ADDED)
                    .expecteRelationTypeName(TYPE_PROCESS)
                    .expectRelationGuids(process_0_Guid, process_1_Guid)
                    .validate();

            //table_2
            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_1_guid))
                    .forRelationName("inputToProcesses")
                    .relationType(AssertionUtils.RelationType.RELATION_ADDED)
                    .expecteRelationTypeName(TYPE_PROCESS)
                    .expectRelationGuids(process_0_Guid, process_1_Guid)
                    .validate();

            //table_3
            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_3_guid))
                    .forRelationName("inputToProcesses")
                    .relationType(AssertionUtils.RelationType.RELATION_ADDED)
                    .expecteRelationTypeName(TYPE_PROCESS)
                    .expectRelationGuids(process_0_Guid, process_1_Guid)
                    .validate();
        }

        EntityAuditSearchResult audit_table_0 = getEntityAudit(table_0_guid);
        EntityAuditSearchResult audit_table_1 = getEntityAudit(table_1_guid);
        EntityAuditSearchResult audit_table_2 = getEntityAudit(table_2_guid);
        EntityAuditSearchResult audit_table_3 = getEntityAudit(table_3_guid);

        assertEquals(1, audit_table_0.getEntityAudits().size());
        assertEquals(1, audit_table_1.getEntityAudits().size());
        assertEquals(1, audit_table_2.getEntityAudits().size());
        assertEquals(1, audit_table_3.getEntityAudits().size());

        Map details; List<Map> inputs;
        details = audit_table_0.getEntityAudits().get(0).getDetail();
        inputs = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("inputToProcesses");
        assertEquals(2, inputs.size());
        allGuids = inputs.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(process_0_Guid, process_1_Guid)));

        details = audit_table_1.getEntityAudits().get(0).getDetail();
        inputs = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("inputToProcesses");
        assertEquals(2, inputs.size());
        allGuids = inputs.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(process_0_Guid, process_1_Guid)));

        details = audit_table_2.getEntityAudits().get(0).getDetail();
        inputs = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("inputToProcesses");
        assertEquals(2, inputs.size());
        allGuids = inputs.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(process_0_Guid, process_1_Guid)));

        details = audit_table_3.getEntityAudits().get(0).getDetail();
        inputs = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("inputToProcesses");
        assertEquals(2, inputs.size());
        allGuids = inputs.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(process_0_Guid, process_1_Guid)));



        // Update Processes to remove a 2 relationship.inputs

        process_0 = getEntity(process_0_Guid);
        process_1 = getEntity(process_1_Guid);

        process_0.removeAttribute("inputs");
        process_1.removeAttribute("inputs");
        process_0.setRelationshipAttribute("inputs", getObjectIdsAsList(TYPE_TABLE, table_1_guid, table_2_guid));
        process_1.setRelationshipAttribute("inputs", getObjectIdsAsList(TYPE_TABLE, table_2_guid, table_3_guid));
        updateEntitiesBulk(process_0, process_1);

        sleep(SLEEP_FOR);

        if (!isBeta) {
            messages = AtlasKafkaConsumer.pollMessages(6);
            assertEquals(5, messages.values().stream().filter(x -> x.getOperationType().equals(ENTITY_UPDATE)).count());

            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(process_0_Guid))
                    .forRelationName("inputs")
                    .relationType(AssertionUtils.RelationType.RELATION)
                    .expecteRelationTypeName(TYPE_TABLE)
                    .expectRelationGuids(table_1_guid, table_2_guid)
                    .validate();

            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(process_1_Guid))
                    .forRelationName("inputs")
                    .relationType(AssertionUtils.RelationType.RELATION)
                    .expecteRelationTypeName(TYPE_TABLE)
                    .expectRelationGuids(table_2_guid, table_3_guid)
                    .validate();

            //table_0
            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_0_guid))
                    .forRelationName("inputToProcesses")
                    .relationType(AssertionUtils.RelationType.RELATION_REMOVED)
                    .expecteRelationTypeName(TYPE_PROCESS)
                    .expectRelationGuids(process_0_Guid, process_1_Guid)
                    .validate();

            //table_1
            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_1_guid))
                    .forRelationName("inputToProcesses")
                    .relationType(AssertionUtils.RelationType.RELATION_REMOVED)
                    .expecteRelationTypeName(TYPE_PROCESS)
                    .expectRelationGuids(process_1_Guid)
                    .validate();

            //table_3
            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_3_guid))
                    .forRelationName("inputToProcesses")
                    .relationType(AssertionUtils.RelationType.RELATION_REMOVED)
                    .expecteRelationTypeName(TYPE_PROCESS)
                    .expectRelationGuids(process_0_Guid)
                    .validate();
        }

        audit_table_0 = getEntityAudit(table_0_guid);
        audit_table_1 = getEntityAudit(table_1_guid);
        audit_table_2 = getEntityAudit(table_2_guid);
        audit_table_3 = getEntityAudit(table_3_guid);


        assertEquals(2, audit_table_0.getEntityAudits().size());
        assertEquals(2, audit_table_1.getEntityAudits().size());
        assertEquals(1, audit_table_2.getEntityAudits().size()); // Why unnecessary update Kafka event? because it assumes relationship update
        assertEquals(2, audit_table_3.getEntityAudits().size());

        details = audit_table_0.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        inputs = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("inputToProcesses");
        assertEquals(2, inputs.size());
        allGuids = inputs.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(process_0_Guid, process_1_Guid)));

        details = audit_table_1.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        inputs = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("inputToProcesses");
        assertEquals(1, inputs.size());
        allGuids = inputs.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(process_1_Guid)));

        details = audit_table_3.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        inputs = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("inputToProcesses");
        assertEquals(1, inputs.size());
        allGuids = inputs.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(process_0_Guid)));

        process_0 = getEntity(process_0_Guid);
        process_1 = getEntity(process_1_Guid);

        List<Map> inputsAsRel;
        inputsAsRel = (List<Map>) process_0.getRelationshipAttribute("inputs");
        assertEquals(4, inputsAsRel.size());
        allGuids = inputsAsRel.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(table_1_guid, table_2_guid)));
        allGuids = inputsAsRel.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(table_0_guid, table_3_guid)));

        inputsAsRel = (List<Map>) process_1.getRelationshipAttribute("inputs");
        assertEquals(4, inputsAsRel.size());
        allGuids = inputsAsRel.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(table_2_guid, table_3_guid)));
        allGuids = inputsAsRel.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(table_0_guid, table_1_guid)));


        //Update Processes to remove remaining 2 relationship.inputs

        /*
        * process_0.inputs -> [table_1, table_2]
        * process_1.inputs -> [table_2, table_3]
        * */

        process_0 = getEntity(process_0_Guid);
        process_1 = getEntity(process_1_Guid);

        process_0.removeAttribute("inputs");
        process_1.removeAttribute("inputs");
        process_0.setRelationshipAttribute("inputs", null);
        process_1.setRelationshipAttribute("inputs", null);
        updateEntitiesBulk(process_0, process_1);

        sleep(SLEEP_FOR);

        if (!isBeta) {
            messages = AtlasKafkaConsumer.pollMessages(6);
            assertEquals(5, messages.values().stream().filter(x -> x.getOperationType().equals(ENTITY_UPDATE)).count());

            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(process_0_Guid))
                    .forRelationName("inputs")
                    .relationType(AssertionUtils.RelationType.RELATION)
                    .validate();

            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(process_1_Guid))
                    .forRelationName("inputs")
                    .relationType(AssertionUtils.RelationType.RELATION)
                    .validate();

            //table_1
            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_1_guid))
                    .forRelationName("inputToProcesses")
                    .relationType(AssertionUtils.RelationType.RELATION_REMOVED)
                    .expecteRelationTypeName(TYPE_PROCESS)
                    .expectRelationGuids(process_0_Guid)
                    .validate();

            //table_2
            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_2_guid))
                    .forRelationName("inputToProcesses")
                    .relationType(AssertionUtils.RelationType.RELATION_REMOVED)
                    .expecteRelationTypeName(TYPE_PROCESS)
                    .expectRelationGuids(process_0_Guid, process_1_Guid)
                    .validate();

            //table_3
            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_3_guid))
                    .forRelationName("inputToProcesses")
                    .relationType(AssertionUtils.RelationType.RELATION_REMOVED)
                    .expecteRelationTypeName(TYPE_PROCESS)
                    .expectRelationGuids(process_1_Guid)
                    .validate();
        }

        audit_table_0 = getEntityAudit(table_0_guid);
        audit_table_1 = getEntityAudit(table_1_guid);
        audit_table_2 = getEntityAudit(table_2_guid);
        audit_table_3 = getEntityAudit(table_3_guid);


        assertEquals(2, audit_table_0.getEntityAudits().size());
        assertEquals(3, audit_table_1.getEntityAudits().size());
        assertEquals(2, audit_table_2.getEntityAudits().size());
        assertEquals(3, audit_table_3.getEntityAudits().size());

        details = audit_table_1.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        inputs = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("inputToProcesses");
        assertEquals(1, inputs.size());
        allGuids = inputs.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(process_0_Guid)));

        details = audit_table_2.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        inputs = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("inputToProcesses");
        assertEquals(2, inputs.size());
        allGuids = inputs.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(process_0_Guid, process_1_Guid)));

        details = audit_table_3.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        inputs = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("inputToProcesses");
        assertEquals(1, inputs.size());
        allGuids = inputs.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(process_1_Guid)));

        process_0 = getEntity(process_0_Guid);
        process_1 = getEntity(process_1_Guid);

        inputsAsRel = (List<Map>) process_0.getRelationshipAttribute("inputs");
        assertEquals(4, inputsAsRel.size());
        allGuids = inputsAsRel.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertEquals(0, allGuids.size());
        allGuids = inputsAsRel.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(table_0_guid, table_1_guid, table_2_guid, table_3_guid)));

        inputsAsRel = (List<Map>) process_1.getRelationshipAttribute("inputs");
        assertEquals(4, inputsAsRel.size());
        allGuids = inputsAsRel.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertEquals(0, allGuids.size());
        allGuids = inputsAsRel.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(allGuids.containsAll(Arrays.asList(table_0_guid, table_1_guid, table_2_guid, table_3_guid)));


        LOG.info("<< updateProcessesToLinkTablesSingleRequest");
    }

    @Test
    private static void updateColumnChangeTable() throws Exception {
        LOG.info(">> updateColumnChangeTable");

        /*
         * Create Tables table_0, table_1
         *
         *
         * Create Column with relationship.table = table_0
         *
         * ---------------- ----------------
         *
         * Update Column to change relationship.table = table_1
         *
         * */

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");
        AtlasEntity table_1 = getAtlasEntity(TYPE_TABLE, "table_1");
        EntityMutationResponse response = createEntitiesBulk(table_0, table_1);

        String table_0_guid = response.getGuidAssignments().get(table_0.getGuid());
        String table_1_guid = response.getGuidAssignments().get(table_1.getGuid());

        sleep(SLEEP_FOR);

        Map<String, KafkaMessage> messages;
        if (!isBeta) {
            messages = AtlasKafkaConsumer.pollMessages(2);
            assertEquals(2, messages.size());
        }

        // Create Column with relationship.table = table_0
        AtlasEntity column_0 = getAtlasEntity(TYPE_COLUMN, "column_0");
        column_0.setRelationshipAttribute("table", getObjectId(table_0_guid, TYPE_TABLE));
        String columnGuid = createEntity(column_0).getGuidAssignments().values().iterator().next();
        sleep(SLEEP_FOR);

        if (!isBeta) {
            messages = AtlasKafkaConsumer.pollMessages(2);
            assertEquals(2, messages.size());
            assertEquals(1, messages.values().stream().filter(x -> x.getOperationType().equals(ENTITY_CREATE)).count());
            assertEquals(1, messages.values().stream().filter(x -> x.getOperationType().equals(ENTITY_UPDATE)).count());

/*
        new AssertionUtils.KafkaEventRelationshipValidator()
                .event(messages.get(columnGuid))
                .forRelationName("table")
                .expecteRelationTypeName(TYPE_TABLE)
                .expectedRelationGuid(table_0_guid)
                .relationType(AssertionUtils.RelationType.RELATION)
                .validate();
*/

            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_0_guid))
                    .forRelationName("columns")
                    .relationType(AssertionUtils.RelationType.RELATION_ADDED)
                    .expecteRelationTypeName(TYPE_COLUMN)
                    .expectRelationGuids(columnGuid)
                    .validate();
        }


        // Update Column to change relationship.table = table_1
        column_0 = getEntity(columnGuid);

        column_0.removeAttribute("table");
        column_0.setRelationshipAttribute("table", getObjectId(table_1_guid, TYPE_TABLE));
        updateEntity(column_0);
        sleep(SLEEP_FOR);

        if (!isBeta) {
            messages = AtlasKafkaConsumer.pollMessages(3);
            assertEquals(3, messages.size());
            assertEquals(3, messages.values().stream().filter(x -> x.getOperationType().equals(ENTITY_UPDATE)).count());

            new AssertionUtils.KafkaEventRelationshipValidator()
                    .event(messages.get(columnGuid))
                    .forRelationName("table")
                    .expectedRelationGuid(table_1_guid)
                    .expecteRelationTypeName(TYPE_TABLE)
                    .relationType(AssertionUtils.RelationType.RELATION)
                    .validate();

            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_0_guid))
                    .forRelationName("columns")
                    .relationType(AssertionUtils.RelationType.RELATION_REMOVED)
                    .expecteRelationTypeName(TYPE_COLUMN)
                    .expectRelationGuids(columnGuid)
                    .validate();

            new AssertionUtils.KafkaEventListRelationshipValidator()
                    .event(messages.get(table_1_guid))
                    .forRelationName("columns")
                    .relationType(AssertionUtils.RelationType.RELATION_ADDED)
                    .expecteRelationTypeName(TYPE_COLUMN)
                    .expectRelationGuids(columnGuid)
                    .validate();
        }


        EntityAuditSearchResult audit_table_0 = getEntityAudit(table_0_guid);
        EntityAuditSearchResult audit_table_1 = getEntityAudit(table_1_guid);

        assertNotNull(audit_table_0);
        assertNotNull(audit_table_1);

        List columns; Map details;
        assertEquals(2, audit_table_0.getEntityAudits().size());

        details = audit_table_0.getEntityAudits().get(0).getDetail();
        assertNull(details.get("addedRelationshipAttributes"));
        columns = (List) ((HashMap) details.get("removedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(columnGuid, ((HashMap) columns.get(0)).get("guid"));

        assertEquals(1, audit_table_1.getEntityAudits().size());

        details = audit_table_1.getEntityAudits().get(0).getDetail();
        assertNull(details.get("removedRelationshipAttributes"));
        columns = (List) ((HashMap) details.get("addedRelationshipAttributes")).get("columns");
        assertEquals(1, columns.size());
        assertEquals(columnGuid, ((HashMap) columns.get(0)).get("guid"));

        table_0 = getEntity(table_0_guid);
        table_1 = getEntity(table_1_guid);

        List<Map> columnsAsRel = (List) table_0.getRelationshipAttribute("columns");
        assertEquals(1, columnsAsRel.size());
        assertEquals("DELETED", columnsAsRel.get(0).get("relationshipStatus"));

        columnsAsRel = (List) table_1.getRelationshipAttribute("columns");
        assertEquals(1, columnsAsRel.size());
        assertEquals("ACTIVE", columnsAsRel.get(0).get("relationshipStatus"));

        LOG.info("<< updateColumnChangeTable");
    }
}
