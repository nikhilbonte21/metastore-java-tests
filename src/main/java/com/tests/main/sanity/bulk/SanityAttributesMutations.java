package com.tests.main.sanity.bulk;

import com.tests.main.Test;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getObjectIdsAsList;
import static com.tests.main.utils.TestUtil.mapOf;
import static com.tests.main.utils.TestUtil.sleep;
import static org.junit.Assert.*;


public class SanityAttributesMutations implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(SanityAttributesMutations.class);

    public static String TYPE_PROJECT = "TableauProject";
    public static String TYPE_WORKOBOOK = "TableauWorkbook";
    public static String TYPE_WORKOSHEET = "TableauWorksheet";
    public static String TYPE_CALC_FIELD = "TableauCalculatedField";

    public static String REL_TYPE_PROJECT = "project";
    public static String REL_TYPE_WORKOBOOKS = "workbooks";
    public static String REL_TYPE_CALC_FIELDS = "calculatedFields";
    public static String REL_TYPE_WORKSHEETS = "worksheets";

    public static String ATTR_OWNER_USERS = "ownerUsers";
    public static String ATTR_PROJECT_HIERARCHY = "projectHierarchy";
    public static String ATTR_USER_RECORD_LIST = "sourceReadRecentUserRecordList";

    public static String STRUCT_TYPE_POPULARITY_INSIGHTS = "PopularityInsights";

    public static void main(String[] args) throws Exception {
        try {
            new SanityAttributesMutations().run();
            //TestRunner.runTests(SanityAttributesMutations.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running SanityAttributesMutations tests");

        long start = System.currentTimeMillis();
        try {
            //https://atlanhq.atlassian.net/wiki/spaces/~61277197fc5509007110a8d6/pages/728760421/MLH-173+DEV+tests

            relationshipOneToMany(); // Project 1 <> n Workbook
            relationshipManyToOne(); // Workbook n <> 1 Project
            relationshipManyToMany(); // Workbook n <> 1 Project

            arrayOfMaps(); // projectHierarchy array<map<string,string>>

            arrayOfStrings(); // Table.ownerUsers
            arrayOfStructs(); // Table.sourceReadRecentUserRecordList ==> PopularityInsights

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running SanityAttributesMutations tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    @Test
    private static void relationshipOneToMany() throws Exception {
        LOG.info(">> relationshipOneToMany");

        /*
        * Project 1 <> n Workbook
        *
        * Update Project
        * 1. Add one Workbook
        * 2. Add two more Workbook
        * 3. Remove one Workbook
        * 4. Add one + Remove one Workbook
        * 5. Remove all Workbooks
        * 6. Add all 3 Workbooks back
        * */

        AtlasEntity tableauProject = getAtlasEntity(TYPE_PROJECT, "project_0");
        String projectGuid = createEntity(tableauProject).getGuidAssignments().values().iterator().next();

        AtlasEntity tableauWB_0 = getAtlasEntity(TYPE_WORKOBOOK, "wb_0");
        AtlasEntity tableauWB_1 = getAtlasEntity(TYPE_WORKOBOOK, "wb_1");
        AtlasEntity tableauWB_2 = getAtlasEntity(TYPE_WORKOBOOK, "wb_2");

        String tableauWB_0_guid = createEntity(tableauWB_0).getGuidAssignments().values().iterator().next();
        String tableauWB_1_guid = createEntity(tableauWB_1).getGuidAssignments().values().iterator().next();
        String tableauWB_2_guid = createEntity(tableauWB_2).getGuidAssignments().values().iterator().next();

        /*System.out.println(projectGuid);
        System.out.println(tableauWB_0_guid);
        System.out.println(tableauWB_1_guid);
        System.out.println(tableauWB_2_guid);*/

        sleep(2);
        tableauProject = getEntity(projectGuid);

        // 1. Add one Workbook
        tableauProject.setRelationshipAttribute(REL_TYPE_WORKOBOOKS, getObjectIdsAsList(TYPE_WORKOBOOK, tableauWB_0_guid));
        createEntity(tableauProject);
        sleep(2);

        tableauProject = getEntity(projectGuid);

        assertNotNull(tableauProject);
        assertNotNull(tableauProject.getRelationshipAttributes());
        assertNotNull(tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS));

        List<Map<String, Object>> relations = (List<Map<String, Object>>) tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS);
        assertEquals(1, relations.size());
        assertEquals(1, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        // 2. Add two more Workbook
        tableauProject.setRelationshipAttribute(REL_TYPE_WORKOBOOKS, getObjectIdsAsList(TYPE_WORKOBOOK, tableauWB_0_guid, tableauWB_1_guid, tableauWB_2_guid));
        createEntity(tableauProject);
        sleep(2);

        tableauProject = getEntity(projectGuid);

        assertNotNull(tableauProject);
        assertNotNull(tableauProject.getRelationshipAttributes());
        assertNotNull(tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS));

        relations = (List<Map<String, Object>>) tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS);
        assertEquals(3, relations.size());
        assertEquals(3, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        tableauWB_0 = getEntity(tableauWB_0_guid);
        tableauWB_1 = getEntity(tableauWB_1_guid);
        tableauWB_2 = getEntity(tableauWB_2_guid);

        Map<String, Object> relation = (Map<String, Object>) tableauWB_0.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_1.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_2.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));

        // 3. Remove one Workbook
        tableauProject.setRelationshipAttribute(REL_TYPE_WORKOBOOKS, getObjectIdsAsList(TYPE_WORKOBOOK, tableauWB_1_guid, tableauWB_2_guid));
        createEntity(tableauProject);
        sleep(2);

        tableauProject = getEntity(projectGuid);

        assertNotNull(tableauProject);
        assertNotNull(tableauProject.getRelationshipAttributes());
        assertNotNull(tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS));

        relations = (List<Map<String, Object>>) tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS);
        assertEquals(3, relations.size());
        assertEquals(1, relations.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());
        assertEquals(2, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        tableauWB_0 = getEntity(tableauWB_0_guid);
        tableauWB_1 = getEntity(tableauWB_1_guid);
        tableauWB_2 = getEntity(tableauWB_2_guid);

        relation = (Map<String, Object>) tableauWB_0.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("DELETED".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_1.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_2.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));

        // 4. Add one + Remove one Workbook
        tableauProject.setRelationshipAttribute(REL_TYPE_WORKOBOOKS, getObjectIdsAsList(TYPE_WORKOBOOK, tableauWB_0_guid, tableauWB_1_guid));
        createEntity(tableauProject);
        sleep(2);

        tableauProject = getEntity(projectGuid);

        relations = (List<Map<String, Object>>) tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS);
        assertEquals(4, relations.size());
        assertEquals(2, relations.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());
        assertEquals(2, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        tableauWB_0 = getEntity(tableauWB_0_guid);
        tableauWB_1 = getEntity(tableauWB_1_guid);
        tableauWB_2 = getEntity(tableauWB_2_guid);

        relation = (Map<String, Object>) tableauWB_0.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_1.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_2.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("DELETED".equals(relation.get("relationshipStatus")));

        // 5. Remove all Workbooks
        tableauProject.setRelationshipAttribute(REL_TYPE_WORKOBOOKS, getObjectIdsAsList(TYPE_WORKOBOOK));
        createEntity(tableauProject);
        sleep(2);

        tableauProject = getEntity(projectGuid);

        relations = (List<Map<String, Object>>) tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS);
        assertEquals(4, relations.size());
        assertEquals(4, relations.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());

        tableauWB_0 = getEntity(tableauWB_0_guid);
        tableauWB_1 = getEntity(tableauWB_1_guid);
        tableauWB_2 = getEntity(tableauWB_2_guid);

        relation = (Map<String, Object>) tableauWB_0.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("DELETED".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_1.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("DELETED".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_2.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("DELETED".equals(relation.get("relationshipStatus")));

        // 6. Add all 3 Workbooks back
        tableauProject.setRelationshipAttribute(REL_TYPE_WORKOBOOKS, getObjectIdsAsList(TYPE_WORKOBOOK, tableauWB_0_guid, tableauWB_1_guid, tableauWB_2_guid));
        createEntity(tableauProject);
        sleep(2);

        tableauProject = getEntity(projectGuid);

        relations = (List<Map<String, Object>>) tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS);
        assertEquals(7, relations.size());
        assertEquals(4, relations.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());
        assertEquals(3, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        tableauWB_0 = getEntity(tableauWB_0_guid);
        tableauWB_1 = getEntity(tableauWB_1_guid);
        tableauWB_2 = getEntity(tableauWB_2_guid);

        relation = (Map<String, Object>) tableauWB_0.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_1.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_2.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));

        LOG.info(">> relationshipOneToMany");
    }

    @Test
    private static void relationshipManyToOne() throws Exception {
        LOG.info(">> relationshipManyToOne");

        /*
         * Workbook n <> 1 Project
         *
         * Update Workbook
         * 1. Update Workbook to Link Project
         * 2. Update 2 more Workbooks to Link Project
         * 3. Update 1 Workbook to Unlink Project
         * 4. Update 1 Workbook to Link + another Workbook to Unlink Project
         * 5. Update all Workbooks to Unlink Project
         * 6. Update all Workbooks to Link back to Project
         * */

        AtlasEntity tableauProject = getAtlasEntity(TYPE_PROJECT, "project_0");
        String projectGuid = createEntity(tableauProject).getGuidAssignments().values().iterator().next();

        AtlasEntity tableauWB_0 = getAtlasEntity(TYPE_WORKOBOOK, "wb_0");
        AtlasEntity tableauWB_1 = getAtlasEntity(TYPE_WORKOBOOK, "wb_1");
        AtlasEntity tableauWB_2 = getAtlasEntity(TYPE_WORKOBOOK, "wb_2");

        String tableauWB_0_guid = createEntity(tableauWB_0).getGuidAssignments().values().iterator().next();
        String tableauWB_1_guid = createEntity(tableauWB_1).getGuidAssignments().values().iterator().next();
        String tableauWB_2_guid = createEntity(tableauWB_2).getGuidAssignments().values().iterator().next();

        /*System.out.println(projectGuid);
        System.out.println(tableauWB_0_guid);
        System.out.println(tableauWB_1_guid);
        System.out.println(tableauWB_2_guid);*/

        sleep(2);
        tableauProject = getEntity(projectGuid);
        tableauWB_0 = getEntity(tableauWB_0_guid);

        // 1. Link one Workbooks to Project
        tableauWB_0.setRelationshipAttribute(REL_TYPE_PROJECT, new AtlasObjectId(projectGuid, TYPE_PROJECT));
        createEntity(tableauWB_0);
        sleep(2);

        tableauProject = getEntity(projectGuid);
        tableauWB_0 = getEntity(tableauWB_0_guid);

        List<Map<String, Object>> relations = (List<Map<String, Object>>) tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS);
        assertEquals(1, relations.size());
        assertEquals(1, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        Map<String, Object> relation = (Map<String, Object>) tableauWB_0.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));

        // 2. Link two more Workbooks to Project
        tableauWB_1.setRelationshipAttribute(REL_TYPE_PROJECT, new AtlasObjectId(projectGuid, TYPE_PROJECT));
        tableauWB_2.setRelationshipAttribute(REL_TYPE_PROJECT, new AtlasObjectId(projectGuid, TYPE_PROJECT));

        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(tableauWB_1);
        entitiesWithExtInfo.addEntity(tableauWB_2);
        createEntitiesBulk(entitiesWithExtInfo);
        sleep(2);

        tableauProject = getEntity(projectGuid);

        relations = (List<Map<String, Object>>) tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS);
        assertEquals(3, relations.size());
        assertEquals(3, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        tableauWB_0 = getEntity(tableauWB_0_guid);
        tableauWB_1 = getEntity(tableauWB_1_guid);
        tableauWB_2 = getEntity(tableauWB_2_guid);

        relation = (Map<String, Object>) tableauWB_0.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_1.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_2.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));

        // 3. Unlink one Workbook from Project
        tableauWB_0.setRelationshipAttribute(REL_TYPE_PROJECT, null);
        createEntity(tableauWB_0);
        sleep(2);

        tableauProject = getEntity(projectGuid);

        relations = (List<Map<String, Object>>) tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS);
        assertEquals(3, relations.size());
        assertEquals(1, relations.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());
        assertEquals(2, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        tableauWB_0 = getEntity(tableauWB_0_guid);
        tableauWB_1 = getEntity(tableauWB_1_guid);
        tableauWB_2 = getEntity(tableauWB_2_guid);

        relation = (Map<String, Object>) tableauWB_0.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("DELETED".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_1.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));
        relation = (Map<String, Object>) tableauWB_2.getRelationshipAttribute(REL_TYPE_PROJECT);
        assertTrue("ACTIVE".equals(relation.get("relationshipStatus")));

        // 4. Link one + Unlink one Workbook
        tableauWB_0.setRelationshipAttribute(REL_TYPE_PROJECT, new AtlasObjectId(projectGuid, TYPE_PROJECT));
        tableauWB_2.setRelationshipAttribute(REL_TYPE_PROJECT, null);

        entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(tableauWB_0);
        entitiesWithExtInfo.addEntity(tableauWB_2);
        createEntitiesBulk(entitiesWithExtInfo);
        sleep(2);

        tableauProject = getEntity(projectGuid);

        relations = (List<Map<String, Object>>) tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS);
        assertEquals(4, relations.size());
        assertEquals(2, relations.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());
        assertEquals(2, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        tableauWB_0 = getEntity(tableauWB_0_guid);
        tableauWB_1 = getEntity(tableauWB_1_guid);
        tableauWB_2 = getEntity(tableauWB_2_guid);


        assertTrue("ACTIVE".equals(((Map<String, Object>) tableauWB_0.getRelationshipAttribute(REL_TYPE_PROJECT)).get("relationshipStatus")));
        assertTrue("ACTIVE".equals(((Map<String, Object>) tableauWB_1.getRelationshipAttribute(REL_TYPE_PROJECT)).get("relationshipStatus")));
        assertTrue("DELETED".equals(((Map<String, Object>) tableauWB_2.getRelationshipAttribute(REL_TYPE_PROJECT)).get("relationshipStatus")));


        // 5. Unlink all Workbooks from Project
        tableauWB_0.setRelationshipAttribute(REL_TYPE_PROJECT, null);
        tableauWB_1.setRelationshipAttribute(REL_TYPE_PROJECT, null);
        tableauWB_2.setRelationshipAttribute(REL_TYPE_PROJECT, null);

        entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(tableauWB_0);
        entitiesWithExtInfo.addEntity(tableauWB_1);
        entitiesWithExtInfo.addEntity(tableauWB_2);
        createEntitiesBulk(entitiesWithExtInfo);
        sleep(2);

        tableauProject = getEntity(projectGuid);

        relations = (List<Map<String, Object>>) tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS);
        assertEquals(4, relations.size());
        assertEquals(4, relations.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());

        tableauWB_0 = getEntity(tableauWB_0_guid);
        tableauWB_1 = getEntity(tableauWB_1_guid);
        tableauWB_2 = getEntity(tableauWB_2_guid);


        assertTrue("DELETED".equals(((Map<String, Object>) tableauWB_0.getRelationshipAttribute(REL_TYPE_PROJECT)).get("relationshipStatus")));
        assertTrue("DELETED".equals(((Map<String, Object>) tableauWB_1.getRelationshipAttribute(REL_TYPE_PROJECT)).get("relationshipStatus")));
        assertTrue("DELETED".equals(((Map<String, Object>) tableauWB_2.getRelationshipAttribute(REL_TYPE_PROJECT)).get("relationshipStatus")));

        // 6. Link all 3 Workbooks back to Project
        tableauWB_0.setRelationshipAttribute(REL_TYPE_PROJECT, new AtlasObjectId(projectGuid, TYPE_PROJECT));
        tableauWB_1.setRelationshipAttribute(REL_TYPE_PROJECT, new AtlasObjectId(projectGuid, TYPE_PROJECT));
        tableauWB_2.setRelationshipAttribute(REL_TYPE_PROJECT, new AtlasObjectId(projectGuid, TYPE_PROJECT));

        entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(tableauWB_0);
        entitiesWithExtInfo.addEntity(tableauWB_1);
        entitiesWithExtInfo.addEntity(tableauWB_2);
        createEntitiesBulk(entitiesWithExtInfo);
        sleep(2);

        tableauProject = getEntity(projectGuid);

        relations = (List<Map<String, Object>>) tableauProject.getRelationshipAttribute(REL_TYPE_WORKOBOOKS);
        assertEquals(7, relations.size());
        assertEquals(4, relations.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());
        assertEquals(3, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        tableauWB_0 = getEntity(tableauWB_0_guid);
        tableauWB_1 = getEntity(tableauWB_1_guid);
        tableauWB_2 = getEntity(tableauWB_2_guid);

        assertTrue("ACTIVE".equals(((Map<String, Object>) tableauWB_0.getRelationshipAttribute(REL_TYPE_PROJECT)).get("relationshipStatus")));
        assertTrue("ACTIVE".equals(((Map<String, Object>) tableauWB_1.getRelationshipAttribute(REL_TYPE_PROJECT)).get("relationshipStatus")));
        assertTrue("ACTIVE".equals(((Map<String, Object>) tableauWB_2.getRelationshipAttribute(REL_TYPE_PROJECT)).get("relationshipStatus")));

        LOG.info(">> relationshipManyToOne");
    }

    @Test
    private static void relationshipManyToMany() throws Exception {
        LOG.info(">> relationshipManyToMany");

        /*
         * TableauWorksheet n <> n TableauCalculatedField
         *
         * Update TableauCalculatedField
         * 1. Link one TableauWorksheet to TableauCalculatedField
         * 2. Link two more TableauWorksheets to TableauCalculatedField
         * 3. Unlink one TableauWorksheet from TableauCalculatedField
         * 4. Link one + Unlink one TableauWorksheet
         * 5. Unlink all TableauWorksheets from TableauCalculatedField
         * 6. Link all 3 TableauWorksheets back to TableauCalculatedField
         * */

        AtlasEntity calcField = getAtlasEntity(TYPE_CALC_FIELD, "calcField_0");
        String calcFieldGuid = createEntity(calcField).getGuidAssignments().values().iterator().next();

        AtlasEntity workSheet_0 = getAtlasEntity(TYPE_WORKOSHEET, "workSheet_0");
        AtlasEntity workSheet_1 = getAtlasEntity(TYPE_WORKOSHEET, "workSheet_1");
        AtlasEntity workSheet_2 = getAtlasEntity(TYPE_WORKOSHEET, "workSheet_2");

        String sheet_0_guid = createEntity(workSheet_0).getGuidAssignments().values().iterator().next();
        String sheet_1_guid = createEntity(workSheet_1).getGuidAssignments().values().iterator().next();
        String sheet_2_guid = createEntity(workSheet_2).getGuidAssignments().values().iterator().next();

        /*System.out.println(calcFieldGuid);
        System.out.println(sheet_0_guid);
        System.out.println(sheet_1_guid);
        System.out.println(sheet_2_guid);*/

        sleep(2);
        calcField = getEntity(calcFieldGuid);

        // 1. Link one TableauWorksheet to TableauCalculatedField
        calcField.setRelationshipAttribute(REL_TYPE_WORKSHEETS, getObjectIdsAsList(TYPE_WORKOSHEET, sheet_0_guid));
        createEntity(calcField);
        sleep(2);

        calcField = getEntity(calcFieldGuid);

        assertNotNull(calcField);
        assertNotNull(calcField.getRelationshipAttributes());
        assertNotNull(calcField.getRelationshipAttribute(REL_TYPE_WORKSHEETS));

        List<Map<String, Object>> relations = (List<Map<String, Object>>) calcField.getRelationshipAttribute(REL_TYPE_WORKSHEETS);
        assertEquals(1, relations.size());
        assertEquals(1, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        // 2. Link two more TableauWorksheets to TableauCalculatedField
        calcField.setRelationshipAttribute(REL_TYPE_WORKSHEETS, getObjectIdsAsList(TYPE_WORKOSHEET, sheet_0_guid, sheet_1_guid, sheet_2_guid));
        createEntity(calcField);
        sleep(2);

        calcField = getEntity(calcFieldGuid);

        relations = (List<Map<String, Object>>) calcField.getRelationshipAttribute(REL_TYPE_WORKSHEETS);
        assertEquals(3, relations.size());
        assertEquals(3, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        workSheet_0 = getEntity(sheet_0_guid);
        workSheet_1 = getEntity(sheet_1_guid);
        workSheet_2 = getEntity(sheet_2_guid);

        relations = (List<Map<String, Object>>) workSheet_0.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(1, relations.size());
        assertTrue("ACTIVE".equals(relations.get(0).get("relationshipStatus")));

        relations = (List<Map<String, Object>>) workSheet_1.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(1, relations.size());
        assertTrue("ACTIVE".equals(relations.get(0).get("relationshipStatus")));

        relations = (List<Map<String, Object>>) workSheet_2.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(1, relations.size());
        assertTrue("ACTIVE".equals(relations.get(0).get("relationshipStatus")));

        // 3. Unlink one TableauWorksheet from TableauCalculatedField
        calcField.setRelationshipAttribute(REL_TYPE_WORKSHEETS, getObjectIdsAsList(TYPE_WORKOBOOK, sheet_1_guid, sheet_2_guid));
        createEntity(calcField);
        sleep(2);

        calcField = getEntity(calcFieldGuid);

        relations = (List<Map<String, Object>>) calcField.getRelationshipAttribute(REL_TYPE_WORKSHEETS);
        assertEquals(3, relations.size());
        assertEquals(1, relations.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());
        assertEquals(2, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        workSheet_0 = getEntity(sheet_0_guid);
        workSheet_1 = getEntity(sheet_1_guid);
        workSheet_2 = getEntity(sheet_2_guid);

        relations = (List<Map<String, Object>>) workSheet_0.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(1, relations.size());
        assertTrue("DELETED".equals(relations.get(0).get("relationshipStatus")));

        relations = (List<Map<String, Object>>) workSheet_1.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(1, relations.size());
        assertTrue("ACTIVE".equals(relations.get(0).get("relationshipStatus")));

        relations = (List<Map<String, Object>>) workSheet_2.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(1, relations.size());
        assertTrue("ACTIVE".equals(relations.get(0).get("relationshipStatus")));

        // 4. Link one + Unlink one TableauWorksheet
        calcField.setRelationshipAttribute(REL_TYPE_WORKSHEETS, getObjectIdsAsList(TYPE_WORKOBOOK, sheet_0_guid, sheet_1_guid));
        createEntity(calcField);
        sleep(2);

        calcField = getEntity(calcFieldGuid);

        relations = (List<Map<String, Object>>) calcField.getRelationshipAttribute(REL_TYPE_WORKSHEETS);
        assertEquals(4, relations.size());
        assertEquals(2, relations.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());
        assertEquals(2, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        workSheet_0 = getEntity(sheet_0_guid);
        workSheet_1 = getEntity(sheet_1_guid);
        workSheet_2 = getEntity(sheet_2_guid);

        relations = (List<Map<String, Object>>) workSheet_0.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(2, relations.size());
        assertTrue("DELETED".equals(relations.get(0).get("relationshipStatus")));

        relations = (List<Map<String, Object>>) workSheet_1.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(1, relations.size());
        assertTrue("ACTIVE".equals(relations.get(0).get("relationshipStatus")));

        relations = (List<Map<String, Object>>) workSheet_2.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(1, relations.size());
        assertTrue("DELETED".equals(relations.get(0).get("relationshipStatus")));


        // 5. Unlink all TableauWorksheets from TableauCalculatedField
        calcField.setRelationshipAttribute(REL_TYPE_WORKSHEETS, getObjectIdsAsList(TYPE_WORKOBOOK));
        createEntity(calcField);
        sleep(2);

        calcField = getEntity(calcFieldGuid);

        relations = (List<Map<String, Object>>) calcField.getRelationshipAttribute(REL_TYPE_WORKSHEETS);
        assertEquals(4, relations.size());
        assertEquals(4, relations.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());

        workSheet_0 = getEntity(sheet_0_guid);
        workSheet_1 = getEntity(sheet_1_guid);
        workSheet_2 = getEntity(sheet_2_guid);

        relations = (List<Map<String, Object>>) workSheet_0.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(2, relations.size());
        assertTrue("DELETED".equals(relations.get(1).get("relationshipStatus")));

        relations = (List<Map<String, Object>>) workSheet_1.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(1, relations.size());
        assertTrue("DELETED".equals(relations.get(0).get("relationshipStatus")));

        relations = (List<Map<String, Object>>) workSheet_2.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(1, relations.size());
        assertTrue("DELETED".equals(relations.get(0).get("relationshipStatus")));

        // 6. Link all 3 TableauWorksheets back to TableauCalculatedField
        calcField.setRelationshipAttribute(REL_TYPE_WORKSHEETS, getObjectIdsAsList(TYPE_WORKOBOOK, sheet_0_guid, sheet_1_guid, sheet_2_guid));
        createEntity(calcField);
        sleep(2);

        calcField = getEntity(calcFieldGuid);

        relations = (List<Map<String, Object>>) calcField.getRelationshipAttribute(REL_TYPE_WORKSHEETS);
        assertEquals(7, relations.size());
        assertEquals(4, relations.stream().filter(x -> "DELETED".equals(x.get("relationshipStatus"))).count());
        assertEquals(3, relations.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).count());

        workSheet_0 = getEntity(sheet_0_guid);
        workSheet_1 = getEntity(sheet_1_guid);
        workSheet_2 = getEntity(sheet_2_guid);

        relations = (List<Map<String, Object>>) workSheet_0.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(3, relations.size());
        assertTrue("ACTIVE".equals(relations.get(2).get("relationshipStatus")));

        relations = (List<Map<String, Object>>) workSheet_1.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(2, relations.size());
        assertTrue("ACTIVE".equals(relations.get(1).get("relationshipStatus")));

        relations = (List<Map<String, Object>>) workSheet_2.getRelationshipAttribute(REL_TYPE_CALC_FIELDS);
        assertEquals(2, relations.size());
        assertTrue("ACTIVE".equals(relations.get(1).get("relationshipStatus")));

        LOG.info(">> relationshipManyToMany");
    }

    @Test
    private static void arrayOfMaps() throws Exception {
        LOG.info(">> arrayOfMaps");

        /*
         * TableauWorkbook.projectHierarchy ==> array<map<string,string>>
         *
         * Update TableauWorksheet
         * 1. Add one value in list
         * 2. Add two more values in list
         * 3. Remove one value from list
         * 4. Add one + Remove one value
         * 5. Remove all values from list
         * 6. Add all 3 TableauWorksheets back to list
         * */

        AtlasEntity workbook = getAtlasEntity(TYPE_WORKOBOOK, "workbook_0");
        String workbookGuid = createEntity(workbook).getGuidAssignments().values().iterator().next();

        //System.out.println(workbookGuid);

        sleep(2);
        workbook = getEntity(workbookGuid);

        // 1.Add one value in list
        workbook.setAttribute(ATTR_PROJECT_HIERARCHY, Collections.singleton(mapOf("key_1", "value_1")));
        createEntity(workbook);

        sleep(2);
        workbook = getEntity(workbookGuid);

        assertNotNull(workbook.getAttribute(ATTR_PROJECT_HIERARCHY));
        List<Map<String, String>> projectHierarchy = (List<Map<String, String>>) workbook.getAttribute(ATTR_PROJECT_HIERARCHY);

        assertEquals(1, projectHierarchy.size());
        assertEquals("value_1", projectHierarchy.get(0).get("key_1"));


        // 2. Add two more values in list
        projectHierarchy = new ArrayList<>(3);
        projectHierarchy.add(mapOf("key_0", "value_0"));
        projectHierarchy.add(mapOf("key_1", "value_1"));
        projectHierarchy.add(mapOf("key_2", "value_2"));
        workbook.setAttribute(ATTR_PROJECT_HIERARCHY, projectHierarchy);
        createEntity(workbook);

        sleep(2);
        workbook = getEntity(workbookGuid);

        assertNotNull(workbook.getAttribute(ATTR_PROJECT_HIERARCHY));
        projectHierarchy = (List<Map<String, String>>) workbook.getAttribute(ATTR_PROJECT_HIERARCHY);

        assertEquals(3, projectHierarchy.size());
        assertEquals("value_0", projectHierarchy.get(0).get("key_0"));
        assertEquals("value_1", projectHierarchy.get(1).get("key_1"));
        assertEquals("value_2", projectHierarchy.get(2).get("key_2"));

        // 3. Remove one value from list
        projectHierarchy = new ArrayList<>(2);
        projectHierarchy.add(mapOf("key_0", "value_0"));
        projectHierarchy.add(mapOf("key_2", "value_2"));
        workbook.setAttribute(ATTR_PROJECT_HIERARCHY, projectHierarchy);
        createEntity(workbook);

        sleep(2);
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

        sleep(2);
        workbook = getEntity(workbookGuid);

        assertNotNull(workbook.getAttribute(ATTR_PROJECT_HIERARCHY));
        projectHierarchy = (List<Map<String, String>>) workbook.getAttribute(ATTR_PROJECT_HIERARCHY);

        assertEquals(2, projectHierarchy.size());
        assertEquals("value_1", projectHierarchy.get(0).get("key_1"));
        assertEquals("value_2", projectHierarchy.get(1).get("key_2"));

        // 5. Remove all values from list
        workbook.setAttribute(ATTR_PROJECT_HIERARCHY, null);
        createEntity(workbook);

        sleep(2);
        workbook = getEntity(workbookGuid);

        assertNull(workbook.getAttribute(ATTR_PROJECT_HIERARCHY));

        // 6. Add all 3 TableauWorksheets back to list
        projectHierarchy = new ArrayList<>(3);
        projectHierarchy.add(mapOf("key_0", "value_0"));
        projectHierarchy.add(mapOf("key_2", "value_2"));
        projectHierarchy.add(mapOf("key_1", "value_1"));
        workbook.setAttribute(ATTR_PROJECT_HIERARCHY, projectHierarchy);
        createEntity(workbook);

        sleep(2);
        workbook = getEntity(workbookGuid);

        assertNotNull(workbook.getAttribute(ATTR_PROJECT_HIERARCHY));
        projectHierarchy = (List<Map<String, String>>) workbook.getAttribute(ATTR_PROJECT_HIERARCHY);

        assertEquals(3, projectHierarchy.size());
        assertEquals("value_0", projectHierarchy.get(0).get("key_0"));
        assertEquals("value_2", projectHierarchy.get(1).get("key_2"));
        assertEquals("value_1", projectHierarchy.get(2).get("key_1"));


        LOG.info(">> arrayOfMaps");
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

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "table_0");
        String tableGuid = createEntity(table).getGuidAssignments().values().iterator().next();

        //System.out.println(tableGuid);

        sleep(2);
        table = getEntity(tableGuid);

        // 1.Add one value in list
        table.setAttribute(ATTR_OWNER_USERS, Collections.singleton("value_1"));
        createEntity(table);

        sleep(2);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_OWNER_USERS));
        List<String> ownerUsers = (List<String>) table.getAttribute(ATTR_OWNER_USERS);

        assertEquals(1, ownerUsers.size());
        assertEquals("value_1", ownerUsers.get(0));


        // 2. Add two more values in list
        ownerUsers = new ArrayList<>(3);
        ownerUsers.add("value_0");
        ownerUsers.add("value_1");
        ownerUsers.add("value_2");
        table.setAttribute(ATTR_OWNER_USERS, ownerUsers);
        createEntity(table);

        sleep(2);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_OWNER_USERS));
        ownerUsers = (List<String>) table.getAttribute(ATTR_OWNER_USERS);

        assertEquals(3, ownerUsers.size());
        assertEquals("value_0", ownerUsers.get(0));
        assertEquals("value_1", ownerUsers.get(1));
        assertEquals("value_2", ownerUsers.get(2));

        // 3. Remove one value from list
        ownerUsers = new ArrayList<>(2);
        ownerUsers.add("value_0");
        ownerUsers.add("value_2");
        table.setAttribute(ATTR_OWNER_USERS, ownerUsers);
        createEntity(table);

        sleep(2);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_OWNER_USERS));
        ownerUsers = (List<String>) table.getAttribute(ATTR_OWNER_USERS);

        assertEquals(2, ownerUsers.size());
        assertEquals("value_0", ownerUsers.get(0));
        assertEquals("value_2", ownerUsers.get(1));

        // 4. Add one + Remove one value
        ownerUsers = new ArrayList<>(2);
        ownerUsers.add("value_1");
        ownerUsers.add("value_2");
        table.setAttribute(ATTR_OWNER_USERS, ownerUsers);
        createEntity(table);

        sleep(2);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_OWNER_USERS));
        ownerUsers = (List<String>) table.getAttribute(ATTR_OWNER_USERS);

        assertEquals(2, ownerUsers.size());
        assertEquals("value_1", ownerUsers.get(0));
        assertEquals("value_2", ownerUsers.get(1));

        // 5. Remove all values from list
        table.setAttribute(ATTR_OWNER_USERS, null);
        createEntity(table);

        sleep(2);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_OWNER_USERS));
        assertEquals(0, ((List) table.getAttribute(ATTR_OWNER_USERS)).size());

        // 6. Add all 3 TableauWorksheets back to list
        ownerUsers = new ArrayList<>(3);
        ownerUsers.add("value_0");
        ownerUsers.add("value_2");
        ownerUsers.add("value_1");
        table.setAttribute(ATTR_OWNER_USERS, ownerUsers);
        createEntity(table);

        sleep(2);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_OWNER_USERS));
        ownerUsers = (List<String>) table.getAttribute(ATTR_OWNER_USERS);

        assertEquals(3, ownerUsers.size());
        assertTrue(ownerUsers.contains("value_0"));
        assertTrue(ownerUsers.contains("value_2"));
        assertTrue(ownerUsers.contains("value_1"));


        LOG.info(">> arrayOfStrings");
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

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "table_0");
        String tableGuid = createEntity(table).getGuidAssignments().values().iterator().next();

        //System.out.println(tableGuid);

        sleep(2);
        table = getEntity(tableGuid);

        // 1. Add one value in list
        List<AtlasStruct> structs = new ArrayList<>();
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_0")));
        table.setAttribute(ATTR_USER_RECORD_LIST, structs);

        createEntity(table);
        sleep(2);
        table = getEntity(tableGuid);

        assertNotNull(table.getAttribute(ATTR_USER_RECORD_LIST));

        List<Map> structsAsMap = (List<Map>) table.getAttribute(ATTR_USER_RECORD_LIST);
        assertEquals(1, structsAsMap.size());
        assertEquals(STRUCT_TYPE_POPULARITY_INSIGHTS, structsAsMap.get(0).get("typeName"));
        assertEquals("user_0", ((Map)((List<Map>)table.getAttribute(ATTR_USER_RECORD_LIST)).get(0).get("attributes")).get("recordUser"));


        // 2. Add two more values in list
        structs = new ArrayList<>();
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_0")));
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_1")));
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_2")));
        table.setAttribute(ATTR_USER_RECORD_LIST, structs);

        createEntity(table);
        sleep(5);
        table = getEntity(tableGuid);

        structsAsMap = (List<Map>) table.getAttribute(ATTR_USER_RECORD_LIST);
        assertEquals(3, structsAsMap.size());
        assertEquals(STRUCT_TYPE_POPULARITY_INSIGHTS, structsAsMap.get(0).get("typeName"));
        List<String> users = ((List<Map>)table.getAttribute(ATTR_USER_RECORD_LIST)).stream().map(x -> ((Map) x.get("attributes")).get("recordUser").toString()).collect(Collectors.toList());
        assertTrue(users.contains("user_0"));
        assertTrue(users.contains("user_1"));
        assertTrue(users.contains("user_2"));

        // 3. Remove one value from list
        structs = new ArrayList<>();
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_0")));
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_2")));
        table.setAttribute(ATTR_USER_RECORD_LIST, structs);

        createEntity(table);
        sleep(2);
        table = getEntity(tableGuid);

        structsAsMap = (List<Map>) table.getAttribute(ATTR_USER_RECORD_LIST);
        assertEquals(2, structsAsMap.size());
        assertEquals(STRUCT_TYPE_POPULARITY_INSIGHTS, structsAsMap.get(0).get("typeName"));
        users = ((List<Map>)table.getAttribute(ATTR_USER_RECORD_LIST)).stream().map(x -> ((Map) x.get("attributes")).get("recordUser").toString()).collect(Collectors.toList());
        assertTrue(users.contains("user_0"));
        assertTrue(users.contains("user_2"));

        // 4. Add one + Remove one value
        structs = new ArrayList<>();
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_1")));
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_0")));
        table.setAttribute(ATTR_USER_RECORD_LIST, structs);

        createEntity(table);
        sleep(2);
        table = getEntity(tableGuid);

        structsAsMap = (List<Map>) table.getAttribute(ATTR_USER_RECORD_LIST);
        assertEquals(2, structsAsMap.size());
        assertEquals(STRUCT_TYPE_POPULARITY_INSIGHTS, structsAsMap.get(0).get("typeName"));
        users = ((List<Map>)table.getAttribute(ATTR_USER_RECORD_LIST)).stream().map(x -> ((Map) x.get("attributes")).get("recordUser").toString()).collect(Collectors.toList());
        assertTrue(users.contains("user_1"));
        assertTrue(users.contains("user_0"));

        // 5. Remove all values from list
        structs = new ArrayList<>();
        table.setAttribute(ATTR_USER_RECORD_LIST, structs);

        createEntity(table);
        sleep(2);
        table = getEntity(tableGuid);

        structsAsMap = (List<Map>) table.getAttribute(ATTR_USER_RECORD_LIST);
        assertEquals(0, structsAsMap.size());


        // 6. Add all 3 TableauWorksheets back to list
        structs = new ArrayList<>();
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_0")));
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_1")));
        structs.add(new AtlasStruct(STRUCT_TYPE_POPULARITY_INSIGHTS, mapOf("recordUser", "user_2")));
        table.setAttribute(ATTR_USER_RECORD_LIST, structs);

        createEntity(table);
        sleep(2);
        table = getEntity(tableGuid);

        structsAsMap = (List<Map>) table.getAttribute(ATTR_USER_RECORD_LIST);
        assertEquals(3, structsAsMap.size());
        assertEquals(STRUCT_TYPE_POPULARITY_INSIGHTS, structsAsMap.get(0).get("typeName"));
        users = ((List<Map>)table.getAttribute(ATTR_USER_RECORD_LIST)).stream().map(x -> ((Map) x.get("attributes")).get("recordUser").toString()).collect(Collectors.toList());
        assertTrue(users.contains("user_0"));
        assertTrue(users.contains("user_1"));
        assertTrue(users.contains("user_2"));


        LOG.info(">> arrayOfStructs");
    }
}