package com.tests.main.tests.authz;


import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.TestUtil;
import okhttp3.OkHttpClient;
import org.apache.atlas.model.instance.AtlasEntity;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tests.main.tests.authz.Authz.getMatchedPolicyIdFromAccessLogs;
import static com.tests.main.utils.ESUtil.searchWithQueryBuilderAccess;
import static com.tests.main.utils.TestUtil.CONNECTION_PREFIX;
import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.TYPE_POLICY;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getMap;
import static com.tests.main.utils.TestUtil.setAtlasClient;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.toJson;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class AuthzOnlyABAC implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(AuthzOnlyABAC.class);

    private static final String ABAC_SERVICE = "atlas_abac";

    private static OkHttpClient ARGO;
    private static OkHttpClient ADMIN;

    private static String CONNECTION_QN;
    private static AtlasEntity SAMPLE_POLICY_BOOT;
    private static AtlasEntity SAMPLE_POLICY_0;
    private static AtlasEntity SAMPLE_POLICY_1;
    private static AtlasEntity SAMPLE_POLICY_2;

    public static void main(String[] args) throws Exception {
        try {
            new AuthzOnlyABAC().run();
        } finally {
            System.out.println("Connection: " + CONNECTION_QN);
            setAtlasClient(ARGO);
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running AuthzOnlyABAC tests");

        long start = System.currentTimeMillis();

        ARGO = TestUtil.getClientLocal("service-account-atlan-argo");
        ADMIN = TestUtil.getClientLocal("nikhil.bonte");

        setAtlasClient(ARGO);

        CONNECTION_QN = "default/snowflake/dummy_abac_conn";
        CONNECTION_PREFIX = CONNECTION_QN + "/";

        try {
            //createConnection();
            addBootPolicytoAllow();  //SAMPLE_POLICY_BOOT

            checkCreateTableAllowed();

            addAllowResourcePolicyWithHighPriority(); //SAMPLE_POLICY_0

            // 1 normal allow + 1 override allow
            checkCreateTableAllowedWithHighPriorityPolicy();

            addDenyResourcePolicyWithNormalPriority(); //update SAMPLE_POLICY_0 itself

            // 1 normal allow + 1 normal deny
            checkCreateTableDeniedWithLowPriorityPolicy();

            addAnotherAllowResourcePolicyWithHighPriority(); //SAMPLE_POLICY_1

            // 1 normal allow + 1 normal deny + 1 override allow
            checkAnotherCreateTableWithHighPriorityPolicy();

            addAnotherDenyResourcePolicyWithHighPriority();

            // 1 normal allow + 1 normal deny + 1 override allow + 1 override deny
            checkAnotherCreateTableDenyWithHighPriorityPolicy();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running AuthzOnlyABAC tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void addBootPolicytoAllow() throws Exception {
        LOG.info(">> addBootPolicytoAllow");

        setAtlasClient(ARGO);

        SAMPLE_POLICY_BOOT = getABACPolicy("policy_boot", "allow", 0);

        String policy_guid = createEntity(SAMPLE_POLICY_BOOT).getCreatedEntities().get(0).getGuid();

        sleep();
        SAMPLE_POLICY_BOOT = getEntity(policy_guid);

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> addBootPolicytoAllow");
    }

    private static void checkCreateTableAllowed() throws Exception {
        LOG.info(">> checkCreateTableAllowed");

        setAtlasClient(ADMIN);

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "table_0");
        table.setAttribute("connectionQualifiedName", CONNECTION_QN);
        String table_guid = createEntity(table).getCreatedEntities().get(0).getGuid();

        sleep();
        table = getEntity(table_guid);

        assertEquals(CONNECTION_PREFIX + "table_0", table.getAttribute(NAME));
        assertEquals(CONNECTION_PREFIX + "table_0", table.getAttribute(QUALIFIED_NAME));

        sleep(3);

        //Map<String, Object> accessLog = getMatchedPolicyIdFromAccessLogs(TYPE_TABLE, (String) table.getAttribute(QUALIFIED_NAME), ADMIN.getBasicAuthUser());

        //assertEquals(SAMPLE_POLICY_BOOT.getGuid(), (String) accessLog.get("policyId"));

        LOG.info(">> checkCreateTableAllowed");
    }

    private static void addAllowResourcePolicyWithHighPriority() throws Exception {
        LOG.info(">> addAllowResourcePolicyWithHighPriority");

        setAtlasClient(ARGO);

        SAMPLE_POLICY_0 = getABACPolicy("policy_0", "allow", 1);

        String policy_guid = createEntity(SAMPLE_POLICY_0).getCreatedEntities().get(0).getGuid();

        sleep();
        SAMPLE_POLICY_0 = getEntity(policy_guid);

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> addAllowResourcePolicyWithHighPriority");
    }

    private static void checkCreateTableAllowedWithHighPriorityPolicy() throws Exception {
        LOG.info(">> checkCreateTableAllowedWithHighPriorityPolicy");

        setAtlasClient(ADMIN);

        String tableName = "table_1";

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, tableName);
        table.setAttribute("connectionQualifiedName", CONNECTION_QN);
        String table_guid = createEntity(table).getCreatedEntities().get(0).getGuid();

        sleep();
        table = getEntity(table_guid);

        assertEquals(CONNECTION_PREFIX + tableName, table.getAttribute(NAME));
        assertEquals(CONNECTION_PREFIX + tableName, table.getAttribute(QUALIFIED_NAME));

        sleep(5);

        //Map<String, Object> accessLog = getMatchedPolicyIdFromAccessLogs(TYPE_TABLE, (String) table.getAttribute(QUALIFIED_NAME), ADMIN.getBasicAuthUser());
        //String policyId = (String) accessLog.get("policyId");

        //assertEquals(SAMPLE_POLICY_0.getGuid(), policyId);

        LOG.info(">> checkCreateTableAllowedWithHighPriorityPolicy");
    }

    private static void addDenyResourcePolicyWithNormalPriority() throws Exception {
        LOG.info(">> addDenyResourcePolicyWithNormalPriority");

        setAtlasClient(ARGO);

        SAMPLE_POLICY_0.setAttribute("policyType", "deny");
        SAMPLE_POLICY_0.setAttribute("policyPriority", 0);

        String policy_guid = createEntity(SAMPLE_POLICY_0).getUpdatedEntities().get(0).getGuid();

        sleep();
        SAMPLE_POLICY_0 = getEntity(SAMPLE_POLICY_0.getGuid());

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> addDenyResourcePolicyWithNormalPriority");
    }

    // 1 normal allow + 1 normal deny
    private static void checkCreateTableDeniedWithLowPriorityPolicy() throws Exception {
        LOG.info(">> checkCreateTableAllowedWithHighPriorityPolicy");

        setAtlasClient(ADMIN);

        String tableName = "table_2";

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, tableName);
        table.setAttribute("connectionQualifiedName", CONNECTION_QN);
        boolean failed = false;
        try {
            createEntity(table).getCreatedEntities().get(0).getGuid();
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 403);
            assertTrue(exception.getMessage().contains("is not authorized to perform"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }
        sleep(5);

        //Map<String, Object> accessLog = getMatchedPolicyIdFromAccessLogs(TYPE_TABLE, (String) table.getAttribute(QUALIFIED_NAME), ADMIN.getBasicAuthUser(), false, true);

        //assertEquals(SAMPLE_POLICY_0.getGuid(), (String) accessLog.get("policyId"));

        LOG.info(">> checkCreateTableAllowedWithHighPriorityPolicy");
    }

    private static void addAnotherAllowResourcePolicyWithHighPriority() throws Exception {
        LOG.info(">> addAnotherAllowResourcePolicyWithHighPriority");

        setAtlasClient(ARGO);

        SAMPLE_POLICY_1 =  getABACPolicy("policy_1", "allow", 1);

        String policy_guid = createEntity(SAMPLE_POLICY_1).getCreatedEntities().get(0).getGuid();

        sleep();
        SAMPLE_POLICY_1 = getEntity(policy_guid);

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> addAnotherAllowResourcePolicyWithHighPriority");
    }

    // 1 normal allow + 1 normal deny + 1 override allow
    private static void checkAnotherCreateTableWithHighPriorityPolicy() throws Exception {
        LOG.info(">> checkAnotherCreateTableWithHighPriorityPolicy");

        setAtlasClient(ADMIN);

        String tableName = "table_3";

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, tableName);
        table.setAttribute("connectionQualifiedName", CONNECTION_QN);
        String table_guid = createEntity(table).getCreatedEntities().get(0).getGuid();

        sleep();
        table = getEntity(table_guid);

        assertEquals(CONNECTION_PREFIX + tableName, table.getAttribute(NAME));
        assertEquals(CONNECTION_PREFIX + tableName, table.getAttribute(QUALIFIED_NAME));

        sleep(5);

        //Map<String, Object> accessLog = getMatchedPolicyIdFromAccessLogs(TYPE_TABLE, (String) table.getAttribute(QUALIFIED_NAME), ADMIN.getBasicAuthUser());
        //String policyId = (String) accessLog.get("policyId");

        //assertTrue(SAMPLE_POLICY_1.getGuid().equals(policyId));

        LOG.info(">> checkAnotherCreateTableWithHighPriorityPolicy");
    }

    private static void addAnotherDenyResourcePolicyWithHighPriority() throws Exception {
        LOG.info(">> addAnotherDenyResourcePolicyWithHighPriority");

        setAtlasClient(ARGO);

        SAMPLE_POLICY_2 = getABACPolicy("policy_2", "deny", 1);

        String policy_guid = createEntity(SAMPLE_POLICY_2).getCreatedEntities().get(0).getGuid();

        sleep();
        SAMPLE_POLICY_2 = getEntity(policy_guid);

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> addAnotherDenyResourcePolicyWithHighPriority");
    }


    private static void checkAnotherCreateTableDenyWithHighPriorityPolicy() throws Exception {
        LOG.info(">> checkAnotherCreateTableDenyWithHighPriorityPolicy");

        setAtlasClient(ADMIN);

        String tableName = "table_4";

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, tableName);
        table.setAttribute("connectionQualifiedName", CONNECTION_QN);
        boolean failed = false;
        try {
            createEntity(table).getCreatedEntities().get(0).getGuid();
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 403);
            assertTrue(exception.getMessage().contains("is not authorized to perform"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        sleep(5);

        /*Map<String, Object> accessLog = getMatchedPolicyIdFromAccessLogs(TYPE_TABLE, (String) table.getAttribute(QUALIFIED_NAME), ADMIN.getBasicAuthUser(), false, true);
        String policyId = (String) accessLog.get("policyId");

        assertTrue(SAMPLE_POLICY_2.getGuid().equals(policyId));*/

        LOG.info(">> checkAnotherCreateTableDenyWithHighPriorityPolicy");
    }

    //------------------

    private static AtlasEntity getABACPolicy(String name, String policyType, int priority) {

        List<Map<String, Object>> orClauses = new ArrayList<>();
        orClauses.add(getMap("attributeName", "qualifiedName",
                "attributeValue", CONNECTION_QN,
                "operator", "EQUALS"));
        orClauses.add(getMap("attributeName", "connectionQualifiedName",
                "attributeValue", CONNECTION_QN,
                "operator", "EQUALS"));

        Map<String, Object> filterCriteriaAsMap = getMap("entity", getMap("condition", "OR", "criterion", orClauses));

        AtlasEntity atlasEntity = new AtlasEntity(TYPE_POLICY);
        atlasEntity.setAttribute(QUALIFIED_NAME, "abac/" + name);
        atlasEntity.setAttribute(NAME, name);
        atlasEntity.setAttribute("policyType", policyType);
        atlasEntity.setAttribute("policyPriority", priority);
        //atlasEntity.setAttribute("policyUsers", Collections.singletonList(ADMIN.getBasicAuthUser()));
        atlasEntity.setAttribute("policyFilterCriteria", toJson(filterCriteriaAsMap));

        atlasEntity.setAttribute("policyActions", Arrays.asList("entity-read", "entity-create", "entity-update", "entity-delete"));
        atlasEntity.setAttribute("policyCategory", "bootstrap");
        atlasEntity.setAttribute("policySubCategory", "default");
        atlasEntity.setAttribute("policyResourceCategory", "ENTITY");
        atlasEntity.setAttribute("policyServiceName", ABAC_SERVICE);

        return atlasEntity;
    }
}