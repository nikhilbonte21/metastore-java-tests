package com.tests.main.tests.authz;


import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.TestUtil;
import okhttp3.OkHttpClient;
import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.tests.main.tests.authz.Authz.getMatchedPolicyIdFromAccessLogs;
import static com.tests.main.tests.authz.Authz.populateConnectionPolicies;
import static com.tests.main.utils.TestUtil.CONNECTION_PREFIX;
import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.TYPE_POLICY;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.setAtlasClient;
import static com.tests.main.utils.TestUtil.sleep;
import static org.junit.Assert.*;


public class AuthzUsual implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(AuthzUsual.class);

    private static OkHttpClient ARGO;
    private static OkHttpClient ADMIN;

    private static Map<String, String> CONN_POLICIES;
    private static String CONNECTION_QN;
    private static AtlasEntity SAMPLE_POLICY_0;
    private static AtlasEntity SAMPLE_POLICY_1;
    private static AtlasEntity SAMPLE_POLICY_2;

    public static void main(String[] args) throws Exception {
        try {
            new AuthzUsual().run();
        } finally {
            System.out.println("Connection: " + CONNECTION_QN);
            setAtlasClient(ARGO);
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running AuthzUsual tests");

        long start = System.currentTimeMillis();

        ARGO = TestUtil.getClientLocal("service-account-atlan-argo");
        ADMIN = TestUtil.getClientLocal("nikhil.bonte");

        setAtlasClient(ARGO);
        try {
            createConnection();
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
            LOG.info("Completed running AuthzUsual tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void createConnection() throws Exception {
        LOG.info(">> createConnection");

        setAtlasClient(ARGO);

        CONNECTION_QN = String.format("default/snowflake/%s", getRandomName());

        System.out.println("ConnectionName: "+ CONNECTION_QN);

        AtlasEntity connection = new AtlasEntity("Connection");
        connection.setAttribute(QUALIFIED_NAME, CONNECTION_QN);
        connection.setAttribute("name", "connection_00");
        connection.setAttribute("adminUsers", Collections.singletonList("nikhil.bonte"));

        String guid = createEntity(connection).getCreatedEntities().get(0).getGuid();

        TestUtil.CONNECTION_PREFIX = CONNECTION_QN + "/";

        CONN_POLICIES = populateConnectionPolicies(guid);

        LOG.info(">> createConnection");
    }

    private static void checkCreateTableAllowed() throws Exception {
        LOG.info(">> checkCreateTableAllowed");
        sleep(15);

        setAtlasClient(ADMIN);

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "table_0");
        String table_guid = createEntity(table).getCreatedEntities().get(0).getGuid();

        sleep();
        table = getEntity(table_guid);

        assertEquals(CONNECTION_PREFIX + "table_0", table.getAttribute(NAME));
        assertEquals(CONNECTION_PREFIX + "table_0", table.getAttribute(QUALIFIED_NAME));

        sleep(3);

        /*Map<String, Object> accessLog = getMatchedPolicyIdFromAccessLogs(TYPE_TABLE, (String) table.getAttribute(QUALIFIED_NAME), ADMIN.getBasicAuthUser());
        assertTrue(CONN_POLICIES.containsKey((String) accessLog.get("policyId")));*/

        LOG.info(">> checkCreateTableAllowed");
    }

    private static void addAllowResourcePolicyWithHighPriority() throws Exception {
        LOG.info(">> addAllowResourcePolicyWithHighPriority");

        setAtlasClient(ARGO);

        String connectionResource = String.format("entity:%s/*", CONNECTION_QN);
        List<String> policyResources = new ArrayList<>();
        policyResources.add(connectionResource);
        policyResources.add("entity-type:*");
        policyResources.add("entity-classification:*");

        AtlasEntity policy = new AtlasEntity(TYPE_POLICY);
        policy.setAttribute(QUALIFIED_NAME, "policy_0");
        policy.setAttribute(NAME, "policy_0");
        //policy.setAttribute("policyUsers", Collections.singletonList(ADMIN.getBasicAuthUser()));
        policy.setAttribute("policyResources", policyResources);
        policy.setAttribute("policyActions", Arrays.asList("entity-create"));
        policy.setAttribute("policyCategory", "bootstrap");
        policy.setAttribute("policySubCategory", "default");
        policy.setAttribute("policyServiceName", "atlas");
        policy.setAttribute("policyType", "allow");
        policy.setAttribute("policyResourceCategory", "ENTITY");
        policy.setAttribute("policyPriority", 1);

        String policy_guid = createEntity(policy).getCreatedEntities().get(0).getGuid();

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
        String table_guid = createEntity(table).getCreatedEntities().get(0).getGuid();

        sleep();
        table = getEntity(table_guid);

        assertEquals(CONNECTION_PREFIX + tableName, table.getAttribute(NAME));
        assertEquals(CONNECTION_PREFIX + tableName, table.getAttribute(QUALIFIED_NAME));

        sleep(5);

        /*Map<String, Object> accessLog = getMatchedPolicyIdFromAccessLogs(TYPE_TABLE, (String) table.getAttribute(QUALIFIED_NAME), ADMIN.getBasicAuthUser());
        String policyId = (String) accessLog.get("policyId");*/

        //assertTrue(SAMPLE_POLICY_0.getGuid().equals(policyId) || CONN_POLICIES.containsKey(policyId));

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

    private static void checkCreateTableDeniedWithLowPriorityPolicy() throws Exception {
        LOG.info(">> checkCreateTableAllowedWithHighPriorityPolicy");

        setAtlasClient(ADMIN);

        String tableName = "table_2";

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, tableName);
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

        assertEquals(SAMPLE_POLICY_0.getGuid(), (String) accessLog.get("policyId"));*/

        LOG.info(">> checkCreateTableAllowedWithHighPriorityPolicy");
    }

    private static void addAnotherAllowResourcePolicyWithHighPriority() throws Exception {
        LOG.info(">> addAnotherAllowResourcePolicyWithHighPriority");

        setAtlasClient(ARGO);

        String connectionResource = String.format("entity:%s/*", CONNECTION_QN);
        List<String> policyResources = new ArrayList<>();
        policyResources.add(connectionResource);
        policyResources.add("entity-type:*");
        policyResources.add("entity-classification:*");

        SAMPLE_POLICY_1 = new AtlasEntity(TYPE_POLICY);
        SAMPLE_POLICY_1.setAttribute(QUALIFIED_NAME, "policy_1");
        SAMPLE_POLICY_1.setAttribute(NAME, "policy_1");
        //SAMPLE_POLICY_1.setAttribute("policyUsers", Collections.singletonList(ADMIN.getBasicAuthUser()));
        SAMPLE_POLICY_1.setAttribute("policyResources", policyResources);
        SAMPLE_POLICY_1.setAttribute("policyActions", Arrays.asList("entity-create"));
        SAMPLE_POLICY_1.setAttribute("policyCategory", "bootstrap");
        SAMPLE_POLICY_1.setAttribute("policySubCategory", "default");
        SAMPLE_POLICY_1.setAttribute("policyServiceName", "atlas");
        SAMPLE_POLICY_1.setAttribute("policyType", "allow");
        SAMPLE_POLICY_1.setAttribute("policyResourceCategory", "ENTITY");
        SAMPLE_POLICY_1.setAttribute("policyPriority", 1);

        String policy_guid = createEntity(SAMPLE_POLICY_1).getCreatedEntities().get(0).getGuid();

        sleep();
        SAMPLE_POLICY_1 = getEntity(policy_guid);

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> addAnotherAllowResourcePolicyWithHighPriority");
    }


    private static void checkAnotherCreateTableWithHighPriorityPolicy() throws Exception {
        LOG.info(">> checkAnotherCreateTableWithHighPriorityPolicy");

        setAtlasClient(ADMIN);

        String tableName = "table_3";

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, tableName);
        String table_guid = createEntity(table).getCreatedEntities().get(0).getGuid();

        sleep();
        table = getEntity(table_guid);

        assertEquals(CONNECTION_PREFIX + tableName, table.getAttribute(NAME));
        assertEquals(CONNECTION_PREFIX + tableName, table.getAttribute(QUALIFIED_NAME));

        sleep(5);

        /*Map<String, Object> accessLog = getMatchedPolicyIdFromAccessLogs(TYPE_TABLE, (String) table.getAttribute(QUALIFIED_NAME), ADMIN.getBasicAuthUser());
        String policyId = (String) accessLog.get("policyId");

        assertTrue(SAMPLE_POLICY_1.getGuid().equals(policyId));*/

        LOG.info(">> checkAnotherCreateTableWithHighPriorityPolicy");
    }

    private static void addAnotherDenyResourcePolicyWithHighPriority() throws Exception {
        LOG.info(">> addAnotherDenyResourcePolicyWithHighPriority");

        setAtlasClient(ARGO);

        String connectionResource = String.format("entity:%s/*", CONNECTION_QN);
        List<String> policyResources = new ArrayList<>();
        policyResources.add(connectionResource);
        policyResources.add("entity-type:*");
        policyResources.add("entity-classification:*");

        SAMPLE_POLICY_2 = new AtlasEntity(TYPE_POLICY);
        SAMPLE_POLICY_2.setAttribute(QUALIFIED_NAME, "policy_2");
        SAMPLE_POLICY_2.setAttribute(NAME, "policy_2");
        //SAMPLE_POLICY_2.setAttribute("policyUsers", Collections.singletonList(ADMIN.getBasicAuthUser()));
        SAMPLE_POLICY_2.setAttribute("policyResources", policyResources);
        SAMPLE_POLICY_2.setAttribute("policyActions", Arrays.asList("entity-create"));
        SAMPLE_POLICY_2.setAttribute("policyCategory", "bootstrap");
        SAMPLE_POLICY_2.setAttribute("policySubCategory", "default");
        SAMPLE_POLICY_2.setAttribute("policyServiceName", "atlas");
        SAMPLE_POLICY_2.setAttribute("policyType", "deny");
        SAMPLE_POLICY_2.setAttribute("policyResourceCategory", "ENTITY");
        SAMPLE_POLICY_2.setAttribute("policyPriority", 1);

        String policy_guid = createEntity(SAMPLE_POLICY_2).getCreatedEntities().get(0).getGuid();

        sleep();
        SAMPLE_POLICY_2 = getEntity(policy_guid);

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> addAnotherDenyResourcePolicyWithHighPriority");
    }

    // 1 normal allow + 1 normal deny + 1 override allow + 1 override deny
    private static void checkAnotherCreateTableDenyWithHighPriorityPolicy() throws Exception {
        LOG.info(">> checkAnotherCreateTableDenyWithHighPriorityPolicy");

        setAtlasClient(ADMIN);

        String tableName = "table_4";

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, tableName);
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

}