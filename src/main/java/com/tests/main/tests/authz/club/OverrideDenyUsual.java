package com.tests.main.tests.authz.club;


import com.tests.main.tests.authz.Authz;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import okhttp3.OkHttpClient;
import org.elasticsearch.index.query.QueryBuilder;
import com.tests.main.utils.TestUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static com.tests.main.utils.ESUtil.searchWithQueryBuilderAccess;
import static com.tests.main.utils.TestUtil.CONNECTION_PREFIX;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.setAtlasClient;
import static com.tests.main.utils.TestUtil.sleep;
import static org.junit.Assert.*;


public class OverrideDenyUsual implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(OverrideDenyUsual.class);

    private static OkHttpClient ARGO;
    private static OkHttpClient ADMIN;

    private static String CONNECTION_QN;
    private static AtlasEntity DENY_POLICY;

    public static void main(String[] args) throws Exception {
        try {
            new OverrideDenyUsual().run();
        } finally {
            setAtlasClient(ARGO);
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running OverrideDenyUsual tests");

        ARGO = TestUtil.getClientLocal("service-account-atlan-argo");
        ADMIN = TestUtil.getClientLocal("nikhil.bonte");

        setAtlasClient(ARGO);

        CONNECTION_QN = "default/snowflake/dummy_abac_conn_" + getRandomName();
        CONNECTION_PREFIX = CONNECTION_QN + "/";

        long start = System.currentTimeMillis();
        try {
            createDenyResourcePolicy();

            // override deny (resource)
            checkCreateTable();

            createResourceAllowHighPriorityPolicy();

            // override deny + override Allow (resource)
            checkCreateTable();

            createResourceAllowLowPriorityPolicy();

            // override deny + override Allow +  normal allow (resource)
            checkCreateTable();

            createABACAllowHighPriorityPolicy();

            // override deny + override Allow +  normal allow + abac override allow
            checkCreateTable();


            createABACDenyHighPriorityPolicy();

            // override deny + override Allow +  normal allow + abac override allow + abac override deny
            checkCreateTable();



        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running OverrideDenyUsual tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void createDenyResourcePolicy() throws Exception {
        LOG.info(">> createDenyResourcePolicy");

        setAtlasClient(ARGO);

        DENY_POLICY = new Authz.PolicyBuilder("deny_override_resource_policy")
                .resources(CONNECTION_QN)
                .type("deny")
                //.user(ADMIN.getBasicAuthUser())
                .priority(RangerPolicy.POLICY_PRIORITY_OVERRIDE)
                .build();

        String policy_guid = createEntity(DENY_POLICY).getCreatedEntities().get(0).getGuid();

        sleep();
        DENY_POLICY = getEntity(policy_guid);

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> createDenyResourcePolicy");
    }

    private static void createResourceAllowHighPriorityPolicy() throws Exception {
        LOG.info(">> createResourceAllowHighPriorityPolicy");

        setAtlasClient(ARGO);

        AtlasEntity policy = new Authz.PolicyBuilder("allow_override_resource_policy" + getRandomName())
                .resources(CONNECTION_QN)
                .type("allow")
                //.user(ADMIN.getBasicAuthUser())
                .priority(RangerPolicy.POLICY_PRIORITY_OVERRIDE)
                .build();

        String policy_guid = createEntity(policy).getCreatedEntities().get(0).getGuid();

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> createResourceAllowHighPriorityPolicy");
    }

    private static void createResourceAllowLowPriorityPolicy() throws Exception {
        LOG.info(">> createResourceAllowHighPriorityPolicy");

        setAtlasClient(ARGO);

        AtlasEntity policy = new Authz.PolicyBuilder("allow_normal_resource_policy" + getRandomName())
                .resources(CONNECTION_QN)
                .type("allow")
                //.user(ADMIN.getBasicAuthUser())
                .priority(RangerPolicy.POLICY_PRIORITY_NORMAL)
                .build();

        String policy_guid = createEntity(policy).getCreatedEntities().get(0).getGuid();

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> createResourceAllowHighPriorityPolicy");
    }

    private static void createABACAllowHighPriorityPolicy() throws Exception {
        LOG.info(">> createABACAllowHighPriorityPolicy");

        setAtlasClient(ARGO);

        AtlasEntity policy = new Authz.PolicyBuilder("allow_override_abac_policy" + getRandomName())
                .filterCriteria(CONNECTION_QN)
                .type("allow")
                //.user(ADMIN.getBasicAuthUser())
                .priority(RangerPolicy.POLICY_PRIORITY_OVERRIDE)
                .build();

        String policy_guid = createEntity(policy).getCreatedEntities().get(0).getGuid();

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> createABACAllowHighPriorityPolicy");
    }

    private static void createABACDenyHighPriorityPolicy() throws Exception {
        LOG.info(">> createABACDenyHighPriorityPolicy");

        setAtlasClient(ARGO);

        AtlasEntity policy = new Authz.PolicyBuilder("deny_override_abac_policy" + getRandomName())
                .filterCriteria(CONNECTION_QN)
                .type("deny")
                //.user(ADMIN.getBasicAuthUser())
                .priority(RangerPolicy.POLICY_PRIORITY_OVERRIDE)
                .build();

        String policy_guid = createEntity(policy).getCreatedEntities().get(0).getGuid();

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> createABACDenyHighPriorityPolicy");
    }

    private static void checkCreateTable() throws Exception {
        LOG.info(">> checkCreateTable");

        setAtlasClient(ADMIN);

        long since = System.currentTimeMillis();

        String tableName = "table_1";

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

        QueryBuilder queryBuilder = new Authz.AccessLogsQueryBuilder()
                .since(since)
                //.user(ADMIN.getBasicAuthUser())
                .result(0)
                .logType(Authz.AccessLogsQueryBuilder.LogType.AtlasAuthZAudit)
                .includeOnlyExplicitDeny()
                .resource(TYPE_TABLE, (String) table.getAttribute(QUALIFIED_NAME))
                .build();

        SearchHit[] hits = searchWithQueryBuilderAccess(queryBuilder);

        assertEquals(1, hits.length);
        Map<String, Object> accessLog = hits[0].getSourceAsMap();

        assertEquals(DENY_POLICY.getGuid(), (String) accessLog.get("policyId"));

        LOG.info(">> checkCreateTable");
    }
}