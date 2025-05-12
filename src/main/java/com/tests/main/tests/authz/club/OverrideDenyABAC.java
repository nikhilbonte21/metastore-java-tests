package com.tests.main.tests.authz.club;


import com.tests.main.tests.authz.Authz;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.TestUtil;
import okhttp3.OkHttpClient;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.ranger.plugin.model.RangerPolicy;
import org.elasticsearch.index.query.QueryBuilder;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class OverrideDenyABAC implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(OverrideDenyABAC.class);

    private static OkHttpClient ARGO;
    private static OkHttpClient ADMIN;

    private static String CONNECTION_QN;
    private static AtlasEntity DENY_POLICY;

    public static void main(String[] args) throws Exception {
        try {
            new OverrideDenyABAC().run();
        } finally {
            setAtlasClient(ARGO);
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running OverrideDenyABAC tests");

        ARGO = TestUtil.getClientLocal("service-account-atlan-argo");
        ADMIN = TestUtil.getClientLocal("nikhil.bonte");

        setAtlasClient(ARGO);

        CONNECTION_QN = "default/snowflake/dummy_abac_conn_" + getRandomName();
        CONNECTION_PREFIX = CONNECTION_QN + "/";

        long start = System.currentTimeMillis();
        try {
            createDenyABACPolicy();

            // override deny abac
            checkCreateTable();

            createResourceAllowHighPriorityPolicy();

            // override deny abac + override Allow (resource)
            checkCreateTable();

            createResourceAllowLowPriorityPolicy();

            // override deny abac + override Allow +  normal allow (resource)
            checkCreateTable();

            createABACAllowHighPriorityPolicy();

            // override deny abac + override Allow +  normal allow + abac override allow
            checkCreateTable();

            createABACAllowLowPriorityPolicy();

            // override deny abac + override Allow +  normal allow + abac override allow + abac normal allow
            checkCreateTable();


            createABACDenyLowPriorityPolicy();

            // override deny abac + override Allow +  normal allow + abac override allow + abac normal allow + abac normal deny
            checkCreateTable();


            createResourceDenyLowPriorityPolicy();

            // override deny abac + override Allow +  normal allow + abac override allow + abac normal allow + abac normal deny + deny normal resource
            checkCreateTable();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running OverrideDenyABAC tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void createDenyABACPolicy() throws Exception {
        LOG.info(">> createDenyABACPolicy");

        setAtlasClient(ARGO);

        DENY_POLICY = new Authz.PolicyBuilder("deny_override_abac_policy_0")
                .filterCriteria(CONNECTION_QN)
                .type("deny")
                //.user(ADMIN.getBasicAuthUser())
                .priority(RangerPolicy.POLICY_PRIORITY_OVERRIDE)
                .build();

        String policy_guid = createEntity(DENY_POLICY).getCreatedEntities().get(0).getGuid();

        sleep();
        DENY_POLICY = getEntity(policy_guid);

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> createDenyABACPolicy");
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

    private static void createABACAllowLowPriorityPolicy() throws Exception {
        LOG.info(">> createABACAllowLowPriorityPolicy");

        setAtlasClient(ARGO);

        AtlasEntity policy = new Authz.PolicyBuilder("allow_normal_abac_policy" + getRandomName())
                .filterCriteria(CONNECTION_QN)
                .type("allow")
                //.user(ADMIN.getBasicAuthUser())
                .priority(RangerPolicy.POLICY_PRIORITY_NORMAL)
                .build();

        createEntity(policy).getCreatedEntities().get(0).getGuid();

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> createABACAllowLowPriorityPolicy");
    }

    private static void createABACDenyLowPriorityPolicy() throws Exception {
        LOG.info(">> createABACDenyLowPriorityPolicy");

        setAtlasClient(ARGO);

        AtlasEntity policy = new Authz.PolicyBuilder("deny_normal_abac_policy" + getRandomName())
                .filterCriteria(CONNECTION_QN)
                .type("deny")
                //.user(ADMIN.getBasicAuthUser())
                .priority(RangerPolicy.POLICY_PRIORITY_NORMAL)
                .build();

        createEntity(policy).getCreatedEntities().get(0).getGuid();

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> createABACDenyLowPriorityPolicy");
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

    private static void createResourceDenyLowPriorityPolicy() throws Exception {
        LOG.info(">> createResourceDenyLowPriorityPolicy");

        setAtlasClient(ARGO);

        AtlasEntity policy = new Authz.PolicyBuilder("deny_normal_resource_policy" + getRandomName())
                .resources(CONNECTION_QN)
                .type("deny")
                //.user(ADMIN.getBasicAuthUser())
                .priority(RangerPolicy.POLICY_PRIORITY_NORMAL)
                .build();

        String policy_guid = createEntity(policy).getCreatedEntities().get(0).getGuid();

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> createResourceDenyLowPriorityPolicy");
    }

    private static void checkCreateTable() throws Exception {
        LOG.info(">> checkCreateTable");

        setAtlasClient(ADMIN);

        long since = System.currentTimeMillis();

        String tableName = "table_1";

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

        QueryBuilder queryBuilder = new Authz.AccessLogsQueryBuilder()
                .since(since)
                //.user(ADMIN.getBasicAuthUser())
                .result(0)
                .logType(Authz.AccessLogsQueryBuilder.LogType.NewAuthZAudit)
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