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


public class OverrideAllowUsual implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(OverrideAllowUsual.class);

    private static OkHttpClient ARGO;
    private static OkHttpClient ADMIN;

    private static String CONNECTION_QN;
    private static AtlasEntity ALLOW_RESOURCE_POLICY;

    public static void main(String[] args) throws Exception {
        try {
            new OverrideAllowUsual().run();
        } finally {
            setAtlasClient(ARGO);
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running OverrideAllowUsual tests");

        ARGO = TestUtil.getClientLocal("service-account-atlan-argo");
        ADMIN = TestUtil.getClientLocal("nikhil.bonte");

        setAtlasClient(ARGO);

        CONNECTION_QN = "default/snowflake/dummy_abac_conn_" + getRandomName();
        CONNECTION_PREFIX = CONNECTION_QN + "/";

        long start = System.currentTimeMillis();
        try {
            createAllowResourceOverridePolicy();

            // override allow (resource)
            checkCreateTable("table_1");


            createABACDenyNormalPriorityPolicy();

            // override allow +  normal deny ABAC
            checkCreateTable("table_2");


            createABACAllowNormalPriorityPolicy();

            // override allow +  normal deny ABAC +  normal allow ABAC
            checkCreateTable("table_3");


            createResourceAllowLowPriorityPolicy();

            // override allow +  normal deny ABAC +  normal allow ABAC + normal allow resource
            checkCreateTable("table_4");


        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running OverrideAllowUsual tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void createAllowResourceOverridePolicy() throws Exception {
        LOG.info(">> createAllowResourceOverridePolicy");

        setAtlasClient(ARGO);

        ALLOW_RESOURCE_POLICY = new Authz.PolicyBuilder("allow_override_resource_policy")
                .resources(CONNECTION_QN)
                .type("allow")
                //.user(ADMIN.getBasicAuthUser())
                .priority(RangerPolicy.POLICY_PRIORITY_OVERRIDE)
                .build();

        String policy_guid = createEntity(ALLOW_RESOURCE_POLICY).getCreatedEntities().get(0).getGuid();

        sleep();
        ALLOW_RESOURCE_POLICY = getEntity(policy_guid);

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> createAllowResourceOverridePolicy");
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

    private static void createABACAllowNormalPriorityPolicy() throws Exception {
        LOG.info(">> createABACAllowNormalPriorityPolicy");

        setAtlasClient(ARGO);

        AtlasEntity policy = new Authz.PolicyBuilder("allow_normal_abac_policy" + getRandomName())
                .filterCriteria(CONNECTION_QN)
                .type("allow")
                //.user(ADMIN.getBasicAuthUser())
                .priority(RangerPolicy.POLICY_PRIORITY_NORMAL)
                .build();

        String policy_guid = createEntity(policy).getCreatedEntities().get(0).getGuid();

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> createABACAllowNormalPriorityPolicy");
    }

    private static void createABACDenyNormalPriorityPolicy() throws Exception {
        LOG.info(">> createABACDenyNormalPriorityPolicy");

        setAtlasClient(ARGO);

        AtlasEntity policy = new Authz.PolicyBuilder("deny_normal_abac_policy" + getRandomName())
                .filterCriteria(CONNECTION_QN)
                .type("deny")
                //.user(ADMIN.getBasicAuthUser())
                .priority(RangerPolicy.POLICY_PRIORITY_NORMAL)
                .build();

        String policy_guid = createEntity(policy).getCreatedEntities().get(0).getGuid();

        sleep(15);
        setAtlasClient(ADMIN);

        LOG.info(">> createABACDenyNormalPriorityPolicy");
    }

    private static void checkCreateTable(String tableName) throws Exception {
        LOG.info(">> checkCreateTable");

        setAtlasClient(ADMIN);

        long since = System.currentTimeMillis();

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, tableName);

        String table_guid = createEntity(table).getCreatedEntities().get(0).getGuid();

        sleep(3);

        table = getEntity(table_guid);

        QueryBuilder queryBuilder = new Authz.AccessLogsQueryBuilder()
                .since(since)
                //.user(ADMIN.getBasicAuthUser())
                .result(1)
                .logType(Authz.AccessLogsQueryBuilder.LogType.AtlasAuthZAudit)
                .resource(TYPE_TABLE, (String) table.getAttribute(QUALIFIED_NAME))
                .build();

        SearchHit[] hits = searchWithQueryBuilderAccess(queryBuilder);

        assertEquals(1, hits.length);
        Map<String, Object> accessLog = hits[0].getSourceAsMap();

        assertEquals(ALLOW_RESOURCE_POLICY.getGuid(), (String) accessLog.get("policyId"));

        LOG.info(">> checkCreateTable");
    }
}