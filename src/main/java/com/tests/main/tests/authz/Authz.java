package com.tests.main.tests.authz;

import com.tests.main.utils.ESUtil;
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

import static com.tests.main.utils.ESUtil.searchWithQueryBuilderAccess;
import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.getMap;
import static com.tests.main.utils.TestUtil.toJson;

public class Authz {
    private static final Logger LOG = LoggerFactory.getLogger(Authz.class);

    public static Map<String, String> populateConnectionPolicies(String connectionGuid) {

        Map<String, String> ret = new HashMap<>();
        SearchHit[] searchHit = ESUtil.searchWithPrefixQN(connectionGuid).getHits().getHits();

        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            ret.put( (String) sourceAsMap.get("__guid"), (String) sourceAsMap.get("qualifiedName"));
        }
        return ret;
    }

    public static Map<String, Object> getMatchedPolicyIdFromAccessLogs(String entityType, String entityQualifiedName, String userName) {
        return getMatchedPolicyIdFromAccessLogs(entityType,entityQualifiedName, userName, true, null);
    }

    public static Map<String, Object> getMatchedPolicyIdFromAccessLogs(String entityType, String entityQualifiedName,
                                                                String userName,
                                                                boolean checkAllow,
                                                                Boolean explictDeny) {
        String resourceValue =  entityType + "/[]/" + entityQualifiedName;

        BoolQueryBuilder boolQueryBuilder = QueryBuilders
                .boolQuery()
                .must(QueryBuilders.termQuery("action", "entity-create"))
                .must(QueryBuilders.termQuery("reqUser", userName))
                .must(QueryBuilders.termQuery("resource", resourceValue));

        if (checkAllow) {
            boolQueryBuilder.must(QueryBuilders.termQuery("result", 1));
        } else {
            boolQueryBuilder.must(QueryBuilders.termQuery("result", 0));
            if (Boolean.TRUE.equals(explictDeny)) {
                boolQueryBuilder.mustNot(QueryBuilders.termQuery("policyId.keyword", "-1"));
            }
        }

        SearchHit[] searchHit = searchWithQueryBuilderAccess(boolQueryBuilder);

        return searchHit[0].getSourceAsMap();
    }

    public static class AccessLogsQueryBuilder {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        public enum LogType {
            AtlasAuthZAudit,
            NewAuthZAudit;
        }

        public AccessLogsQueryBuilder() {
            this.boolQueryBuilder.must(QueryBuilders.termQuery("action", "entity-create"));
        }

        public AccessLogsQueryBuilder action(String action) {
            this.boolQueryBuilder.must(QueryBuilders.termQuery("action", action));
            return this;
        }

        public AccessLogsQueryBuilder user(String userName) {
            this.boolQueryBuilder.must(QueryBuilders.termQuery("reqUser", userName));
            return this;
        }

        public AccessLogsQueryBuilder resource(String entityType, String entityQualifiedName) {
            String resourceValue =  entityType + "/[]/" + entityQualifiedName;
            this.boolQueryBuilder.must(QueryBuilders.termQuery("resource", resourceValue));
            return this;
        }

        public AccessLogsQueryBuilder result(int result) {
            this.boolQueryBuilder.must(QueryBuilders.termQuery("result", result));
            return this;
        }

        public AccessLogsQueryBuilder since(long since) {
            this.boolQueryBuilder.must(QueryBuilders.rangeQuery("evtTime").gte(since));
            return this;
        }

        public AccessLogsQueryBuilder logType(LogType logType) {
            this.boolQueryBuilder.must(QueryBuilders.termQuery("logType", logType.name()));
            return this;
        }

        public AccessLogsQueryBuilder includeOnlyExplicitDeny() {
            boolQueryBuilder.mustNot(QueryBuilders.termQuery("policyId.keyword", "-1"));
            return this;
        }

        public BoolQueryBuilder build() {
            return boolQueryBuilder;
        }
    }

    public static class PolicyBuilder {
        AtlasEntity policy = new AtlasEntity("AuthPolicy");

        public PolicyBuilder(String name) {
            this.policy.setAttribute(NAME, name);
            this.policy.setAttribute(QUALIFIED_NAME, "resource_" + name);

            policy.setAttribute("policyCategory", "bootstrap");
            policy.setAttribute("policySubCategory", "default");
            policy.setAttribute("policyResourceCategory", "ENTITY");

            policy.setAttribute("policyType", "allow");
            policy.setAttribute("policyPriority", 0);

            policy.setAttribute("policyActions", Arrays.asList("entity-create", "entity-update", "entity-read", "entity-delete"));
        }

        public PolicyBuilder name(String name) {
            this.policy.setAttribute(NAME, name);
            return this;
        }

        public PolicyBuilder qualifiedName(String qualifiedName) {
            this.policy.setAttribute(QUALIFIED_NAME, qualifiedName);
            return this;
        }

        public PolicyBuilder user(String user) {
            this.policy.setAttribute("policyUsers", Collections.singletonList(user));
            return this;
        }

        public PolicyBuilder users(List<String> users) {
            this.policy.setAttribute("policyUsers", users);
            return this;
        }

        public PolicyBuilder priority(int policyPriority) {
            this.policy.setAttribute("policyPriority", policyPriority);
            return this;
        }

        public PolicyBuilder type(String policyType) {
            this.policy.setAttribute("policyType", policyType);
            return this;
        }

        public PolicyBuilder resources(String qualifiedName) {
            String resource = String.format("entity:%s/*", qualifiedName);
            List<String> policyResources = new ArrayList<>();
            policyResources.add(resource);
            policyResources.add("entity-type:*");
            policyResources.add("entity-classification:*");

            policy.setAttribute("policyServiceName", "atlas");
            policy.setAttribute("policyResources", policyResources);
            return this;
        }

        public PolicyBuilder filterCriteria(String qualifiedName) {
            List<Map<String, Object>> orClauses = new ArrayList<>();
            orClauses.add(getMap("attributeName", "qualifiedName",
                    "attributeValue", qualifiedName,
                    "operator", "EQUALS"));
            orClauses.add(getMap("attributeName", "connectionQualifiedName",
                    "attributeValue", qualifiedName,
                    "operator", "EQUALS"));

            Map<String, Object> filterCriteriaAsMap = getMap("entity", getMap("condition", "OR", "criterion", orClauses));

            policy.setAttribute("policyServiceName", "atlas_abac");
            policy.setAttribute("policyFilterCriteria", toJson(filterCriteriaAsMap));
            return this;
        }

        public AtlasEntity build() {
            return policy;
        }
    }
}
