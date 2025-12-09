package com.tests.main.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tests.main.client.okhttp3.ConfigReader;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tests.main.utils.AtlasElasticsearchDatabase.getHttpHosts;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.verifyESAttributes;
import static com.tests.main.utils.TestUtil.verifyESDocumentNotPresent;
import static com.tests.main.utils.TestUtil.verifyESHasNot;
import static org.junit.Assert.assertTrue;

public class ESUtil {

    private static final Logger       LOG = LoggerFactory.getLogger(ESUtil.class);
    public static RestClient          lowLevelClient;
    public static RestHighLevelClient highLevelClient;
    public static String              JG_VERTEX_INDEX = ConfigReader.getString("search.vertex.index.name");
    public static String              index_access_logs = "ranger-audit";

    private static RequestOptions requestOptions = RequestOptions.DEFAULT;
    private static int            bufferLimit    = 2000 * 1024 * 1024;

    static {
        setupClients();

        RequestOptions.Builder builder = requestOptions.toBuilder();
        builder.setHttpAsyncResponseConsumerFactory(new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(bufferLimit));
        requestOptions = builder.build();
    }

    private static void setupClients(){
        lowLevelClient = getLowLevelClient();
        highLevelClient = getClient();
        LOG.info("Client setup is successful!");
    }

    public static SearchResponse searchWithName(String entityName) {
        LOG.info("searchWithName: {}", entityName);
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("name", entityName));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);

        SearchRequest searchRequest = new SearchRequest("ranger-audit");
        searchRequest.source(sourceBuilder);

        return runQuery(searchRequest);
    }

    public static SearchResponse searchWithTypeName(String typeName, int from, int size) {
        LOG.info("searchWithTypeName: {}", typeName);
        QueryBuilder queryBuilder = QueryBuilders.boolQuery()

                .must(QueryBuilders.matchQuery("__typeName", typeName))
                .must(QueryBuilders.matchQuery("__state", "ACTIVE"));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        sourceBuilder.size(size);
        sourceBuilder.from(from);
        sourceBuilder.sort("__guid", SortOrder.ASC);

        SearchRequest searchRequest = new SearchRequest(JG_VERTEX_INDEX);
        searchRequest.source(sourceBuilder);
        LOG.info(" sourceBuilder {}", sourceBuilder);
        return runQuery(searchRequest);
    }

    public static SearchResponse searchWithGuid(String guid) {
        LOG.info("searchWithGuid: {}", guid);
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("__guid", guid));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);

        SearchRequest searchRequest = new SearchRequest(JG_VERTEX_INDEX);
        searchRequest.source(sourceBuilder);

        return runQuery(searchRequest);
    }

    public static SearchResponse searchWithPrefixQN(String guid) {
        LOG.info("searchWithPrefixQN: {}", guid);
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.prefixQuery("qualifiedName", guid));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);

        SearchRequest searchRequest = new SearchRequest(JG_VERTEX_INDEX);
        searchRequest.source(sourceBuilder);

        return runQuery(searchRequest);
    }

    public static SearchHit[] searchWithQueryBuilderAccess(QueryBuilder queryBuilder) {
        LOG.info("searchWithQueryBuilder");

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);
        sourceBuilder.sort("evtTime", SortOrder.DESC);

        SearchRequest searchRequest = new SearchRequest(index_access_logs);
        searchRequest.source(sourceBuilder);

        return runQuery(searchRequest).getHits().getHits();
    }

    public static SearchResponse runQuery(SearchRequest searchRequest) {

        try {
            SearchResponse response = highLevelClient.search(searchRequest, requestOptions);
            return response;
        } catch (Exception e) {
            LOG.info("Re-creating highLevelClient");
            try {
                highLevelClient = getClient(true);
                return highLevelClient.search(searchRequest, requestOptions);
            } catch (Exception ec) {
                ec.printStackTrace();
            }
        }
        return null;
    }

    public static Map<String, Object> runAliasGetQuery(String aliasName) {

        try {
            GetAliasesRequest requestWithAlias = new GetAliasesRequest(aliasName);
            GetAliasesResponse response = highLevelClient.indices().getAlias(requestWithAlias, requestOptions);

            ObjectMapper mapper = new ObjectMapper();
            TypeReference<HashMap<String,Object>> typeRef = new TypeReference<HashMap<String,Object>>() {};
            return mapper.readValue(response.getAliases().get("janusgraph_vertex_index").iterator().next().filter().toString(), typeRef);

        } catch (Exception e) {
            LOG.info("Re-creating highLevelClient");
            try {
                highLevelClient = getClient(true);
                //return highLevelClient.search(searchRequest, requestOptions);
            } catch (Exception ec) {
                ec.printStackTrace();
            }
        }

        return null;
    }

    private static RestHighLevelClient getClient(boolean force) {
        try {
            List<HttpHost> httpHosts = getHttpHosts();

            RestClientBuilder restClientBuilder = RestClient.builder(httpHosts.toArray(new HttpHost[0]))
                    .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(900000)
                            .setSocketTimeout(900000));
            highLevelClient =
                    new RestHighLevelClient(restClientBuilder);
        } catch (Exception e) {
            LOG.error("Failed to initialize high level client for ES");
        }
        return highLevelClient;
    }

    private static RestHighLevelClient getClient() {
        if (highLevelClient == null) {
            synchronized (AtlasElasticsearchDatabase.class) {
                if (highLevelClient == null) {
                    try {
                        List<HttpHost> httpHosts = getHttpHosts();

                        RestClientBuilder restClientBuilder = RestClient.builder(httpHosts.toArray(new HttpHost[0]))
                                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                                        .setConnectTimeout(90)
                                        .setSocketTimeout(90)
                                );
                        highLevelClient =
                                new RestHighLevelClient(restClientBuilder);
                    } catch (Exception e) {
                        LOG.error("Failed to initialize high level client for ES");
                    }
                }
            }
        }
        return highLevelClient;
    }

    private static RestClient getLowLevelClient() {
        if (lowLevelClient == null) {
            synchronized (AtlasElasticsearchDatabase.class) {
                if (lowLevelClient == null) {
                    try {
                        List<HttpHost> httpHosts = getHttpHosts();

                        RestClientBuilder builder = RestClient.builder(httpHosts.get(0));
                        builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                                .setConnectTimeout(90)
                                .setSocketTimeout(90));

                        lowLevelClient = builder.build();
                    } catch (Exception e) {
                        LOG.error("Failed to initialize low level rest client for ES");
                    }
                }
            }
        }
        return lowLevelClient;
    }

    public static void close() {
        try {
            highLevelClient.close();
            LOG.info("Closed highLevelClient");
            lowLevelClient.close();
            LOG.info("Closed lowLevelClient");
        } catch (IOException io) {
            LOG.info("Failed to close ES client/s");
        }
    }

    public static void deleteESDocByGuid(String... guids) throws Exception {
        DeleteByQueryRequest deleteRequest = new DeleteByQueryRequest(JG_VERTEX_INDEX);
        deleteRequest.setQuery(QueryBuilders.termsQuery("__guid", guids));
        deleteRequest.setConflicts("proceed");
        deleteRequest.setRefresh(true);

        highLevelClient.deleteByQuery(deleteRequest, RequestOptions.DEFAULT);

        sleep(1000);

        verifyESDocumentNotPresent(guids);
    }

    public static void updateESDocByGuid(String guid, Map<String, Object> attrs) throws Exception {
        UpdateByQueryRequest request = new UpdateByQueryRequest(JG_VERTEX_INDEX);

        request.setConflicts("proceed");
        request.setQuery(QueryBuilders.termQuery("__guid", guid));

        Map<String, Object> params = new HashMap<>();
        params.put("new_status", "processed");
        params.put("current_timestamp", System.currentTimeMillis());

        String scriptCode = "for (entry in params.newProperties.entrySet()) { " +
                "  ctx._source[entry.getKey()] = entry.getValue(); " +
                "}";

        // Pass your 'newProperties' map as a parameter to the script
        Map<String, Object> scriptParams = Collections.singletonMap("newProperties", attrs);

        Script script = new Script(
                ScriptType.INLINE,
                "painless",
                scriptCode,
                scriptParams
        );
        request.setScript(script);

        highLevelClient.updateByQuery(request, RequestOptions.DEFAULT);

        //sleep(1000);

        //verifyESAttributes(guid, attrs);
    }

    public static void overrideESDocByGuid(String guid, Map<String, Object> newDocContent) throws Exception {
        UpdateByQueryRequest request = new UpdateByQueryRequest(JG_VERTEX_INDEX);

        request.setConflicts("proceed");
        request.setQuery(QueryBuilders.termQuery("__guid", guid));

        String scriptCode = "ctx._source = params.newDocument;";

        // Pass the new document content as a parameter to the script
        Map<String, Object> scriptParams = Collections.singletonMap("newDocument", newDocContent);

        Script script = new Script(
                ScriptType.INLINE,
                "painless",
                scriptCode,
                scriptParams
        );
        request.setScript(script);

        highLevelClient.updateByQuery(request, RequestOptions.DEFAULT);

        //sleep(1000);

        //verifyESAttributes(guid, newDocContent);
    }
}
