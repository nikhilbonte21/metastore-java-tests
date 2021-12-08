package com.tests.main.glossary.utils;

import com.tests.main.glossary.client.ClientBuilder;
import org.apache.atlas.AtlasException;
import org.apache.http.HttpHost;
import org.apache.log4j.Category;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static com.tests.main.glossary.utils.AtlasElasticsearchDatabase.getHttpHosts;

public class ESUtils {

    private static final Logger       LOG = LoggerFactory.getLogger(ESUtils.class);
    public static RestClient          lowLevelClient;
    public static RestHighLevelClient highLevelClient;
    public static String              index = "janusgraph_vertex_index";

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

        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(sourceBuilder);

        return runQuery(searchRequest);
    }

    public static SearchResponse searchWithGuid(String guid) {
        LOG.info("searchWithGuid: {}", guid);
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("__guid", guid));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(queryBuilder);

        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.source(sourceBuilder);

        return runQuery(searchRequest);
    }

    public static SearchResponse runQuery(SearchRequest searchRequest) {

        try {
            SearchResponse response = highLevelClient.search(searchRequest, requestOptions);
            return response;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static RestHighLevelClient getClient() {
        if (highLevelClient == null) {
            synchronized (AtlasElasticsearchDatabase.class) {
                if (highLevelClient == null) {
                    try {
                        List<HttpHost> httpHosts = getHttpHosts();

                        RestClientBuilder restClientBuilder = RestClient.builder(httpHosts.toArray(new HttpHost[0]))
                                .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(900000)
                                        .setSocketTimeout(900000));
                        highLevelClient =
                                new RestHighLevelClient(restClientBuilder);
                    } catch (AtlasException e) {
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
                                .setConnectTimeout(900000)
                                .setSocketTimeout(900000));

                        lowLevelClient = builder.build();
                    } catch (AtlasException e) {
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
}
