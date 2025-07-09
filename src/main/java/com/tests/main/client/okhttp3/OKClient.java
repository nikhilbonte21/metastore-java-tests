package com.tests.main.client.okhttp3;

import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import com.tests.main.IndexSearchParams;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.tests.main.utils.Constants.DELETE_HANDLER_DEFAULT;
import static com.tests.main.utils.TestUtil.fromJson;
import static com.tests.main.utils.TestUtil.isRunAsAdmin;
import static com.tests.main.utils.TestUtil.isRunAsGuest;
import static com.tests.main.utils.TestUtil.isRunAsMember;
import static com.tests.main.utils.TestUtil.toJson;

public class OKClient {
    private static final Logger LOG = LoggerFactory.getLogger(OKClient.class);

    public static boolean isBeta = "beta".equals(ConfigReader.getString("atlas.client.mode", ""));

    private static String BASE_URL = isBeta ?
            ConfigReader.getString("beta.client.host")  + "/api/meta"
            : ConfigReader.getString("local.client.rest.address");

    private static String URL_TYPE_DEF = BASE_URL + "/types/typedefs";
    private static String URL_TYPE_DEF_BY_NAME = BASE_URL + "/types/typedef/name/%s";

    private static String URL_ENTITY = BASE_URL + "/entity";
    private static String URL_ENTITY_BULK = BASE_URL + "/entity/bulk?replaceTags=true";
    private static String URL_GET_ENTITY_GUID = BASE_URL + "/entity/guid/%s";

    private static String URL_UPSERT_BM = BASE_URL + "/entity/guid/%s/businessmetadata/%s";
    private static String URL_UPSERT_BM_BULK = BASE_URL + "/entity/guid/%s/businessmetadata";

    private static String URL_REPAIR_ASSET = BASE_URL + "/entity/guid/%s/repairindex";
    private static String URL_REPAIR_ASSETS = BASE_URL + "/entity/guid/bulk/repairindex";

    private static String URL_INDEX_SEARCH = BASE_URL + "/search/indexsearch";

    private static OkHttpClient finalClient = null;

    public static String GOD, ADMIN, MEMBER, GUEST;

    public static OkHttpClient getClient() {
        if (finalClient == null) {
            synchronized (OKClient.class) {
                if (finalClient == null) {
                    finalClient = new OkHttpClient.Builder()
                            .connectTimeout(60, TimeUnit.SECONDS)
                            .readTimeout(60, TimeUnit.SECONDS)
                            .writeTimeout(60, TimeUnit.SECONDS)
                            .callTimeout(60, TimeUnit.SECONDS)
                            .build();
                }
            }
        }

        return finalClient;
    }

    public static String getToken() throws Exception {
        if (GOD == null) {
            loadTokens();
        }
        String token = GOD;

        if (isRunAsAdmin()) {
            token = ADMIN;
        } else if (isRunAsMember()) {
            token = MEMBER;
        } else if (isRunAsGuest()) {
            token = GUEST;
        }

        return token;
    }

    private static void loadTokens() throws Exception {
        if (isBeta) {
            GOD = "Bearer " + getOrCreateToken("beta.client.auth.token", null);
            ADMIN = "Bearer " + getOrCreateToken("beta.client.admin.auth.token", "nikhil.bonte");

            MEMBER = "Bearer " + getOrCreateToken("beta.client.member.auth.token", "nikhil.member");
            GUEST = "Bearer " + getOrCreateToken("beta.client.guest.auth.token", "nikhil.guest");
        } else {
            GOD = "Basic " + Base64.getEncoder().encodeToString("service-account-atlan-argo:admin".getBytes(StandardCharsets.UTF_8));
            ADMIN = "Basic " + Base64.getEncoder().encodeToString("nikhil.bonte:admin".getBytes(StandardCharsets.UTF_8));

            MEMBER = "Basic " + Base64.getEncoder().encodeToString("nikhil.member:admin".getBytes(StandardCharsets.UTF_8));
            GUEST = "Basic " + Base64.getEncoder().encodeToString("nikhil.guest:admin".getBytes(StandardCharsets.UTF_8));
        }
    }

    private static String getOrCreateToken(String conf, String userName) throws Exception {
        String token = ConfigReader.getString(conf);
        if (StringUtils.isEmpty(token)) {
            token = getKeycloakToken(userName);
        }

        return token;
    }

    private static Request getRequest(String url) throws Exception {
        return getRequest(url, null);
    }

    private static Request getRequestDELETE(String url) throws Exception {
        Request.Builder builder = new Request.Builder()
                .url(url)
                .delete()
                .header("Authorization", getToken())
                .header("Content-Type", "application/json")
                .header("x-atlan-request-id", "tests-2.0-client");

        return builder.build();
    }

    private static Request getRequest(String url, Object payload) throws Exception {

        Request.Builder builder = new Request.Builder()
                .url(url)
                .header("Authorization", getToken())
                .header("Content-Type", "application/json")
                .header("x-atlan-request-id", "tests-2.0-client");

        if (payload != null) {
            RequestBody body = RequestBody.create(toJson(payload), MediaType.get("application/json"));
            builder.post(body);
        }

        return builder.build();
    }

    private static Request getRequestPOST(String url, Object payload) throws Exception {

        Request.Builder builder = new Request.Builder()
                .url(url)
                .header("Authorization", getToken())
                .header("Content-Type", "application/json")
                .header("x-atlan-request-id", "tests-2.0-client");

        RequestBody body = okhttp3.RequestBody.create(new byte[0]);

        if (payload != null) {
            body = RequestBody.create(toJson(payload), MediaType.get("application/json"));
        }
        builder.post(body);

        return builder.build();
    }

    public AtlasSearchResult indexSearch(IndexSearchParams indexSearchParams) throws Exception {
        Request request = getRequest(URL_INDEX_SEARCH, indexSearchParams);

        return executeRequest(request, AtlasSearchResult.class);
    }

    public AtlasEntity.AtlasEntityWithExtInfo getEntityByGuid(String guid) throws Exception {
        Request request = getRequest(String.format(URL_GET_ENTITY_GUID, guid));

        return executeRequest(request, AtlasEntity.AtlasEntityWithExtInfo.class);
    }

    public EntityMutationResponse createEntity(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws Exception {
        Request request = getRequest(URL_ENTITY, entityWithExtInfo);

        return executeRequest(request, EntityMutationResponse.class);
    }

    public AtlasTypesDef createTypeDef(AtlasTypesDef typesDef) throws Exception {
        Request request = getRequest(URL_TYPE_DEF, typesDef);

        return executeRequest(request, AtlasTypesDef.class);
    }

    public AtlasTypesDef getBMDefs() throws Exception {
        Request request = getRequest(URL_TYPE_DEF + "?type=BUSINESS_METADATA");

        return executeRequest(request, AtlasTypesDef.class);
    }
    public AtlasTypesDef getTagDefs() throws Exception {
        Request request = getRequest(URL_TYPE_DEF + "?type=CLASSIFICATION");

        return executeRequest(request, AtlasTypesDef.class);
    }

    public void deleteTypeDefByName(String name) throws Exception {
        Request request = getRequestDELETE(String.format(URL_TYPE_DEF_BY_NAME, name));

        executeRequest(request, null);
    }

    public EntityMutationResponse createEntities(AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo) throws Exception {
        Request request = getRequest(URL_ENTITY_BULK, entitiesWithExtInfo);

        return executeRequest(request, EntityMutationResponse.class);
    }

    public EntityMutationResponse deleteEntityByGuid(String guid, String deleteType) throws Exception {
        String url = BASE_URL + "/entity/guid/" + guid;
        if (StringUtils.isNotEmpty(deleteType) && !DELETE_HANDLER_DEFAULT.equals(deleteType)) {
            url = url + "?deleteType=" + deleteType;
        }
        Request request = getRequestDELETE(url);

        return executeRequest(request, EntityMutationResponse.class);
    }

    public EntityMutationResponse deleteEntitiesByGuids(List<String> guids) throws Exception {
        if (CollectionUtils.isEmpty(guids)) {
            return null;
        }

        StringBuilder queryParams = new StringBuilder();
        queryParams.append(BASE_URL + "/entity/bulk?deleteType=PURGE&");
        guids.forEach(guid -> queryParams.append("guid=" + guid + "&"));

        String url = queryParams.toString();
        Request request = getRequestDELETE(url);

        return executeRequest(request, EntityMutationResponse.class);
    }

    public EntityAuditSearchResult getEntityAudit(Object params) throws Exception {
        if (params == null) {
            return null;
        }

        String url = BASE_URL + "/entity/auditSearch";
        Request request = getRequest(url, params);

        return executeRequest(request, EntityAuditSearchResult.class);
    }

    public void addOrUpdateCMAttrBulk(String assetGuid, Map<String, Object> bm) throws Exception {
        Request request = getRequest(String.format(URL_UPSERT_BM_BULK, assetGuid), bm);

        executeRequest(request, null);
    }

    public void repairEntityByGuid(String assetGuid) throws Exception {
        Request request = getRequestPOST(String.format(URL_REPAIR_ASSET, assetGuid), null);

        executeRequest(request, null);
    }

    public void repairEntitiesByGuid(List<String> assetGuids) throws Exception {
        Request request = getRequestPOST(URL_REPAIR_ASSETS, assetGuids);

        executeRequest(request, null);
    }

    public void addOrUpdateCMAttr(String assetGuid, String bmName, Map<String, Object> bm) throws Exception {
        Request request = getRequest(String.format(URL_UPSERT_BM, assetGuid, bmName), bm);

        executeRequest(request, null);
    }

    public Map<String, Object> searchTasks(Object searchRequest) throws Exception {
        String url = BASE_URL + "/task/search";
        Request request = getRequest(url, searchRequest);

        return executeRequest(request, Map.class);
    }

    private <T> T executeRequest(Request request, Class<T> returnType) throws Exception {
        try (Response response = getClient().newCall(request).execute()) {
            if (response.code() == 200 || response.code() == 204) {
                if (returnType != null) {
                    return fromJson(response.body().string(), returnType);
                }
            } else {
                String responseBody = response.body() != null ? response.body().string() : "";
                String errorMessage = String.format("%d | %s | %s", 
                    response.code(), 
                    response.message(), 
                    responseBody);
                LOG.error("Request failed: {}", errorMessage);
                throw new Exception(errorMessage);
            }
        }
        return null;
    }

    private static String getKeycloakToken(String userName) throws Exception {
        LOG.info("fetching token for {}", userName);
        String keycloakUrl = ConfigReader.getString("beta.client.host") + "/auth/realms/default/protocol/openid-connect/token";
        String clientSecret = ConfigReader.getString("beta.client.secret");

        Map<String, String> formData = new HashMap<>();
        formData.put("client_id", "atlan-argo");
        formData.put("client_secret", clientSecret);
        formData.put("grant_type", "client_credentials");

        if (StringUtils.isNotEmpty(userName)) {
            formData.put("username", userName);
            formData.put("password", ConfigReader.getString("beta.client.password"));
        }

        Request request = new Request.Builder()
                .url(keycloakUrl)
                .post(RequestBody.create(
                        MediaType.parse("application/x-www-form-urlencoded"),
                        formData.entrySet().stream()
                                .map(e -> e.getKey() + "=" + e.getValue())
                                .collect(Collectors.joining("&"))
                ))
                .build();

        try (Response response = OKClient.getClient().newCall(request).execute()) {
            if (response.code() == 200) {
                Map<String, Object> responseMap = fromJson(response.body().string(), Map.class);
                return responseMap.get("access_token") + "";
            } else {
                String errorBody = response.body() != null ? response.body().string() : "";
                throw new Exception("Failed to get token from Keycloak. Status: " + response.code() + ", Response: " + errorBody);
            }
        }
    }
}
