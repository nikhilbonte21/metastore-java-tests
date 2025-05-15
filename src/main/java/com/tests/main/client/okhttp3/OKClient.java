package com.tests.main.client.okhttp3;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.tests.main.utils.TestUtil.fromJson;
import static com.tests.main.utils.TestUtil.isRunAsAdmin;
import static com.tests.main.utils.TestUtil.isRunAsGuest;
import static com.tests.main.utils.TestUtil.isRunAsMember;
import static com.tests.main.utils.TestUtil.toJson;

public class OKClient {
    private static final Logger LOG = LoggerFactory.getLogger(OKClient.class);

    public static boolean isBeta = "beta".equals(ConfigReader.getString("atlas.client.mode", ""));

    private static String BASE_URL = isBeta ?
            ConfigReader.getString("beta.client.rest.address")
            : ConfigReader.getString("local.client.rest.address");

    private static String URL_TYPE_DEF = BASE_URL + "/types/typedefs";

    private static String URL_ENTITY = BASE_URL + "/entity";
    private static String URL_ENTITY_BULK = BASE_URL + "/entity/bulk";
    private static String URL_GET_ENTITY_GUID = BASE_URL + "/entity/guid/%s";

    private static OkHttpClient finalClient = null;

    public static String GOD, ADMIN, MEMBER, GUEST;

    public static OkHttpClient getClient() {
        if (finalClient == null) {
            synchronized (OKClient.class) {
                if (finalClient == null) {
                    finalClient = new OkHttpClient.Builder()
                            .connectTimeout(5, TimeUnit.MINUTES)
                            .readTimeout(5, TimeUnit.MINUTES)
                            .build();
                }
            }
        }

        return finalClient;
    }

    public static String getToken() {
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

    private static void loadTokens() {
        if (isBeta) {
            GOD = "Bearer " + ConfigReader.getString("beta.client.auth.token");
            ADMIN = "Bearer " + ConfigReader.getString("beta.client.admin.auth.token");

            MEMBER = "Bearer " + ConfigReader.getString("beta.client.member.auth.token");
            GUEST = "Bearer " + ConfigReader.getString("beta.client.guest.auth.token");
        } else {
            GOD = "Basic " + Base64.getEncoder().encodeToString("service-account-atlan-argo:admin".getBytes(StandardCharsets.UTF_8));
            ADMIN = "Basic " + Base64.getEncoder().encodeToString("nikhil.bonte:admin".getBytes(StandardCharsets.UTF_8));

            MEMBER = "Basic " + Base64.getEncoder().encodeToString("nikhil.member:admin".getBytes(StandardCharsets.UTF_8));
            GUEST = "Basic " + Base64.getEncoder().encodeToString("nikhil.guest:admin".getBytes(StandardCharsets.UTF_8));
        }
    }

    private static Request getRequest(String url) {
        return getRequest(url, null);
    }

    private static Request getRequestDELETE(String url) {
        Request.Builder builder = new Request.Builder()
                .url(url)
                .delete()
                .header("Authorization", getToken())
                .header("Content-Type", "application/json")
                .header("x-atlan-request-id", "tests-2.0-client");

        return builder.build();
    }

    private static Request getRequest(String url, Object payload) {

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

    public AtlasEntity.AtlasEntityWithExtInfo getEntityByGuid(String guid) throws Exception {
        Request request = getRequest(String.format(URL_GET_ENTITY_GUID, guid));

        return executeRequest(request, AtlasEntity.AtlasEntityWithExtInfo.class);
    }

    public EntityMutationResponse createEntity(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws Exception {
        Request request = getRequest(URL_ENTITY, entityWithExtInfo);

        return executeRequest(request, EntityMutationResponse.class);
    }

    public void createTypeDef(AtlasTypesDef typesDef) throws Exception {
        Request request = getRequest(URL_TYPE_DEF, typesDef);

        executeRequest(request, AtlasTypesDef.class);
    }

    public EntityMutationResponse createEntities(AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo) throws Exception {
        Request request = getRequest(URL_ENTITY_BULK, entitiesWithExtInfo);

        return executeRequest(request, EntityMutationResponse.class);
    }

    public EntityMutationResponse deleteEntityByGuid(String guid, String deleteType) throws Exception {
        String url = BASE_URL + "/entity/guid/" + guid;
        if (StringUtils.isNotEmpty(deleteType)) {
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
        queryParams.append(BASE_URL + "/entity/bulk?deleteType=HARD&");
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

    private <T> T executeRequest(Request request, Class<T> returnType) throws Exception {
        try (Response response = getClient().newCall(request).execute()) {
            if (response.code() == 200 || response.code() == 204) {
                return fromJson(response.body().string(), returnType);
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
    }
}
