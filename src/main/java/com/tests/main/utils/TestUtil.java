package com.tests.main.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tests.main.client.ClientBuilder;
import com.tests.main.client.okhttp3.OKClient;
import com.tests.main.lineage.AtlasLineageInfo;
import com.tests.main.lineage.AtlasLineageRequest;
import okhttp3.OkHttpClient;
import org.apache.atlas.model.audit.EntityAuditSearchResult;
import org.apache.atlas.model.discovery.AtlasSearchResult;
import com.tests.main.IndexSearchParams;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import com.tests.main.tests.glossary.models.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.relations.AtlasGlossaryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedCategoryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.atlas.model.glossary.relations.AtlasTermCategorizationHeader;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasStructDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.atlas.v1.model.instance.Struct;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.tests.main.tests.glossary.tests.TestsRunner.guidsToDelete;
import static com.tests.main.utils.Constants.DIMENSIONS;
import static com.tests.main.utils.Constants.FACTS;
import static com.tests.main.utils.Constants.TABLE;
import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class TestUtil {
    private static final Logger LOG = LoggerFactory.getLogger(TestUtil.class);

    public static String CONNECTION_PREFIX;

    private static final String at = "@";
    private static final String dot = ".";
    public static String[] ATLAS_URLS = {"http://localhost:21000"};
    public static String[] CREDS = {"admin", "admin"};
    public static OkHttpClient atlasClient = null;

    public static final String TYPE_GLOSSARY = "AtlasGlossary";
    public static final String TYPE_CATEGORY = "AtlasGlossaryCategory";
    public static final String TYPE_TERM = "AtlasGlossaryTerm";
    public static final String TYPE_PROCESS = "Process";
    public static final String TYPE_COLUMN_PROCESS = "ColumnProcess";
    public static final String TYPE_DATABASE = "Database";
    public static final String TYPE_SCHEMA = "Schema";
    public static final String TYPE_TABLE = "Table";
    public static final String TYPE_VIEW = "View";
    public static final String TYPE_COLUMN = "Column";
    public static final String TYPE_CONNECTION = "Connection";
    public static final String TYPE_POLICY = "AuthPolicy";

    public static final String ES_GLOSSARY = "__glossary";
    public static final String ES_CATEGORIES = "__categories";
    public static final String ES_MEANINGS = "__meanings";
    public static final String ES_MEANING_NAMES = "__meaningNames";
    public static final String ES_MEANING_TEXT = "__meaningsText";
    public static final String ES_PARENT_CAT = "__parentCategory";

    public static final String REL_ANCHOR = "anchor";
    public static final String REL_CATEGORIES = "categories";
    public static final String REL_PARENT_CAT = "parentCategory";
    public static final String REL_TERMS = "terms";
    public static final String REL_CHILDREN_CATS = "childrenCategories";
    public static final String REL_MEANINGS = "meanings";


    public static final String PREFIX_QUERY_QN       = "/default/collection/admin";
    public static final String TYPE_QUERY            = "Query";
    public static final String TYPE_QUERY_FOLDER     = "QueryFolder";
    public static final String TYPE_QUERY_COLLECTION = "QueryCollection";
    public static final String PARENT                = "parent";
    public static final String CHILDREN_FOLDERS      = "childrenFolders";
    public static final String CHILDREN_QUERIES      = "childrenQueries";

    public static final String TERM_ANCHOR              = "AtlasGlossaryTermAnchor";
    public static final String CATEGORY_ANCHOR          = "AtlasGlossaryCategoryAnchor";
    public static final String ANCHOR                   = "anchor";
    public static final String INPUTS_TO_P              = "inputToProcesses";
    public static final String OUTPUTS_FROM_P           = "outputFromProcesses";

    public static final String INPUTS            = "inputs";
    public static final String OUTPUTS           = "outputs";


    public static final String NAME = "name";
    public static final String QUALIFIED_NAME = "qualifiedName";
    public static final String CONNECTION_QUALIFIED_NAME = "connectionQualifiedName";

    public static final ObjectMapper mapper = new ObjectMapper();

    public static OKClient globalClient;

    private static boolean runAsGod = false;
    private static boolean runAsAdmin = false;
    private static boolean runAsMember = false;
    private static boolean runAsGuest = false;


    static {
        ClientBuilder.build();
    }

    public static void setAtlasClient(OkHttpClient atlasClient) {
        TestUtil.atlasClient = atlasClient;
    }

    public static OkHttpClient getClientLocal(String userName){
        return ClientBuilder.buildLocalClientWithCreds(userName, "admin");
    }

    private static OKClient getAtlasClient() {
        return globalClient;
    }

    public static void runAsGod() {
        runAsGod = true;
        runAsAdmin = false;
        runAsMember = false;
        runAsGuest = false;
    }

    public static void runAsAdmin() {
        runAsAdmin = true;
        runAsGod = false;
        runAsMember = false;
        runAsGuest = false;
    }

    public static void runAsMember() {
        runAsMember = true;
        runAsAdmin = false;
        runAsGod = false;
        runAsGuest = false;
    }

    public static void runAsGuest() {
        runAsGuest = true;
        runAsAdmin = false;
        runAsGod = false;
        runAsMember = false;
    }

    public static boolean isRunAsGod() {
        return runAsGod;
    }

    public static boolean isRunAsAdmin() {
        return runAsAdmin;
    }

    public static boolean isRunAsMember() {
        return runAsMember;
    }

    public static boolean isRunAsGuest() {
        return runAsGuest;
    }

    public static String getAdminStatus() throws Exception {
        //return getAtlasClient().getAdminStatus();
        return null;
    }

    public static EntityMutationResponse deleteEntities(List<String> guids) throws Exception {
        EntityMutationResponse response = getAtlasClient().deleteEntitiesByGuids(guids);
        return response;
    }

    public static EntityMutationResponse deleteEntityUniqAttr(String typeName, Map<String, String> uniqAttr) throws Exception {
        /*EntityMutationResponse response = getAtlasClient().deleteEntityByAttribute(typeName, uniqAttr);

        if (CollectionUtils.isNotEmpty(response.getDeletedEntities())) {
            guidsToDelete.remove(response.getDeletedEntities().get(0).getGuid());
        }

        return response;*/
        return null;
    }

    public static EntityMutationResponse deleteEntity(String guid, String deleteType) throws Exception {
        return getAtlasClient().deleteEntityByGuid(guid, deleteType);
    }

    public static EntityMutationResponse deleteEntityDefault(String guid) throws Exception {
        return getAtlasClient().deleteEntityByGuid(guid, null);
    }

    public static EntityMutationResponse deleteEntitySoft(String guid) throws Exception {
        return getAtlasClient().deleteEntityByGuid(guid, "SOFT");
    }

    public static EntityMutationResponse deleteEntityHard(String guid) throws Exception {
        return getAtlasClient().deleteEntityByGuid(guid, "HARD");
    }

    public static EntityMutationResponse deleteEntityPurge(String guid) throws Exception {
        return getAtlasClient().deleteEntityByGuid(guid, "PURGE");
    }

    public static void deleteRelationshipByGuid(String guid) throws Exception {
        //getAtlasClient().deleteRelationshipByGuid(guid);
    }

    public static void deleteRelationshipByGuids(List<String> guids) throws Exception {
        //getAtlasClient().deleteRelationshipByGuids(guids);
    }

    public static AtlasGlossary updateGlossary(String guid, AtlasGlossary glossary) throws Exception {
        //return getAtlasClient().updateGlossaryByGuid(guid, glossary);
        return null;
    }

    public static AtlasGlossaryTerm getTerm(String guid) throws Exception {
        //return getAtlasClient().getGlossaryTerm(guid);
        return null;
    }

    public static AtlasGlossaryTerm createTerm(AtlasGlossaryTerm glossary) throws Exception {
        //return getAtlasClient().createGlossaryTerm(glossary);
        return null;
    }

    public static AtlasGlossaryTerm updateTerm(String termGuid, AtlasGlossaryTerm term) throws Exception {
        //return getAtlasClient().updateGlossaryTermByGuid(termGuid, term);
        return null;
    }

    public static AtlasGlossaryTerm updateTermPartial(String guid, Map<String, String> attributes) throws Exception {
        //return getAtlasClient().partialUpdateTermByGuid(guid, attributes);
        return null;
    }

    public static AtlasGlossaryCategory createCategory(AtlasGlossaryCategory category) throws Exception {
        //return getAtlasClient().createGlossaryCategory(category);
        return null;
    }

    public static AtlasGlossaryCategory getCategory(String guid) throws Exception {
        //return getAtlasClient().getGlossaryCategory(guid);
        return null;
    }

    public static AtlasGlossaryCategory updateCategory(String categoryGuid, AtlasGlossaryCategory category) throws Exception {
        //return getAtlasClient().updateGlossaryCategoryByGuid(categoryGuid, category);
        return null;
    }

    public static AtlasRelationship getRelationshipByGuid(String relationshipGuid) throws Exception {
        //return getAtlasClient().getRelationshipByGuid(relationshipGuid).getRelationship();
        return null;
    }

    public static AtlasRelationship createRelationship(AtlasRelationship relationship) throws Exception {
        //return getAtlasClient().createRelationship(relationship);
        return null;
    }

    public static List<AtlasRelationship> createRelationships(List<AtlasRelationship> relationships) throws Exception {
        //return getAtlasClient().createRelationships(relationships);
        return null;
    }

    public static AtlasRelationship updateRelationship(AtlasRelationship relationship) throws Exception {
        //return getAtlasClient().updateRelationship(relationship);
        return null;
    }

    public static void updateRelationship(String relationshipGuid) throws Exception {
        //getAtlasClient().deleteRelationshipByGuid(relationshipGuid);
    }

    public static void addClassificationsBulk(ClassificationAssociateRequest request) throws Exception {
        //getAtlasClient().addClassificationsBulk(request);
    }

    public static EntityMutationResponse updateEntity(AtlasEntity entity) throws Exception {
        return createEntitiesBulk(entity);
    }

    public static EntityMutationResponse updateEntitiesBulk(AtlasEntity... entities) throws Exception {
        return createEntitiesBulk(entities);
    }

    public static EntityMutationResponse createEntity(AtlasEntity entity) throws Exception {
        return createEntity(new AtlasEntity.AtlasEntityWithExtInfo(entity));
    }

    public static void repairEntityByGuid(String guid) throws Exception {
        getAtlasClient().repairEntityByGuid(guid);
    }

    public static void repairEntitiesByGuid(List<String> guids) throws Exception {
        getAtlasClient().repairEntitiesByGuid(guids);
    }

    public static AtlasEntity createAndGetEntity(AtlasEntity entity) throws Exception {
        EntityMutationResponse response = createEntity(new AtlasEntity.AtlasEntityWithExtInfo(entity));

        return getEntity(response.getCreatedEntities().get(0).getGuid());
    }
    public static AtlasEntity updateAndGetEntity(AtlasEntity entity) throws Exception {
        EntityMutationResponse response = createEntity(new AtlasEntity.AtlasEntityWithExtInfo(entity));

        return getEntity(response.getUpdatedEntities().get(0).getGuid());
    }

    public static EntityMutationResponse createEntity(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws Exception {
        EntityMutationResponse response = getAtlasClient().createEntity(entityWithExtInfo);
        if (CollectionUtils.isNotEmpty(response.getCreatedEntities())) {
            guidsToDelete.add(response.getCreatedEntities().get(0).getGuid());
        }
        return response;
    }

    public static EntityMutationResponse createEntitiesWithTag(AtlasEntity entity,
                                                               boolean replaceClass, boolean appendClass) throws Exception {
        /*AtlasEntity.AtlasEntitiesWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo(entity);

        EntityMutationResponse response = getAtlasClient().createEntitiesWithTag(entityWithExtInfo, replaceClass, appendClass);
        if (CollectionUtils.isNotEmpty(response.getCreatedEntities())) {
            response.getCreatedEntities().forEach(x -> guidsToDelete.add(x.getGuid()));
        }
        return response;*/
        return null;
    }

    public static EntityAuditSearchResult getEntityAudit(String entityGuid) throws Exception {
        Map params = new HashMap<>();
        Map dsl = mapOf("sort", mapOf("created", mapOf("order", "desc")));
        dsl.put("query", mapOf("bool", mapOf("must", Arrays.asList(
                mapOf("term", mapOf("entityId", entityGuid)),
                mapOf("term", mapOf("action", "ENTITY_UPDATE")),
                mapOf("term", mapOf("headers.x-atlan-request-id", "tests-2.0-client"))
        ))));
        params.put("dsl", dsl);

        return getAtlasClient().getEntityAudit(params);
    }

    public static Map mapOf(Object... items) throws Exception {
        Map ret = new HashMap();

        if (items != null) {
            int size = items.length;
            if (size % 2 == 0) {
                for (int i = 0 ; i < size; i= i + 2) {
                    ret.put(items[i], items[i+1]);
                }
            } else {
                throw new Exception("Items size must be even to add into the map");
            }

        } else {
            throw new Exception("Please pass items to add into map");
        }

        return ret;
    }

    public static Set setOf(Object... items) throws Exception {
        Set ret = new HashSet<>();

        if (items != null) {
            for (int i = 0 ; i < items.length; i++) {
                ret.add(items[i]);
            }

        } else {
            throw new Exception("Please pass items to add into map");
        }

        return ret;
    }

    public static List listOf(Object... items) throws Exception {
        List<Object> ret = new ArrayList();

        if (items != null) {
            int size = items.length;

            ret.addAll(Arrays.asList(items).subList(0, size));

        } else {
            throw new Exception("Please pass items to add into list");
        }

        return ret;
    }

    public static EntityMutationResponse createEntitiesBulk(List<AtlasEntity> entities) throws Exception {
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.setEntities(entities);

        return createEntitiesBulk(entitiesWithExtInfo);
    }

    public static EntityMutationResponse createEntitiesBulk(AtlasEntity... entities) throws Exception {
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        Arrays.stream(entities).forEach(entitiesWithExtInfo::addEntity);

        //LOG.info("\n" + TestUtil.toJson(entities) + "\n");

        return createEntitiesBulk(entitiesWithExtInfo);
    }

    public static EntityMutationResponse createEntitiesBulk(AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo) throws Exception {
        EntityMutationResponse response =  getAtlasClient().createEntities(entitiesWithExtInfo);
        if (CollectionUtils.isNotEmpty(response.getCreatedEntities())) {
            guidsToDelete.addAll(response.getGuidAssignments().values());
        }

        return response;
    }

    public static String createClassification(String name) throws Exception {
        /*AtlasTypesDef typesDef = new AtlasTypesDef();
        AtlasClassificationDef classificationDef = new AtlasClassificationDef();
        classificationDef.setName(name);
        classificationDef.setDisplayName(name);
        typesDef.setClassificationDefs(Collections.singletonList(classificationDef));


        return getAtlasClient().createAtlasTypeDefs(typesDef).getClassificationDefs().get(0).getName();*/
        return null;
    }

    public static AtlasBusinessMetadataDef getBusinessMetadataDef(String bmName) throws Exception {
        AtlasBusinessMetadataDef businessMetadataDef = new AtlasBusinessMetadataDef();
        businessMetadataDef.setName(bmName);

        return businessMetadataDef;
    }

    public static AtlasStructDef.AtlasAttributeDef getBMAttrDef(String attrName, String attrType, String... applicableTypes) throws Exception {
        AtlasStructDef.AtlasAttributeDef attr = new AtlasStructDef.AtlasAttributeDef(attrName, attrType);
        attr.setDisplayName(attrName);
        attr.setOption("applicableEntityTypes", Utils.toJson(applicableTypes));
        attr.setOption("maxStrLength", "200");

        attr.setIsOptional(true);
        attr.setIsUnique(false);
        attr.setCardinality(AtlasStructDef.AtlasAttributeDef.Cardinality.SINGLE);

        return attr;
    }

    public static List<AtlasClassificationDef> createClassificationDefs(List<AtlasClassificationDef> tags) throws Exception {
        AtlasTypesDef typesDef = new AtlasTypesDef();
        typesDef.setClassificationDefs(tags);

        //return getAtlasClient().createAtlasTypeDefs(typesDef).getClassificationDefs();
        return null;

    }

    public static void deleteTypeByName(String typeName) throws Exception {
        //getAtlasClient().deleteTypeByName(typeName);
    }

    public static void addClassifications(String guid, List<AtlasClassification> classifications) throws Exception {
        //getAtlasClient().addClassifications(guid, classifications);
    }

    public static void removeClassification(String entityGuid, String tagName) throws Exception {
        //getAtlasClient().removeClassification(entityGuid, tagName, null);
    }

    public static AtlasEntity getEntity(String guid) throws Exception {
        return getAtlasClient().getEntityByGuid(guid).getEntity();
    }

    public static AtlasTypesDef createTypeDefs(AtlasTypesDef typesDef) throws Exception {
        return getAtlasClient().createTypeDef(typesDef);
    }

    public static AtlasTypesDef getBMDefs() throws Exception {
        return getAtlasClient().getBMDefs();
    }

    public static AtlasTypesDef getTagDefs() throws Exception {
        return getAtlasClient().getTagDefs();
    }

    public static AtlasSearchResult indexSearch(IndexSearchParams indexSearchParams) throws Exception {
        return getAtlasClient().indexSearch(indexSearchParams);
    }

    public static AtlasLineageInfo getLineageInfo(String guid, AtlasLineageInfo.LineageDirection direction, int depth, boolean hideProcess) throws Exception {
        //return getAtlasClient().getLineageInfo(guid, direction, depth, hideProcess);
        return null;
    }

    public static AtlasLineageInfo getLineageInfo(AtlasLineageRequest lineageRequest) throws Exception {
        //return getAtlasClient().getLineageInfoPost(lineageRequest);
        return null;
    }


    public static AtlasGlossary getGlossaryModel() {
       return getGlossaryModel(getRandomName());
    }

    public static AtlasGlossary getGlossaryModel(String name) {
        AtlasGlossary glossary = new AtlasGlossary();
        glossary.setName(name);
        glossary.setShortDescription("Short description");
        glossary.setLongDescription("Long description");

        return glossary;
    }

    public static AtlasGlossaryTerm getTermModel(String glossaryGuid) {
        return getTermModel(getRandomName(), glossaryGuid);
    }

    public static AtlasGlossaryTerm getTermModel(String termName, String glossaryGuid) {
        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        term.setName(termName);
        term.setShortDescription("Short description");
        term.setLongDescription("Long description");
        term.setAnchor(getGlossaryHeader(glossaryGuid));

        return term;
    }


    public static AtlasRelatedCategoryHeader getCategoryParentModel(AtlasGlossaryCategory category) {
        AtlasRelatedCategoryHeader parentHeader = new AtlasRelatedCategoryHeader();
        parentHeader.setCategoryGuid(category.getGuid());

        return parentHeader;
    }

    public static AtlasGlossaryCategory getCategoryModelWithParent(String glossaryGuid, AtlasRelatedCategoryHeader parentCategory) {
        AtlasGlossaryCategory category = getCategoryModel(getRandomName(), glossaryGuid);
        category.setParentCategory(parentCategory);
        return category;
    }

    public static AtlasGlossaryCategory getCategoryModelWithParent(String categoryName, String glossaryGuid, AtlasRelatedCategoryHeader parentCategory) {
        AtlasGlossaryCategory category = getCategoryModel(getRandomName(), glossaryGuid);
        category.setName(categoryName);
        category.setParentCategory(parentCategory);
        return category;
    }

    public static AtlasGlossaryCategory getCategoryModel(String glossaryGuid) {
        return getCategoryModel(getRandomName(), glossaryGuid);
    }

    public static AtlasGlossaryCategory getCategoryModel(String categoryName, String glossaryGuid) {
        AtlasGlossaryCategory category = new AtlasGlossaryCategory();
        category.setName(categoryName);
        category.setAnchor(getGlossaryHeader(glossaryGuid));

        return category;
    }

    public static AtlasGlossaryHeader getGlossaryHeader(String glossaryGuid) {
        AtlasGlossaryHeader glossaryHeader = new AtlasGlossaryHeader();
        glossaryHeader.setGlossaryGuid(glossaryGuid);

        return glossaryHeader;
    }

    public static String getRandomName() {
        synchronized (TestUtil.class) {
            return RandomStringUtils.randomAlphanumeric(16);
        }
    }

    public static void cleanUpAll() throws Exception {
        LOG.info("Performing cleanup...");
        deleteAllEntities();
    }

    public static void deleteTypeDefByName(String typeName) throws Exception {
        getAtlasClient().deleteTypeDefByName(typeName);
    }

    public static void cleanUpAllForEach() throws Exception {
        deleteAllEntitiesForEach();
    }

    public static void cleanUpAll(boolean all) throws Exception {
        deleteAllEntities();
    }

    public static CatNCatTermHeader getCatHeader(int count, String glossaryGuid) throws Exception {
        CatNCatTermHeader ret = new CatNCatTermHeader();

        for (int i = 0; i < count; i++) {
            AtlasGlossaryCategory category = createCategory(getCategoryModel(glossaryGuid));
            AtlasTermCategorizationHeader categorizationHeader = new AtlasTermCategorizationHeader();
            categorizationHeader.setCategoryGuid(category.getGuid());

            ret.addCategory(category);
            ret.addHeader(categorizationHeader);
        }

        return ret;
    }


    public static TermNTermHeader getTermHeader(int count, String glossaryGuid) throws Exception {
        TermNTermHeader ret = new TermNTermHeader();

        for (int i = 0; i < count; i++) {
            AtlasGlossaryTerm term = createTerm(getTermModel(glossaryGuid));
            AtlasRelatedTermHeader termHeader = new AtlasRelatedTermHeader();
            termHeader.setTermGuid(term.getGuid());

            ret.addTerm(term);
            ret.addHeader(termHeader);
        }

        return ret;
    }

    public static class CatNCatTermHeader {
        private Set<AtlasGlossaryCategory> categories = new HashSet<>();
        private Set<AtlasTermCategorizationHeader> headers = new HashSet<>();

        public Set<AtlasGlossaryCategory> getCategories() {
            return categories;
        }

        public void addCategory(AtlasGlossaryCategory category) {
            this.categories.add(category);
        }

        public Set<AtlasTermCategorizationHeader> getHeaders() {
            return headers;
        }

        public void addHeader(AtlasTermCategorizationHeader header) {
            this.headers.add(header);
        }

        public CatNCatTermHeader addAll(CatNCatTermHeader other) {
            this.categories.addAll(other.getCategories());
            this.headers.addAll(other.getHeaders());
            return this;
        }
    }

    public static class TermNTermHeader {
        private Set<AtlasGlossaryTerm> terms = new HashSet<>();
        private Set<AtlasRelatedTermHeader> headers = new HashSet<>();

        public Set<AtlasGlossaryTerm> getTerms() {
            return terms;
        }

        public void addTerm(AtlasGlossaryTerm term) {
            this.terms.add(term);
        }

        public Set<AtlasRelatedTermHeader> getHeaders() {
            return headers;
        }

        public void addHeader(AtlasRelatedTermHeader header) {
            this.headers.add(header);
        }

        public TermNTermHeader addAll(TermNTermHeader other) {
            this.terms.addAll(other.getTerms());
            this.headers.addAll(other.getHeaders());
            return this;
        }
    }

    public static AtlasObjectId getObjectId(String guid, String type) {
        return new AtlasObjectId(guid, type);
    }

    public static AtlasRelatedObjectId getAnchorRelatedObject(String glossaryGuid) {
        return AtlasTypeUtil.getAtlasRelatedObjectId(getObjectId(glossaryGuid, TYPE_GLOSSARY), "AtlasGlossaryCategoryAnchor");
    }

    public static AtlasObjectId getAnchorObjectId(String glossaryGuid) {
        return getObjectId(glossaryGuid, TYPE_GLOSSARY);
    }

    public static AtlasObjectId getParentCategoryObjectId(String parentGuid) {
        return getObjectId(parentGuid, TYPE_CATEGORY);
    }

    public static List<AtlasObjectId> getCategoryObjectIds(String... childrenGuids) {
        return getObjectIdsAsList(TYPE_CATEGORY, childrenGuids);
    }

    public static List<AtlasObjectId> getObjectIdsAsList(String type, String... guids) {
        List<AtlasObjectId> ret = new ArrayList<>();

        for (String guid : guids) {
            ret.add(new AtlasObjectId(guid, type));
        }
        return ret;
    }

    public static AtlasRelatedObjectId getTermRelatedObject(String termGuid) {
        return AtlasTypeUtil.getAtlasRelatedObjectId(new AtlasObjectId(termGuid, TYPE_TERM), "AtlasGlossaryTermCategorization");
    }

    public static void deleteAllEntities() throws Exception {
        if (CollectionUtils.isNotEmpty(guidsToDelete)) {
            LOG.info("Deleting {} entities", guidsToDelete.size());
            List<List<String>> chunks = ListUtils.partition(guidsToDelete, 20);
            for (List<String> subList : chunks) {
                TestUtil.deleteEntities(subList);
            }
        }
        guidsToDelete.clear();
    }

    public static void deleteAllEntitiesForEach() throws Exception {
        if (CollectionUtils.isNotEmpty(guidsToDelete)) {
            LOG.info("Deleting {} entities", guidsToDelete.size());
            Collections.reverse(guidsToDelete);
            for (String guid : guidsToDelete) {
                TestUtil.deleteEntityHard(guid);
            }
        }
        guidsToDelete.clear();
    }

    public static AtlasEntity createCategory(String gloGuid, String parentGuid, String... childGuids) throws Exception {
        AtlasEntity entity = new AtlasEntity(TYPE_CATEGORY);
        String name = getRandomName();
        entity.setAttribute(NAME, name);
        entity.setAttribute(QUALIFIED_NAME, name);

        entity.setRelationshipAttribute("anchor", getAnchorObjectId(gloGuid));

        if (StringUtils.isNotEmpty(parentGuid)) {
            entity.setRelationshipAttribute("parentCategory", getParentCategoryObjectId(parentGuid));
        }

        if (ArrayUtils.isNotEmpty(childGuids)) {
            entity.setRelationshipAttribute("childrenCategories", getCategoryObjectIds(childGuids));
        }

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        entityWithExtInfo.setEntity(entity);

        return getEntity(TestUtil.createEntity(entityWithExtInfo).getCreatedEntities().get(0).getGuid());
    }

    public static AtlasEntity createCustomEntity(String typeName, String entityName) throws Exception {

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = getAtlasEntityExt(typeName, entityName);

        return getEntity(TestUtil.createEntity(entityWithExtInfo).getCreatedEntities().get(0).getGuid());

    }

    public static AtlasEntity.AtlasEntityWithExtInfo getAtlasEntityQuery(String entityName,
                                                                         String collectionQualifiedName) {
        AtlasEntity entity = new AtlasEntity(TYPE_QUERY);
        entityName = StringUtils.isNotEmpty(entityName) ? entityName : getRandomName();
        entity.setAttribute(NAME, entityName);
        entity.setAttribute(QUALIFIED_NAME, entityName + "_" + getRandomName());
        entity.setAttribute("rawQuery", "select * from " + getRandomName());
        entity.setAttribute("collectionQualifiedName", collectionQualifiedName);
        entity.setAttribute("parentQualifiedName", "parent");

        return new AtlasEntity.AtlasEntityWithExtInfo(entity);
    }

    public static AtlasEntity.AtlasEntityWithExtInfo getAtlasEntityQueryFolder(String entityName,
                                                                         String collectionQualifiedName) {
        AtlasEntity entity = new AtlasEntity(TYPE_QUERY_FOLDER);
        entityName = StringUtils.isNotEmpty(entityName) ? entityName : getRandomName();
        entity.setAttribute(NAME, entityName);
        entity.setAttribute(QUALIFIED_NAME, entityName + "_" + getRandomName());
        entity.setAttribute("collectionQualifiedName", collectionQualifiedName);
        entity.setAttribute("parentQualifiedName", "parent");

        return new AtlasEntity.AtlasEntityWithExtInfo(entity);
    }

    public static AtlasEntity getAtlasEntity(String typeName, String entityName) {
        AtlasEntity entity = new AtlasEntity(typeName);
        //entityName = StringUtils.isNotEmpty(entityName) ? entityName + "_" + getRandomName() : getRandomName();
        entityName = entityName + getRandomName();
        entityName = CONNECTION_PREFIX + entityName;
        entity.setAttribute(NAME, entityName);
        entity.setAttribute(QUALIFIED_NAME, entityName + "_" + System.currentTimeMillis());

        //LOG.info("entity.qualifiedName : " + entity.getAttribute(QUALIFIED_NAME));

        return entity;
    }

    public static AtlasEntity.AtlasEntityWithExtInfo getAtlasEntityExt(String typeName, String entityName) {
        AtlasEntity entity = getAtlasEntity(typeName, entityName);
        return new AtlasEntity.AtlasEntityWithExtInfo(entity);
    }

    public static AtlasEntity createCategory(String gloGuid, String catName, String parentGuid, String... childGuids) throws Exception {
        AtlasEntity entity = new AtlasEntity(TYPE_CATEGORY);
        entity.setAttribute(NAME, catName);
        entity.setAttribute(QUALIFIED_NAME, catName);

        entity.setRelationshipAttribute("anchor", getAnchorObjectId(gloGuid));

        if (StringUtils.isNotEmpty(parentGuid)) {
            entity.setRelationshipAttribute("parentCategory", getParentCategoryObjectId(parentGuid));
        }

        if (ArrayUtils.isNotEmpty(childGuids)) {
            entity.setRelationshipAttribute("childrenCategories", getCategoryObjectIds(childGuids));
        }

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo(entity);

        return getEntity(TestUtil.createEntity(entityWithExtInfo).getCreatedEntities().get(0).getGuid());
    }

    public static AtlasEntity createGlossary() throws Exception {
        return createGlossary(getRandomName());
    }

    public static AtlasEntity createGlossary(String name) throws Exception {
        AtlasEntity entity = new AtlasEntity(TYPE_GLOSSARY);
        name = name + "_" + getRandomName();
        entity.setAttribute(NAME, name);
        entity.setAttribute(QUALIFIED_NAME, name);

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        entityWithExtInfo.setEntity(entity);

        AtlasEntity ret = getEntity(TestUtil.createEntity(entityWithExtInfo).getCreatedEntities().get(0).getGuid());
        guidsToDelete.add(ret.getGuid());
        return ret;
    }

    public static String getNanoid(String qualifiedName){
        String[] at = qualifiedName.split("@");
        String[] dot = at[0].split("\\.");

        return dot[dot.length-1];
    }

    public static String getQualifiedName(AtlasEntity entity) {
        return (String) entity.getAttribute("qualifiedName");
    }

    public static String concat(String... items) {
        StringBuilder sb = new StringBuilder();
        int size = items.length;

        for (int i = 0; i < size; i++) {
            sb.append(items[i]);

            if (i == size-2) {
                sb.append(at);
            } else {
                sb.append(dot);
            }
        }
        return sb.substring(0, sb.length()-1);
    }

    public static Map<String, String> getParentRelationshipAttribute(AtlasEntity entity){
        return (Map<String, String>) entity.getRelationshipAttribute("parentCategory");
    }

    public static List<Map> getCategoriesRelationshipAttribute(AtlasEntity entity){
        return (List<Map>) entity.getRelationshipAttribute("categories");
    }

    public static List<Map> getTermsRelationshipAttribute(AtlasEntity entity){
        return (List<Map>) entity.getRelationshipAttribute("terms");
    }

    public static Map<String, String> getAnchorRelationshipAttribute(AtlasEntity entity){
        return (Map<String, String>) entity.getRelationshipAttribute("anchor");
    }

    public static void verifyESInLoop(String entityGuid, Map<String, String> whiteMap, String... blackAttrs) throws Exception {
        long maxWait = 90 * 1000;
        long interval = 10 * 1000;
        long elapsed = 0;

        while (true) {
            try {
                Thread.sleep(2000);

                verifyES(entityGuid, whiteMap, blackAttrs);
                LOG.info("Found updated ES attributes ...........................................");
                break;
            } catch (AssertionError ase) {
                elapsed += interval;

                if (maxWait < elapsed) {
                    throw ase;
                }

                try {
                    LOG.info("attributes not updated yet, waiting more ...........................................");
                    LOG.info("maxWait {} elapsed {} ", maxWait, elapsed);
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    throw e;
                }
            }
        }
    }

    public static void verifyES(String entityGuid, Map<String, String> whiteMap, String... blackAttrs) {

        SearchHit[] searchHit = ESUtil.searchWithGuid(entityGuid).getHits().getHits();

        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            if (MapUtils.isNotEmpty(whiteMap)) {
                for (String attrName : whiteMap.keySet()) {
                    Object esValue = sourceAsMap.get(attrName);

                    if (esValue instanceof List) {
                        List<String> values = (List<String>) esValue;

                        assertNotNull(attrName, values);
                        assertTrue(attrName, values.contains(whiteMap.get(attrName)));
                    } else {
                        String value = (String) esValue;

                        assertNotNull(attrName, value);
                        assertEquals(attrName, whiteMap.get(attrName), value);
                    }
                }
            }

            if (ArrayUtils.isNotEmpty(blackAttrs)) {
                for (String attrName : blackAttrs) {
                    try {
                        assertNull(attrName, sourceAsMap.get(attrName));
                    } catch (AssertionError ase) {
                        Object esValue = sourceAsMap.get(attrName);

                        if (esValue instanceof List) {
                            List<String> values = (List<String>) esValue;
                            assertEquals(0, values.size());
                        } else {
                            throw ase;
                        }
                    }
                }
            }
        }
    }

    public static Map<String, Object> getESDoc(String entityGuid) {

        SearchHit[] searchHit = ESUtil.searchWithGuid(entityGuid).getHits().getHits();
        return searchHit[0].getSourceAsMap();
    }

    public static void verifyESHasNot(String entityGuid, String... blackAttrs) {

        SearchHit[] searchHit = ESUtil.searchWithGuid(entityGuid).getHits().getHits();

        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            if (ArrayUtils.isNotEmpty(blackAttrs)) {
                for (String attrName : blackAttrs) {
                    assertNull(sourceAsMap.get(attrName));
                }
            }
        }
    }

    public static void verifyESGlossary(String catGuid, String expectedGloQName) {

        SearchHit[] searchHit = ESUtil.searchWithGuid(catGuid).getHits().getHits();
        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            assertNotNull(sourceAsMap.get("__glossary"));
            String qName = (String) sourceAsMap.get("__glossary");
            assertEquals(expectedGloQName, qName);

            assertNull(sourceAsMap.get("__parentCategory"));
        }
    }

    public static void verifyESHasLineage(String entityGuid) {
        verifyESHasLineage(entityGuid, false);
    }

    public static void verifyESHasLineage(String entityGuid, boolean expectedNull) {

        SearchHit[] searchHit = ESUtil.searchWithGuid(entityGuid).getHits().getHits();
        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            if (expectedNull) {
                assertNull(sourceAsMap.get("__hasLineage"));
            } else {
                assertNotNull(sourceAsMap.get("__hasLineage"));
                boolean value = (boolean) sourceAsMap.get("__hasLineage");
                assertTrue(value);
            }
        }
    }

    public static Map<String, Object> getESAlias(String aliasName) {
        return ESUtil.runAliasGetQuery(aliasName);
    }

    public static void sleep() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void sleep(long millis) {
        try {
            LOG.info("Sleeping for {} seconds", millis / 1000);
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static String getUUID(){
        return NanoIdUtils.randomNanoId();
    }

    public static Map<String, Object> getMap(String key, Object value) {
        Map<String, Object> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    public static Map<String, Object> getMap(Object... args) {
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < args.length; i = i + 2) {
            map.put( (String) args[i], args[i+1]);
        }

        return map;
    }

    public static String toJson(Object obj) {
        try {
            return mapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static <T> T fromJson(String jsonStr, Class<T> type) {
        T ret = null;

        if (jsonStr != null) {
            try {
                ret = mapper.readValue(jsonStr, type);

                if (ret instanceof Struct) {
                    ((Struct) ret).normalize();
                }
            } catch (IOException e) {
                LOG.error("AtlasType.fromJson()", e);

                ret = null;
            }
        }

        return ret;
    }

    public static Map<String, Object> verifyESAttributes(String assetGuid, Map<String, Object> expectedAttributes) throws Exception {
        SearchHit[] searchHits = ESUtil.searchWithGuid(assetGuid).getHits().getHits();
        assertTrue("No ES document found for guid: " + assetGuid, searchHits.length > 0);

        for (SearchHit hit : searchHits) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            
            // Verify each expected attribute
            for (Map.Entry<String, Object> expectedEntry : expectedAttributes.entrySet()) {
                String attrName = expectedEntry.getKey();
                Object expectedValue = expectedEntry.getValue();
                Object actualValue = sourceAsMap.get(attrName);

                if (expectedValue == null) {
                    assertFalse(sourceAsMap.containsKey(expectedValue));
                } else {
                    // Verify attribute exists
                    assertNotNull("Attribute '" + attrName + "' not found in ES document", actualValue);

                    // Handle different value types
                    if (expectedValue instanceof List) {
                        // Handle list values - order doesn't matter
                        assertTrue("Attribute '" + attrName + "' should be a List", actualValue instanceof List);
                        List<?> expectedList = (List<?>) expectedValue;
                        List<?> actualList = (List<?>) actualValue;
                        
                        // Verify no extra values
                        assertEquals("Attribute '" + attrName + "' has unexpected size: ",
                                expectedList.size(), actualList.size());

                        // Verify all expected values are present
                        for (Object expectedItem : expectedList) {
                            boolean found = false;
                            for (Object actualItem : actualList) {
                                if (isValueEqual(expectedItem, actualItem)) {
                                    found = true;
                                    break;
                                }
                            }
                            assertTrue("Expected value '" + expectedItem + "' not found in attribute '" + attrName + "', actual values are " + actualList, found);
                        }
                    } else {
                        // Handle simple values
                        assertTrue("Attribute '" + attrName + "' value mismatch, expected: " + expectedValue + ", actual: " + actualValue,
                                isValueEqual(expectedValue, actualValue));
                    }
                }
            }

            return sourceAsMap;
        }

        return null;
    }

    private static boolean isValueEqual(Object expected, Object actual) {
        if (expected == null) {
            return actual == null;
        }
        if (actual == null) {
            return false;
        }

        // Handle number types
        if (expected instanceof Number && actual instanceof Number) {
            // Handle floating point numbers specifically
            if (expected instanceof Float || expected instanceof Double || 
                actual instanceof Float || actual instanceof Double) {
                // Convert both to float and round to 6 decimal places
                float expectedFloat = Math.round(((Number) expected).floatValue() * 1000000.0f) / 1000000.0f;
                float actualFloat = Math.round(((Number) actual).floatValue() * 1000000.0f) / 1000000.0f;
                return expectedFloat == actualFloat;
            }
            
            // For other number types (Integer, Long, etc.), use string comparison
            return String.valueOf(expected).equals(String.valueOf(actual));
        }

        // Handle other types by converting to string
        return String.valueOf(expected).equals(String.valueOf(actual));
    }

    public static void verifyESDocumentNotPresent(String... assetGuids) throws Exception {
        for (String guid : assetGuids) {
            SearchHit[] searchHits = ESUtil.searchWithGuid(guid).getHits().getHits();
            assertEquals("ES document should not exist for guid: " + guid, 0, searchHits.length);
        }
    }

    public static void addOrUpdateCMAttrBulk(String assetGuid, Map<String, Object> bm) throws Exception {
        getAtlasClient().addOrUpdateCMAttrBulk(assetGuid, bm);
    }

    public static void addOrUpdateCMAttr(String assetGuid, String bmName, Map<String, Object> bmAttrs) throws Exception {
        getAtlasClient().addOrUpdateCMAttr(assetGuid, bmName, bmAttrs);
    }

    public static List<String> createColumnsForTable(String tableGuid, int numColumns) throws Exception {
        List<AtlasEntity> entitiesToCreate = new ArrayList<>();

        for (int i = 0; i < numColumns; i++) {
            AtlasEntity column = getAtlasEntity(TYPE_COLUMN, "test_column_" + i + "_" + getRandomName());
            column.setRelationshipAttribute(TABLE, getObjectId(tableGuid, TYPE_TABLE));
            entitiesToCreate.add(column);
        }

        EntityMutationResponse response = createEntitiesBulk(entitiesToCreate);
        return response.getCreatedEntities().stream().map(AtlasEntityHeader::getGuid).collect(Collectors.toList());
    }

    public static List<String> createTables(int numTables) throws Exception {
        List<AtlasEntity> entitiesToCreate = new ArrayList<>();

        for (int i = 0; i < numTables; i++) {
            AtlasEntity dimensionTable = getAtlasEntity(TYPE_TABLE, "test_table_" + i + "_" + getRandomName());
            entitiesToCreate.add(dimensionTable);
        }

        EntityMutationResponse response = createEntitiesBulk(entitiesToCreate);
        return response.getCreatedEntities().stream().map(AtlasEntityHeader::getGuid).collect(Collectors.toList());
    }

    public static List<String> createDimensionTablesForFact(int numDimensions, String... factTableGuids) throws Exception {
        List<AtlasEntity> entitiesToCreate = new ArrayList<>();

        // Create dimension tables with fact relationship
        for (int i = 0; i < numDimensions; i++) {
            AtlasEntity dimensionTable = getAtlasEntity(TYPE_TABLE, "test_dimension_table_" + i + "_" + getRandomName());
            dimensionTable.setRelationshipAttribute(FACTS, getObjectIdsAsList(TYPE_TABLE, factTableGuids));
            entitiesToCreate.add(dimensionTable);
        }

        EntityMutationResponse response = createEntitiesBulk(entitiesToCreate);
        return response.getCreatedEntities().stream().map(AtlasEntityHeader::getGuid).collect(Collectors.toList());
    }

    public static List<String> createFactTablesForDimension(String dimensionTableGuid, int numFacts) throws Exception {
        List<AtlasEntity> entitiesToCreate = new ArrayList<>();

        // Create Fact tables with fact relationship
        for (int i = 0; i < numFacts; i++) {
            AtlasEntity dimensionTable = getAtlasEntity(TYPE_TABLE, "test_fact_table_" + i + "_" + getRandomName());
            dimensionTable.setRelationshipAttribute(DIMENSIONS, Collections.singletonList(getObjectId(dimensionTableGuid, TYPE_TABLE)));
            entitiesToCreate.add(dimensionTable);
        }

        EntityMutationResponse response = createEntitiesBulk(entitiesToCreate);
        return response.getCreatedEntities().stream().map(AtlasEntityHeader::getGuid).collect(Collectors.toList());
    }

    public static List<String> createChildrenTerms(String glossaryGuid, int numChildren) throws Exception {
        List<AtlasEntity> entitiesToCreate = new ArrayList<>();

        for (int i = 0; i < numChildren; i++) {
            AtlasEntity term = getAtlasEntity(TYPE_TERM, "test_term_" + i + "_" + getRandomName());
            term.setRelationshipAttribute(ANCHOR, getObjectId(glossaryGuid, TYPE_GLOSSARY));
            entitiesToCreate.add(term);
        }

        return createChildren(entitiesToCreate);
    }

    public static List<String> createChildrenCategories(String glossaryGuid, int numChildren) throws Exception {
        List<AtlasEntity> entitiesToCreate = new ArrayList<>();

        List<String> termGuids = createChildrenTerms(glossaryGuid, numChildren);

        for (int i = 0; i < numChildren; i++) {
            AtlasEntity category = getAtlasEntity(TYPE_CATEGORY, "test_category_" + i + "_" + getRandomName());
            category.setRelationshipAttribute(ANCHOR, getObjectId(glossaryGuid, TYPE_GLOSSARY));
            category.setRelationshipAttribute("terms", Collections.singletonList(getObjectId(termGuids.get(i), TYPE_TERM)));
            entitiesToCreate.add(category);
        }

        return createChildren(entitiesToCreate);
    }

    public static Map<String, String> getSourceTagValuePairs(List<Map<String, Object>> sourceTagValues) {
        Map<String, String> ret = new HashMap<>(sourceTagValues.size());

        for (Map<String, Object> item: sourceTagValues) {
            if (item.containsKey("attributes")) {
                item = (Map<String, Object>) item.get("attributes");
            }
            ret.put(item.get("tagAttachmentKey").toString(), item.get("tagAttachmentValue").toString());
        }

        return ret;
    }

    private static List<String> createChildren(List<AtlasEntity> entitiesToCreate) throws Exception {
        List<String> childrenGuids = new ArrayList<>();

        // Create all entities in one request
        EntityMutationResponse response = createEntitiesBulk(entitiesToCreate);
        for (AtlasEntityHeader createdEntity : response.getCreatedEntities()) {
            childrenGuids.add(createdEntity.getGuid());
        }

        return childrenGuids;
    }

    public static Map<String, Object> searchTasks(Object searchRequest) throws Exception {
        return getAtlasClient().searchTasks(searchRequest);
    }
}
