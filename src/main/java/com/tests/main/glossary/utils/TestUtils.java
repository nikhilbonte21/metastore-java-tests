package com.tests.main.glossary.utils;

import com.tests.main.glossary.client.ClientBuilder;
import com.tests.main.glossary.tests.TestsRunner;
import com.tests.main.glossary.tests.glossary.Glossary;
import com.tests.main.glossary.client.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import com.tests.main.glossary.models.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.relations.AtlasGlossaryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedCategoryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedTermHeader;
import org.apache.atlas.model.glossary.relations.AtlasTermCategorizationHeader;
import org.apache.atlas.model.instance.*;
import org.apache.atlas.model.lineage.AtlasLineageInfo;
import org.apache.atlas.type.AtlasTypeUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class TestUtils {
    private static final Logger LOG = LoggerFactory.getLogger(Glossary.class);
    public static Set<String> guidsToDelete = new HashSet<>();

    private static final String at = "@";
    private static final String dot = ".";
    public static String[] ATLAS_URLS = {"http://localhost:21000"};
    public static String[] CREDS = {"admin", "admin"};
    public static AtlasClientV2 atlasClient = null;

    public static final String TYPE_GLOSSARY = "AtlasGlossary";
    public static final String TYPE_CATEGORY = "AtlasGlossaryCategory";
    public static final String TYPE_TERM = "AtlasGlossaryTerm";
    public static final String TYPE_PROCESS = "Process";
    public static final String TYPE_COLUMN_PROCESS = "ColumnProcess";
    public static final String TYPE_TABLE = "Table";
    public static final String TYPE_COLUMN = "Column";

    public static final String PREFIX_QUERY_QN       = "/default/collection/admin";
    public static final String TYPE_QUERY            = "Query";
    public static final String TYPE_QUERY_FOLDER     = "QueryFolder";
    public static final String TYPE_QUERY_COLLECTION = "QueryCollection";
    public static final String PARENT                = "parent";
    public static final String CHILDREN_FOLDERS      = "childrenFolders";
    public static final String CHILDREN_QUERIES      = "childrenQueries";

    public static final String TERM_ANCHOR              = "AtlasGlossaryTermAnchor";
    public static final String CATEGORY_ANCHOR          = "AtlasGlossaryCategoryAnchor";
    public static final String PARENT_CATEGORY          = "parentCategory";
    public static final String ANCHOR                   = "anchor";
    public static final String CHILDREN_CATEGORY        = "childrenCategories";
    public static final String INPUTS_TO_P              = "inputToProcesses";
    public static final String OUTPUTS_FROM_P           = "outputFromProcesses";

    public static final String INPUTS            = "inputs";
    public static final String OUTPUTS           = "outputs";


    public static final String NAME = "name";
    public static final String QUALIFIED_NAME = "qualifiedName";


    static {
        setupClient();
    }

    private static void setupClient(){
        atlasClient = ClientBuilder.build();
        LOG.info("Client setup is successful!");
    }

    public static AtlasGlossary createGlossary(AtlasGlossary glossary) throws AtlasServiceException {
        AtlasGlossary glossary_0 = atlasClient.createGlossary(glossary);
        guidsToDelete.add(glossary_0.getGuid());
        return glossary_0;
    }


    public static String getAdminStatus() throws AtlasServiceException {
        return atlasClient.getAdminStatus();
    }

    public static AtlasGlossary getGlossary(String guid) throws AtlasServiceException {
        return atlasClient.getGlossaryByGuid(guid);
    }

    public static List<AtlasGlossary> getGlossaries() throws AtlasServiceException {
        return atlasClient.getAllGlossaries("ASC", 9999, 0);
    }

    public static void deleteEntities(List<String> guids) throws AtlasServiceException {
        atlasClient.deleteEntitiesByGuids(guids);
    }

    public static AtlasGlossary updateGlossary(String guid, AtlasGlossary glossary) throws AtlasServiceException {
        return atlasClient.updateGlossaryByGuid(guid, glossary);
    }

    public static AtlasGlossaryTerm getTerm(String guid) throws AtlasServiceException {
        return atlasClient.getGlossaryTerm(guid);
    }

    public static AtlasGlossaryTerm createTerm(AtlasGlossaryTerm glossary) throws AtlasServiceException {
        return atlasClient.createGlossaryTerm(glossary);
    }

    public static AtlasGlossaryTerm updateTerm(String termGuid, AtlasGlossaryTerm term) throws AtlasServiceException {
        return atlasClient.updateGlossaryTermByGuid(termGuid, term);
    }

    public static AtlasGlossaryTerm updateTermPartial(String guid, Map<String, String> attributes) throws AtlasServiceException {
        return atlasClient.partialUpdateTermByGuid(guid, attributes);
    }

    public static AtlasGlossaryCategory createCategory(AtlasGlossaryCategory category) throws AtlasServiceException {
        return atlasClient.createGlossaryCategory(category);
    }

    public static AtlasGlossaryCategory getCategory(String guid) throws AtlasServiceException {
        return atlasClient.getGlossaryCategory(guid);
    }

    public static AtlasGlossaryCategory updateCategory(String categoryGuid, AtlasGlossaryCategory category) throws AtlasServiceException {
        return atlasClient.updateGlossaryCategoryByGuid(categoryGuid, category);
    }

    public static AtlasRelationship createRelationship(AtlasRelationship relationship) throws AtlasServiceException {
        return atlasClient.createRelationship(relationship);
    }


    public static EntityMutationResponse createEntity(AtlasEntity entity) throws AtlasServiceException {
        return createEntity(new AtlasEntity.AtlasEntityWithExtInfo(entity));
    }

    public static EntityMutationResponse createEntity(AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo) throws AtlasServiceException {
        EntityMutationResponse response = atlasClient.createEntity(entityWithExtInfo);
        if (CollectionUtils.isNotEmpty(response.getCreatedEntities())) {
            guidsToDelete.add(response.getCreatedEntities().get(0).getGuid());
        }
        return response;
    }

    public static EntityMutationResponse createEntitiesBulk(AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo) throws AtlasServiceException {
        return atlasClient.createEntities(entitiesWithExtInfo);
    }

    public static AtlasEntity getEntity(String guid) throws AtlasServiceException {
        return atlasClient.getEntityByGuid(guid).getEntity();
    }

    public static AtlasLineageInfo getLineageInfo(String guid, AtlasLineageInfo.LineageDirection direction, int depth, boolean hideProcess) throws AtlasServiceException {
        return atlasClient.getLineageInfo(guid, direction, depth, hideProcess);
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
        return RandomStringUtils.randomAlphanumeric(8);
    }

    public static void cleanUpAll() throws AtlasServiceException {
        Glossary.deleteAllEntities();
    }

    public static void cleanUpAll(boolean all) throws AtlasServiceException {
        Glossary.deleteAllEntities();
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

    public static void deleteAllEntities() throws AtlasServiceException {
        if (CollectionUtils.isNotEmpty(guidsToDelete)) {
            LOG.info("Deleting {} glossaries", guidsToDelete.size());
            TestUtils.deleteEntities(new ArrayList<>(guidsToDelete));
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

        return getEntity(TestUtils.createEntity(entityWithExtInfo).getCreatedEntities().get(0).getGuid());
    }

    public static AtlasEntity createCustomEntity(String typeName, String entityName) throws Exception {

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = getAtlasEntity(typeName, entityName);

        return getEntity(TestUtils.createEntity(entityWithExtInfo).getCreatedEntities().get(0).getGuid());

    }

    public static AtlasEntity.AtlasEntityWithExtInfo getAtlasEntityQuery(String entityName,
                                                                         String collectionQualifiedName) {
        AtlasEntity entity = new AtlasEntity(TYPE_QUERY);
        entityName = StringUtils.isNotEmpty(entityName) ? entityName : getRandomName();
        entity.setAttribute(NAME, entityName);
        entity.setAttribute(QUALIFIED_NAME, entityName + "_" + getRandomName());
        entity.setAttribute("rawQuery", "select * from " + getRandomName());
        entity.setAttribute("collectionQualifiedName", collectionQualifiedName);

        return new AtlasEntity.AtlasEntityWithExtInfo(entity);
    }

    public static AtlasEntity.AtlasEntityWithExtInfo getAtlasEntityQueryFolder(String entityName,
                                                                         String collectionQualifiedName) {
        AtlasEntity entity = new AtlasEntity(TYPE_QUERY_FOLDER);
        entityName = StringUtils.isNotEmpty(entityName) ? entityName : getRandomName();
        entity.setAttribute(NAME, entityName);
        entity.setAttribute(QUALIFIED_NAME, entityName + "_" + getRandomName());
        entity.setAttribute("collectionQualifiedName", collectionQualifiedName);

        return new AtlasEntity.AtlasEntityWithExtInfo(entity);
    }

    public static AtlasEntity.AtlasEntityWithExtInfo getAtlasEntity(String typeName, String entityName) {
        AtlasEntity entity = new AtlasEntity(typeName);
        entityName = StringUtils.isNotEmpty(entityName) ? entityName : getRandomName();
        entity.setAttribute(NAME, entityName);
        entity.setAttribute(QUALIFIED_NAME, entityName + "_" + getRandomName());

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

        return getEntity(TestUtils.createEntity(entityWithExtInfo).getCreatedEntities().get(0).getGuid());
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

        AtlasEntity ret = getEntity(TestUtils.createEntity(entityWithExtInfo).getCreatedEntities().get(0).getGuid());
        TestsRunner.guidsToDelete.add(ret.getGuid());
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

    public static void verifyESGlossary(String catGuid, String expectedGloQName) {

        SearchHit[] searchHit = ESUtils.searchWithGuid(catGuid).getHits().getHits();
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

        SearchHit[] searchHit = ESUtils.searchWithGuid(entityGuid).getHits().getHits();
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
}
