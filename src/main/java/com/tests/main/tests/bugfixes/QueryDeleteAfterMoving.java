package com.tests.main.tests.bugfixes;


import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.tests.main.utils.TestUtil.PARENT;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.TYPE_COLUMN;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.deleteEntities;
import static com.tests.main.utils.TestUtil.deleteEntityHard;
import static com.tests.main.utils.TestUtil.deleteEntityPurge;
import static com.tests.main.utils.TestUtil.getAtlasEntityExt;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.getUUID;
import static com.tests.main.utils.TestUtil.sleep;
import static org.junit.Assert.*;


public class QueryDeleteAfterMoving implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(QueryDeleteAfterMoving.class);

    public static void main(String[] args) throws Exception {
        try {
            new QueryDeleteAfterMoving().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running QueryDeleteAfterMoving tests");

        long start = System.currentTimeMillis();
        try {
            deleteCollectionAfterMovingQuery();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running QueryDeleteAfterMoving tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    public static final String USER   = "nikhil.bonte";

    public static final String PREFIX_QUERY_QN   = "default/collection/";
    private static final String collectionQualifiedNameFormat = PREFIX_QUERY_QN + "%s/%s";
    private static final String queryQualifiedNameFormat = "%s/query/%s/%s";

    private static void deleteCollectionAfterMovingQuery() throws Exception {
        LOG.info(">> deleteCollectionAfterMovingQuery");
        AtlasEntity sourceColl, targetColl, query;

        AtlasEntity entity = getAtlasEntityExt("Collection", getRandomName()).getEntity();
        entity.setAttribute(QUALIFIED_NAME, getCollectionQualifiedName());
        String sourceGuid = createEntity(entity).getGuidAssignments().values().iterator().next();

        entity = getAtlasEntityExt("Collection", getRandomName()).getEntity();
        entity.setAttribute(QUALIFIED_NAME, getCollectionQualifiedName());
        String targetGuid = createEntity(entity).getGuidAssignments().values().iterator().next();
        sleep();

        sourceColl = getEntity(sourceGuid);
        targetColl = getEntity(targetGuid);

        String sourceCollQualifiedName = (String) sourceColl.getAttribute(QUALIFIED_NAME);
        String targetCollQualifiedName = (String) targetColl.getAttribute(QUALIFIED_NAME);

        // create Query
        entity = getQuery(sourceGuid, sourceCollQualifiedName);
        String queryGuid = createEntity(entity).getGuidAssignments().values().iterator().next();
        sleep();

        query = getEntity(queryGuid);
        String queryQname = (String) query.getAttribute(QUALIFIED_NAME);

        //move query
        AtlasEntity queryToMove = getQuery(targetGuid, targetCollQualifiedName);
        queryToMove.setGuid(queryGuid);
        String targetQueryQN = queryQname.replaceAll(sourceCollQualifiedName, targetCollQualifiedName);
        queryToMove.setAttribute(QUALIFIED_NAME, targetQueryQN);

        createEntity(queryToMove);
        sleep();

        query = getEntity(queryGuid);

        assertEquals((String) query.getAttribute(QUALIFIED_NAME), targetQueryQN);
        assertEquals((String) query.getAttribute("collectionQualifiedName"), targetCollQualifiedName);
        assertEquals((String) query.getAttribute("parentQualifiedName"), targetCollQualifiedName);

        // delete source collection
        EntityMutationResponse response = deleteEntityHard(sourceGuid);
        sleep();

        query = getEntity(queryGuid);
        assertNotNull(query);
        assertEquals("ACTIVE", query.getStatus().toString());

        LOG.info(">> deleteCollectionAfterMovingQuery");
    }

    private static String getCollectionQualifiedName() {
        return String.format(collectionQualifiedNameFormat, USER, getUUID());

    }

    private static String getQueryQualifiedName(String collectionQualifiedName) {
        return String.format(queryQualifiedNameFormat, collectionQualifiedName, USER, getUUID());
    }

    private static AtlasEntity getQuery(String collGuid, String collQualifiedName) {
        AtlasEntity query = new AtlasEntity("Query");
        query.setAttribute(QUALIFIED_NAME, getQueryQualifiedName(collQualifiedName));
        query.setAttribute("name", getRandomName());
        query.setAttribute("collectionQualifiedName", collQualifiedName);
        query.setAttribute("parentQualifiedName", collQualifiedName);

        query.setRelationshipAttribute("parent", new AtlasObjectId(collGuid, "Collection"));

        return query;
    }
}