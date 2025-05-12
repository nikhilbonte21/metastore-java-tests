package com.tests.main.tests.bugfixes;


import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.deleteEntityPurge;
import static com.tests.main.utils.TestUtil.getAtlasEntityExt;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;


public class GlossaryDeleteAfterMovingTerm implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryDeleteAfterMovingTerm.class);

    public static void main(String[] args) throws Exception {
        try {
            new GlossaryDeleteAfterMovingTerm().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running GlossaryDeleteAfterMovingTerm tests");

        long start = System.currentTimeMillis();
        try {
            deleteGlossaryAfterMovingTerm();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running GlossaryDeleteAfterMovingTerm tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }


    private static void deleteGlossaryAfterMovingTerm() throws Exception {
        LOG.info(">> deleteGlossaryAfterMovingTerm");
        AtlasEntity sourceGlo, targetGlo, term;

        AtlasEntity entity = getAtlasEntityExt("AtlasGlossary", getRandomName()).getEntity();
        String sourceGuid = createEntity(entity).getGuidAssignments().values().iterator().next();

        entity = getAtlasEntityExt("AtlasGlossary", getRandomName()).getEntity();
        String targetGuid = createEntity(entity).getGuidAssignments().values().iterator().next();
        sleep();

        sourceGlo = getEntity(sourceGuid);
        targetGlo = getEntity(targetGuid);

        String sourceGloQualifiedName = (String) sourceGlo.getAttribute(QUALIFIED_NAME);
        String targetGloQualifiedName = (String) targetGlo.getAttribute(QUALIFIED_NAME);

        // create Term
        entity = getTerm(sourceGuid, sourceGloQualifiedName);
        String termGuid = createEntity(entity).getGuidAssignments().values().iterator().next();
        sleep();

        term = getEntity(termGuid);
        String queryQname = (String) term.getAttribute(QUALIFIED_NAME);

        //move query
        AtlasEntity termToMove = getTerm(targetGuid, targetGloQualifiedName);
        termToMove.setGuid(termGuid);
        String targetTermQN = queryQname.replaceAll(sourceGloQualifiedName, targetGloQualifiedName);
        termToMove.setAttribute(QUALIFIED_NAME, targetTermQN);

        createEntity(termToMove);
        sleep();

        term = getEntity(termGuid);

        assertEquals((String) term.getAttribute(QUALIFIED_NAME), targetTermQN);
        //assertEquals((String) query.getAttribute("collectionQualifiedName"), targetCollQualifiedName);
        //assertEquals((String) query.getAttribute("parentQualifiedName"), targetCollQualifiedName);

        // delete source collection
        EntityMutationResponse response = deleteEntityPurge(sourceGuid);
        sleep();


        term = getEntity(termGuid);
        assertNull(term);

        LOG.info(">> deleteGlossaryAfterMovingTerm");
    }

    private static AtlasEntity getTerm(String gloGuid, String collQualifiedName) {
        AtlasEntity query = new AtlasEntity("AtlasGlossaryTerm");
        query.setAttribute(QUALIFIED_NAME, getRandomName());
        query.setAttribute("name", getRandomName());
        //query.setAttribute("collectionQualifiedName", collQualifiedName);
        //query.setAttribute("parentQualifiedName", collQualifiedName);

        query.setRelationshipAttribute("anchor", new AtlasObjectId(gloGuid, "AtlasGlossary"));

        return query;
    }
}