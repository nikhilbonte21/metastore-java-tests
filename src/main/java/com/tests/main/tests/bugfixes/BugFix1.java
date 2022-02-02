package com.tests.main.tests.bugfixes;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtils;
import org.apache.atlas.AtlasException;
import com.tests.main.client.AtlasServiceException;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.Classification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

import static com.tests.main.utils.TestUtils.*;
import static org.junit.Assert.*;


public class BugFix1 { //implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(BugFix1.class);

    public static void main(String[] args) throws Exception {
        try {
            new BugFix1().run();
        } finally {
            cleanUpAll();
            ESUtils.close();
        }
    }

    //@Override
    public void run() throws Exception {
        LOG.info("Running BugFix1 tests");

        long start = System.currentTimeMillis();
        try {
            meta_2820(); // Delete entity fails due to stale propagated classification

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running BugFix1 tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void meta_2820() throws Exception {
        LOG.info(">> meta_2820");

        String tag = createClassification("tag_0" + getRandomName());

        AtlasEntity input_column = getAtlasEntity(TYPE_COLUMN, "input_column").getEntity();
        AtlasClassification classification = new AtlasClassification(tag);

        input_column.addClassifications(Collections.singletonList(classification));

        AtlasEntity output_column = getAtlasEntity(TYPE_COLUMN, "output_column").getEntity();
        AtlasEntity column_process = getAtlasEntity(TYPE_COLUMN_PROCESS, "column_process").getEntity();

        column_process.setRelationshipAttribute("inputs", getObjectIdsAsList(TYPE_COLUMN, input_column.getGuid()));
        column_process.setRelationshipAttribute("outputs", getObjectIdsAsList(TYPE_COLUMN, output_column.getGuid()));

        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        entitiesWithExtInfo.addEntity(input_column);
        entitiesWithExtInfo.addEntity(output_column);
        entitiesWithExtInfo.addEntity(column_process);

        EntityMutationResponse response = createEntitiesBulk(entitiesWithExtInfo);

        String input = response.getGuidAssignments().get(input_column.getGuid());
        String output = response.getGuidAssignments().get(output_column.getGuid());
        String process = response.getGuidAssignments().get(column_process.getGuid());

        Thread.sleep(2000);
        input_column = getEntity(input);
        output_column = getEntity(output);
        column_process = getEntity(process);

        assertEquals(input_column.getClassifications().size(), 1);
        assertEquals(output_column.getClassifications().size(), 1);
        assertEquals(column_process.getClassifications().size(), 1);

        assertEquals(input_column.getClassifications().get(0).getEntityGuid(), input_column.getGuid());
        assertEquals(output_column.getClassifications().get(0).getEntityGuid(), input_column.getGuid());
        assertEquals(column_process.getClassifications().get(0).getEntityGuid(), input_column.getGuid());

        response = deleteEntityHard(input_column.getGuid());

        assertEquals(response.getDeletedEntities().get(0).getDeleteHandler().toLowerCase(), "hard");
        LOG.info(">> meta_2820");
    }

}