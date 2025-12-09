package com.tests.main.sanity.tag.propagation;

import com.tests.main.Test;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

import static com.tests.main.sanity.tag.propagation.PropagationUtils.getTagTypeDefs;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.verifyEntityHasTags;
import static com.tests.main.sanity.tag.propagation.PropagationUtils.verifyEntityNotHaveTags;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.addTagByTypeAPI;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.deleteTagByTypeAPI;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getQualifiedName;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.sleep;


public class DirectTagsViaTypeAPIs implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(DirectTagsViaTypeAPIs.class);

    private static long SLEEP = 2000;

    private static List<String> tagTypeNames;

    static {
        tagTypeNames = getTagTypeDefs(1);
    }

    public static void main(String[] args) throws Exception {
        try {
            new DirectTagsViaTypeAPIs().run();
            //TestRunner.runTests(Propagation.class);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running DirectTagsViaTypeAPIs tests");

        long start = System.currentTimeMillis();
        try {
            attachTag();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running DirectTagsViaTypeAPIs tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    @Test
    public void attachTag() throws Exception {
        LOG.info(">> attachTag");

        // Create a test table with tags
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        String tableQualifiedName = getQualifiedName(table);

        List<AtlasClassification> tagsList = tagTypeNames.stream().map(x -> new AtlasClassification(x)).collect(Collectors.toList());

        addTagByTypeAPI(TYPE_TABLE, tableQualifiedName, tagsList);

        sleep(SLEEP);

        verifyEntityHasTags(tableGuid, tagTypeNames);

        // Delete tags
        for (String tagTypeName: tagTypeNames) {
            deleteTagByTypeAPI(TYPE_TABLE, tableQualifiedName, tagTypeName);
        }

        verifyEntityNotHaveTags(tableGuid, tagTypeNames);

        LOG.info("<< attachTag");
    }
}
