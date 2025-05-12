package com.tests.main.tests.tags;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createClassificationDefs;
import static com.tests.main.utils.TestUtil.createEntitiesWithTag;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static org.junit.Assert.*;


public class AttachTag implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(AttachTag.class);

    static List<String> TAGS = new ArrayList<>();

    public static void main(String[] args) throws Exception {
        try {
            new AttachTag().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running AttachTag tests");

        long start = System.currentTimeMillis();
        try {
            TAGS.add("AwLkvzGvWqLHESZZe99EsX");
            TAGS.add("M4qbgbrFMdPYkKlgxXaf7J");
            TAGS.add("uBcKSDe9cPveOufMXXNrIR");
            //createTags();

            passBothQueryParams();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running AttachTag tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void createTags() throws Exception {
        LOG.info(">> createTags");

        List<AtlasClassificationDef> tagDefs = new ArrayList<>();

        for (int i =0; i <2; i++) {
            AtlasClassificationDef def = new AtlasClassificationDef();
            def.setName("tag_" + i);
            def.setDisplayName("tag_" + i);

            tagDefs.add(def);
        }

        tagDefs = createClassificationDefs(tagDefs);
        tagDefs.forEach(x -> TAGS.add(x.getName()));

        System.out.println(TAGS.toArray());

        LOG.info(">> createTags");
    }

    private static void passBothQueryParams() throws Exception {
        LOG.info(">> passBothQueryParams");

        AtlasEntity table_0 = getAtlasEntity(TYPE_TABLE, "table_0");

        createEntitiesWithTag(table_0, true, true);

        LOG.info(">> passBothQueryParams");
    }

}