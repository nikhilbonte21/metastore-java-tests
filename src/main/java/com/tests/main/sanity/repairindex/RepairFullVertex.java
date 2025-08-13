package com.tests.main.sanity.repairindex;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.atlas.model.typedef.AtlasClassificationDef;
import org.apache.atlas.model.typedef.AtlasTypesDef;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.tests.main.utils.ESUtil.deleteESDocByGuid;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getESDoc;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.getTagDefs;
import static com.tests.main.utils.TestUtil.repairEntitiesByGuid;
import static com.tests.main.utils.TestUtil.repairEntityByGuid;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.verifyES;
import static com.tests.main.utils.TestUtil.verifyESAttributes;
import static org.junit.Assert.assertTrue;

public class RepairFullVertex implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(RepairFullVertex.class);

    private static long SLEEP = 5000;

    public static void main(String[] args) throws Exception {
        try {
            new RepairFullVertex().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running column delete RepairFullVertex tests");

        //repairFullVertex();

        repairFullVertexWithTags();

        //repairFullVertexMulti();
    }

    private void repairFullVertex() throws Exception {
        LOG.info(">> repairFullVertex");

        // Create a test table
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_repair" + getRandomName());
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        Map<String, Object> attrs = getESDoc(tableGuid);

        deleteESDocByGuid(tableGuid);

        repairEntityByGuid(tableGuid);
        sleep(SLEEP);

        verifyESAttributes(tableGuid, attrs);

        LOG.info("<< repairFullVertex");
    }

    private void repairFullVertexWithTags() throws Exception {
        LOG.info(">> repairFullVertexWithTags");

        List<String> tagTypeNames = new ArrayList<>();

        AtlasTypesDef typesDef = getTagDefs();

        assertTrue("Sufficient tags not found", typesDef != null
                && CollectionUtils.isNotEmpty(typesDef.getClassificationDefs())
                && typesDef.getClassificationDefs().size() >= 2);

        tagTypeNames.add(typesDef.getClassificationDefs().get(0).getName());
        tagTypeNames.add(typesDef.getClassificationDefs().get(1).getName());

        // Create a test table with tags
        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_repair" + getRandomName());
        table.setClassifications(Arrays.asList(
                new AtlasClassification(tagTypeNames.get(0)),
                new AtlasClassification(tagTypeNames.get(1))
        ));
        String tableGuid = createEntitiesBulk(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        Map<String, Object> attrs = getESDoc(tableGuid);
        assertTrue(((List<String>) attrs.get("__traitNames")).containsAll(tagTypeNames));
        attrs.remove("__classificationNames"); // Ignoring as order is not consistent always
        attrs.remove("__classificationsText"); // Ignoring as order is not consistent always

        deleteESDocByGuid(tableGuid);

        repairEntityByGuid(tableGuid);
        sleep(SLEEP);

        Map<String, Object> attrsReIndexed = verifyESAttributes(tableGuid, attrs);
        assertTrue(((String) attrsReIndexed.get("__classificationNames")).contains(tagTypeNames.get(0)));
        assertTrue(((String) attrsReIndexed.get("__classificationNames")).contains(tagTypeNames.get(1)));

        assertTrue(((String) attrsReIndexed.get("__classificationsText")).contains(tagTypeNames.get(0)));
        assertTrue(((String) attrsReIndexed.get("__classificationsText")).contains(tagTypeNames.get(1)));

        LOG.info("<< repairFullVertexWithTags");
    }

    private void repairFullVertexMulti() throws Exception {
        LOG.info(">> repairFullVertexMulti");

        int assetCount = 10;

        List<AtlasEntity> tables = new ArrayList<>(assetCount);

        for (int i = 0; i < assetCount ; i++) {
            tables.add(getAtlasEntity(TYPE_TABLE, "test_table_repair" + getRandomName()));
        }
        EntityMutationResponse response = createEntitiesBulk(tables);
        sleep(SLEEP);

        List<String> tableGuids = response.getCreatedEntities().stream().map(x -> x.getGuid()).collect(Collectors.toList());

        List<Map<String, Object>> attrsList = tableGuids.stream().map(x -> getESDoc(x)).collect(Collectors.toList());

        deleteESDocByGuid(tableGuids.toArray(new String[0]));
        sleep(SLEEP);

        repairEntitiesByGuid(tableGuids);
        sleep(SLEEP);

        for (Map<String, Object> attr: attrsList) {
            verifyESAttributes((String) attr.get("__guid"), attr);
        };

        LOG.info("<< repairFullVertexMulti");
    }
}
