package com.tests.main.sanity.repairindex;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.TestUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.tests.main.utils.Constants.ATTR_OWNER_USERS;
import static com.tests.main.utils.ESUtil.deleteESDocByGuid;
import static com.tests.main.utils.ESUtil.updateESDocByGuid;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntitiesBulk;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getESDoc;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.repairEntitiesByGuid;
import static com.tests.main.utils.TestUtil.repairEntityByGuid;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.verifyESAttributes;

public class RepairPartialVertex implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(RepairPartialVertex.class);

    private static long SLEEP = 2000;

    public static void main(String[] args) throws Exception {
        try {
            new RepairPartialVertex().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running column delete RepairFullVertex tests");

        repairFullVertex();

        repairPartialVertexMulti();
    }

    private void repairFullVertex() throws Exception {
        LOG.info(">> repairFullVertex");

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "test_table_repair" + getRandomName());
        table.setAttribute(ATTR_OWNER_USERS, Arrays.asList("value0", "value1"));
        String tableGuid = createEntity(table).getCreatedEntities().get(0).getGuid();
        sleep(SLEEP);

        Map<String, Object> attrs = getESDoc(tableGuid);
        Map<String, Object> attrsCopy = new HashMap<>(attrs);

        attrsCopy.remove(ATTR_OWNER_USERS);
        attrsCopy.put("__createdBy", "any random user");

        updateESDocByGuid(tableGuid, attrsCopy);
        sleep(SLEEP);

        repairEntityByGuid(tableGuid);
        sleep(SLEEP);

        verifyESAttributes(tableGuid, attrs);

        LOG.info("<< repairFullVertex");
    }

    private void repairPartialVertexMulti() throws Exception {
        LOG.info(">> repairPartialVertexMulti");

        int assetCount = 10;

        List<AtlasEntity> tables = new ArrayList<>(assetCount);
        for (int i = 0; i < assetCount ; i++) {
            tables.add(getAtlasEntity(TYPE_TABLE, "test_table_repair" + getRandomName()));
        }
        EntityMutationResponse response = createEntitiesBulk(tables);
        sleep(SLEEP);

        List<String> tableGuids = response.getCreatedEntities().stream().map(AtlasEntityHeader::getGuid).collect(Collectors.toList());

        List<Map<String, Object>> attrsList = tableGuids.stream().map(TestUtil::getESDoc).collect(Collectors.toList());
        List<Map<String, Object>> attrsCopy = attrsList.stream().map(HashMap::new).collect(Collectors.toList());

        for (Map<String, Object> attrCopy: attrsCopy) {
            attrCopy.remove(ATTR_OWNER_USERS);
            attrCopy.put("__createdBy", "any random user");

            updateESDocByGuid((String) attrCopy.get("__guid"), attrCopy);
        }
        sleep(SLEEP);

        repairEntitiesByGuid(tableGuids);
        sleep(SLEEP);

        for (Map<String, Object> attr: attrsList) {
            verifyESAttributes((String) attr.get("__guid"), attr);
        };

        LOG.info("<< repairPartialVertexMulti");
    }
}
