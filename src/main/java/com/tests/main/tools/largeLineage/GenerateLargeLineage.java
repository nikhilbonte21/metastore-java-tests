package com.tests.main.tools.largeLineage;

import com.tests.main.utils.Utils;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.utils.TestUtil.*;

public class GenerateLargeLineage {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateLargeLineage.class);

    private static final String QN_PREFIX = "default/snowflake/1640633578/";

    private static int count = 100;
    // total Process = count
    // total Tables = count + 1
    // total entities = count + count + 1
    private static int processBatchSize = 21;

    public static void main(String[] args) throws Exception {

        try {
            createLinearLineage();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cleanUpAll();
        }
    }

    private static void createLinearLineage() throws Exception {
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        EntityMutationResponse response = null;
        AtlasEntity last = null;
        int createdCount = 0;
        int watch = 0;
        String startGuid = null;
        String endGuid = null;

        for (int j = 0; j < count; j++) {

            AtlasEntity output = getTableEntity();

            if (last == null) {
                last = getTableEntity();
                entitiesWithExtInfo.addEntity(last);
            }

            entitiesWithExtInfo.addEntity(output);

            AtlasEntity process = getProcessEntity(last, output);
            entitiesWithExtInfo.addEntity(process);

            last = output;

            watch++;

            LOG.info("entitiesWithExtInfo : {}", Utils.toJson(entitiesWithExtInfo));
            if (watch % processBatchSize == 0) {
                response = createEntitiesBulk(entitiesWithExtInfo);
                LOG.info("created {} entities", createdCount += response.getCreatedEntities().size());

                if (watch == processBatchSize) {
                    startGuid = response.getCreatedEntities().get(0).getGuid();
                    LOG.info("start guid {}", startGuid);

                    endGuid = response.getCreatedEntities().get(response.getCreatedEntities().size()-1).getGuid();
                }

            }
        }

        if (CollectionUtils.isNotEmpty(entitiesWithExtInfo.getEntities())) {
            response = createEntitiesBulk(entitiesWithExtInfo);
            LOG.info("created {} entities", createdCount + response.getCreatedEntities().size());
            endGuid = response.getCreatedEntities().get(response.getCreatedEntities().size()-1).getGuid();
        }
        LOG.info("start guid {}", startGuid);
        LOG.info("end guid {}", endGuid);

    }

    private static AtlasEntity getProcessEntity(AtlasEntity input, AtlasEntity output) {
        AtlasEntity entity = new AtlasEntity(TYPE_PROCESS);

        String name = getRandomName();
        entity.setAttribute(NAME, name);
        entity.setAttribute(QUALIFIED_NAME, QN_PREFIX + name);

        if (input != null) {
            entity.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, input.getGuid()));
        }

        if (output != null) {
            entity.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, output.getGuid()));
        }

        return entity;
    }

    private static AtlasEntity getTableEntity() {
        AtlasEntity entity = new AtlasEntity(TYPE_TABLE);

        String name = getRandomName();
        entity.setAttribute(NAME, name);
        entity.setAttribute(QUALIFIED_NAME, QN_PREFIX + name);

        return entity;
    }
}
