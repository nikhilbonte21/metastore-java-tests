package com.tests.main.tools.createLoad;


import com.tests.main.client.AtlasClientV2;
import com.tests.main.pc.WorkItemBuilder;
import com.tests.main.pc.WorkItemManager;
import com.tests.main.utils.Utils;
import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.utils.TestUtils.*;


public class Manager extends WorkItemManager<AtlasEntity.AtlasEntitiesWithExtInfo, Creator> {
    private static final Logger LOG = LoggerFactory.getLogger(WorkItemManager.class);

    private int batchSize;

    public Manager(WorkItemBuilder builder, int batchSize, int numWorkers, AtlasClientV2 atlasClientV2) {
        super(builder, "workItemConsumer", batchSize, numWorkers, false);
        this.batchSize = batchSize;
    }

    public void produce(){
        AtlasEntity.AtlasEntitiesWithExtInfo entitiesWithExtInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();

        AtlasEntity tableEntity = getTableEntity();
        entitiesWithExtInfo.addEntity(tableEntity);

        for (int i = 2; i <= batchSize; i++) {
            entitiesWithExtInfo.addEntity(getColumnEntity(i, tableEntity.getGuid()));
        }

        produce(entitiesWithExtInfo);
    }


    private AtlasEntity getColumnEntity(int columnGuid, String tableGuid) {
        AtlasEntity entity = new AtlasEntity("Column");
        entity.setGuid("-" + columnGuid);

        String name = getRandomName();
        entity.setAttribute(NAME, name);
        entity.setAttribute(QUALIFIED_NAME, name);

        entity.setRelationshipAttribute("table", getObjectId(tableGuid, "Table"));

        return entity;
    }

    private AtlasEntity getTableEntity() {
        AtlasEntity entity = new AtlasEntity("Table");
        entity.setGuid("-1");

        String name = getRandomName();
        entity.setAttribute(NAME, name);
        entity.setAttribute(QUALIFIED_NAME, name);

        return entity;
    }
}
