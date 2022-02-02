package com.tests.main.tools.createLoad;

import com.tests.main.client.AtlasClientV2;
import com.tests.main.client.AtlasServiceException;
import com.tests.main.pc.WorkItemConsumer;
import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class Creator extends WorkItemConsumer<AtlasEntity.AtlasEntitiesWithExtInfo> {
    private static final Logger LOG = LoggerFactory.getLogger(WorkItemConsumer.class);

    private AtlasClientV2 atlasClientV2;
    private final int batchSize;


    public Creator(AtlasClientV2 clientV2, BlockingQueue queue, int batchSize) {
        super(queue);
        this.atlasClientV2 = clientV2;
        this.batchSize = batchSize;
    }

    @Override
    protected void processItem(AtlasEntity.AtlasEntitiesWithExtInfo item) {
        try {
            atlasClientV2.createEntities(item);
        } catch (AtlasServiceException e) {
            LOG.info("Invalid entities: Please correct and re-submit!", e);
        }
    }
}
