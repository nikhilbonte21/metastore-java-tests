package com.tests.main.tools.createLoad;

import com.tests.main.client.AtlasClientV2;
import com.tests.main.pc.WorkItemBuilder;
import org.apache.atlas.model.instance.AtlasEntity;

import java.util.concurrent.BlockingQueue;

public class Builder implements WorkItemBuilder<Creator, AtlasEntity.AtlasEntitiesWithExtInfo> {
    private AtlasClientV2 atlasClientV2;
    private int batch;

    public Builder(AtlasClientV2 atlasClientV2, int batch) {
        this.atlasClientV2 = atlasClientV2;
        this.batch = batch;
    }

    @Override
    public Creator build(BlockingQueue<AtlasEntity.AtlasEntitiesWithExtInfo> queue) {
        return new Creator(atlasClientV2, queue, batch);
    }
}
