package com.tests.main.tools.createLoad;

import com.tests.main.client.AtlasClientV2;
import com.tests.main.client.ClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateBulkEntities {
    private static final Logger LOG = LoggerFactory.getLogger(CreateBulkEntities.class);
    private static AtlasClientV2 atlasClientV2 = ClientBuilder.build();

    private static int numWorkers = 2;
    private static int batch = 20;

    public static void main(String[] args) {
        Builder consumerBuilder = new Builder(atlasClientV2, batch);
        Manager creationManager = new Manager(consumerBuilder, batch, numWorkers, atlasClientV2);


        try {
            LOG.info("CreateBulkEntities Starting...");
            creationManager.produce();
            creationManager.drain();
        } catch (Exception ex) {
            LOG.error("CreateBulkEntities: Error: Current position:", ex);
        } finally {
            try {
                creationManager.shutdown();
            } catch (InterruptedException e) {
                LOG.error("Migration Import: Shutdown: Interrupted!", e);
            }
        }

        LOG.info("CreateBulkEntities: Done!");
    }
}
