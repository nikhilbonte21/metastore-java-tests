package com.tests.main.tests.connection;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.TestUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.setAtlasClient;
import static com.tests.main.utils.TestUtil.sleep;
import static org.junit.Assert.*;


public class CreateUpdateConnection implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(CreateUpdateConnection.class);

    private static AtlasEntity CONNECTION_0;

    private static String ADMIN_USERS = "adminUsers";
    private static String ADMIN_GROUPS = "adminGroups";
    private static String ADMIN_ROLES = "adminRoles";

    public static void main(String[] args) throws Exception {
        try {
            new CreateUpdateConnection().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running CreateUpdateConnection tests");

        long start = System.currentTimeMillis();
        try {
            setAtlasClient(TestUtil.getClientLocal("service-account-atlan-argo"));

            createAConnection();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running CreateUpdateConnection tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void createAConnection() throws Exception {
        LOG.info(">> createAConnection");

        CONNECTION_0 = new AtlasEntity("Connection");
        CONNECTION_0.setAttribute(NAME, "connection_0");
        CONNECTION_0.setAttribute(QUALIFIED_NAME, "default/snowflake/123456789");

        String guid = createEntity(CONNECTION_0).getCreatedEntities().get(0).getGuid();
        sleep(30);

        CONNECTION_0 = getEntity(guid);

        assertEquals("service-account-atlan-argo", CONNECTION_0.getAttribute(ADMIN_USERS));
        assertTrue(CollectionUtils.isEmpty((Collection) CONNECTION_0.getAttribute(ADMIN_GROUPS)));
        assertTrue(CollectionUtils.isEmpty((Collection) CONNECTION_0.getAttribute(ADMIN_ROLES)));

        LOG.info(">> createAConnection");
    }


}