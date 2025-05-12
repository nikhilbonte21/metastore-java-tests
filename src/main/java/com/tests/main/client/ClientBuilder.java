package com.tests.main.client;


import com.tests.main.client.okhttp3.ConfigReader;
import com.tests.main.client.okhttp3.OKClient;
import com.tests.main.utils.TestUtil;
import okhttp3.OkHttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.utils.TestUtil.CONNECTION_PREFIX;

public class ClientBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(ClientBuilder.class);
    public static final String ATLAS_CONFIGURATION_DIRECTORY_PROPERTY = "atlas.conf";
    public static final String  APPLICATION_PROPERTIES          = "atlas-application.properties";

    public static void build() {
        try {
            String mode = ConfigReader.getString("atlas.client.mode", "beta");
            LOG.info("mode : {}", mode);

            TestUtil.globalClient = getClient();

        } catch (Exception e) {
            LOG.info("Failed to initialize client");
            e.printStackTrace();
        }
    }

    public static OkHttpClient buildLocalClientWithCreds(String... creds) {
        return null;
    }

    private static OKClient getClient() throws Exception {
        CONNECTION_PREFIX = ConfigReader.getString("connection.name.prefix", "");

        LOG.info("CONNECTION_PREFIX: {}", CONNECTION_PREFIX);
        return new OKClient();
    }
}
