package com.tests.main.glossary.client;

import org.apache.atlas.ApplicationProperties;
import org.apache.atlas.AtlasException;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

public class ClientBuilder extends PropertiesConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(ClientBuilder.class);
    private static volatile Configuration instance = null;
    public static final String ATLAS_CONFIGURATION_DIRECTORY_PROPERTY = "atlas.conf";
    public static final String  APPLICATION_PROPERTIES          = "atlas-application.properties";

    public static AtlasClientV2 build() {
        AtlasClientV2 atlasClient = null;
        try {
            String mode = getProperties().getString("atlas.client.mode", "beta");
            LOG.info("mode : {}", mode);

            switch (mode) {
                case "beta":
                    atlasClient = buildBetaClient();
                    break;
                case "local":
                default:
                    atlasClient = buildLocalClient();
            }

        } catch (AtlasException e) {
            LOG.info("Failed to initialize client");
            e.printStackTrace();
        }
        return atlasClient;
    }

    private static AtlasClientV2 buildLocalClient() throws AtlasException {
        String[] CREDS = {"admin", "admin" };
        return new AtlasClientV2(getProperties().getStringArray("local.client.rest.address"),
                CREDS);
    }

    public static AtlasClientV2 buildCustomClientLocal(String username, String password) throws AtlasException {
        String[] CREDS = {username, password};
        return new AtlasClientV2(getProperties().getStringArray("local.client.rest.address"),
                CREDS);
    }

    private static AtlasClientV2 buildBetaClient() throws AtlasException {
        return new AtlasClientV2(getProperties().getStringArray("beta.client.rest.address"),
                getProperties().getStringArray("local.client.creds"));
    }

    public static Configuration getProperties() throws AtlasException {
        if (instance == null) {
            synchronized (ClientBuilder.class) {
                if (instance == null) {
                    set(get(APPLICATION_PROPERTIES));
                }
            }
        }

        return instance;
    }


    public static Configuration set(Configuration configuration) throws AtlasException {
        synchronized (ApplicationProperties.class) {
            instance = configuration;
        }
        return instance;
    }

    private ClientBuilder(URL url) throws ConfigurationException {
        super(url);
    }

    public static Configuration get(String fileName) throws AtlasException {
        String confLocation = System.getProperty(ATLAS_CONFIGURATION_DIRECTORY_PROPERTY);
        try {
            URL url = null;

            if (confLocation == null) {
                LOG.info("Looking for {} in classpath", fileName);

                url = ApplicationProperties.class.getClassLoader().getResource(fileName);

                if (url == null) {
                    LOG.info("Looking for /{} in classpath", fileName);

                    url = ApplicationProperties.class.getClassLoader().getResource("/" + fileName);
                }
            } else {
                url = new File(confLocation, fileName).toURI().toURL();
            }

            LOG.info("Loading {} from {}", fileName, url);
            ClientBuilder appProperties = new ClientBuilder(url);
            Configuration configuration = appProperties.interpolatedConfiguration();

            return configuration;
        } catch (Exception e) {
            throw new AtlasException("Failed to load application properties", e);
        }
    }
}
