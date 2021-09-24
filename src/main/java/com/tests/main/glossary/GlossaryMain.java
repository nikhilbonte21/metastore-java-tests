package com.tests.main.glossary;

import com.tests.main.glossary.tests.category.Category;
import com.tests.main.glossary.tests.glossary.Glossary;
import org.apache.atlas.AtlasClientV2;
import org.apache.atlas.AtlasServiceException;
import org.checkerframework.checker.units.qual.C;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlossaryMain {
    private static final Logger LOG = LoggerFactory.getLogger(GlossaryMain.class);

    public static String[] ATLAS_URLS = {"http://localhost:21000"};
    public static String[] CREDS = {"admin", "admin"};
    public static AtlasClientV2 atlasClient = null;

    public static void main(String[] args) throws AtlasServiceException {
        setupClient();
        startTests();
    }

    private static void setupClient(){
        atlasClient = new AtlasClientV2(ATLAS_URLS, CREDS);
        LOG.info("Client setup is successful!");
    }

    private static void startTests() throws AtlasServiceException {
        LOG.info("Running tests now");
        Glossary glossary = new Glossary();
        //glossary.runTests();

        Category category = new Category();
        category.runTests();

    }
}
