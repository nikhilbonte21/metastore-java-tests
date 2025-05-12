package com.tests.main.tests.authz;

import com.tests.main.tests.authz.club.OverrideAllowABAC;
import com.tests.main.tests.authz.club.OverrideAllowUsual;
import com.tests.main.tests.authz.club.OverrideDenyABAC;
import com.tests.main.tests.authz.club.OverrideDenyUsual;
import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.TestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.setAtlasClient;
import static org.junit.Assert.*;


public class AuthzClub implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(AuthzClub.class);

    public static void main(String[] args) throws Exception {
        try {
            new AuthzClub().run();
        } finally {
            setAtlasClient(TestUtil.getClientLocal("service-account-atlan-argo"));
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running AuthzClub tests");

        long start = System.currentTimeMillis();
        try {
            new AuthzOnlyABAC().run();
            new AuthzUsual().run();

            new OverrideDenyUsual().run();
            new OverrideDenyABAC().run();
            new OverrideAllowUsual().run();
            new OverrideAllowABAC().run();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running AuthzClub tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }


}