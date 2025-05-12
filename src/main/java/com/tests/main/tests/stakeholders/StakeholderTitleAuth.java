package com.tests.main.tests.stakeholders;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.utils.TestUtil.cleanUpAll;


public class StakeholderTitleAuth implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(StakeholderTitleAuth.class);

    public static void main(String[] args) throws Exception {
        try {
            new StakeholderTitleAuth().run();
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running StakeholderAuth tests");

        long start = System.currentTimeMillis();
        try {


        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running StakeholderAuth tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }


}