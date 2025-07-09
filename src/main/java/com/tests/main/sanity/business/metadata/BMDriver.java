package com.tests.main.sanity.business.metadata;

import com.tests.main.utils.ESUtil;

import static com.tests.main.utils.TestUtil.cleanUpAll;

public class BMDriver {
    public static void main(String[] args) throws Exception {
        try {
            new AddOrUpdateBMsBulk().run();
            new AddOrUpdateSpecificBM().run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }
}
