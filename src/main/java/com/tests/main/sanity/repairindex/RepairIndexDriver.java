package com.tests.main.sanity.repairindex;

import com.tests.main.utils.ESUtil;

import static com.tests.main.utils.TestUtil.cleanUpAll;

public class RepairIndexDriver {
    public static void main(String[] args) throws Exception {
        try {
            new RepairFullVertex().run();
            new RepairPartialVertex().run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }
}
