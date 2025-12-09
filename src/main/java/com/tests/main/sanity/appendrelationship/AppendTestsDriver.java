package com.tests.main.sanity.appendrelationship;

import com.tests.main.utils.ESUtil;

import static com.tests.main.utils.TestUtil.cleanUpAll;

public class AppendTestsDriver {
    public static void main(String[] args) throws Exception {
        try {
            new AppendAddAuditKafka().run();
            new AppendRemoveAuditKafka().run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }
}
