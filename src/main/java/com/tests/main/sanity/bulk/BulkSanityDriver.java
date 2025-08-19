package com.tests.main.sanity.bulk;

import com.tests.main.utils.ESUtil;

import static com.tests.main.utils.TestUtil.cleanUpAll;

public class BulkSanityDriver {
    public static void main(String[] args) throws Exception {
        try {
            new SanityAttributesMutations().run();
            new SanityBasicTypeAttributesMutations().run();
            new SanityComplexTypeAttributesMutations().run();
            new SanityMapTypeAttributesMutations().run();
            new SanityArrayTypeAttributesMutations().run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            cleanUpAll();
            ESUtil.close();
        }
    }
}
