package com.tests.main.utils;

import com.tests.main.client.okhttp3.ConfigReader;

public class FeatureFlagManager {

    private static Boolean isTagsV2Enabled = null;

    public static boolean isTagsV2Enabled() {
        if (isTagsV2Enabled == null) {
            synchronized (FeatureFlagManager.class) {
                isTagsV2Enabled = ConfigReader.getBoolean("feature.flag.tag.v2.enabled");
            }
        }

        if (isTagsV2Enabled == null) {
            return false;
        }

        return isTagsV2Enabled.booleanValue();
    }
}
