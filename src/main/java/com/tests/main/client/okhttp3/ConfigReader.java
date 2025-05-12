package com.tests.main.client.okhttp3;

import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class ConfigReader {
    private static Properties properties = new Properties();

    static {
        try (InputStream fis = ConfigReader.class.getClassLoader().getResourceAsStream("atlas-application.properties");) {
            properties.load(fis);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getString(String key) {
        return getString(key, null);
    }

    public static String getString(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public static String[] getStringArray(String key) {
        String val = properties.getProperty(key);
        if (StringUtils.isNotEmpty(val)) {
            String[] vals = val.split(",");
            return vals;
        }
        return null;
    }
}
