package com.tests.main.sanity;

public class SanityDriver {
    public static void main(String[] args) {
        try {
            new SanityAttributesMutations().run();
            new SanityBasicTypeAttributesMutations().run();
            new SanityArrayTypeAttributesMutations().run();
            new SanityComplexTypeAttributesMutations().run();
            new SanityMapTypeAttributesMutations().run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
