package com.tests.main.glossary.tests;

import com.tests.main.glossary.utils.ESUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tests.main.glossary.utils.TestUtils.cleanUpAll;

public class TestsRunner {
    private static final Logger LOG = LoggerFactory.getLogger(TestsRunner.class);
    public static Set<String> guidsToDelete = new HashSet<>();


    public static void main(String[] args) throws Exception {
        LOG.info("Started running TestsRunner");
        List<String> testsExecuted = new ArrayList<>();

        long start = System.currentTimeMillis();

        try {
            Reflections reflections = new Reflections();
            Set<Class<? extends TestsMain>> tests = reflections.getSubTypesOf(TestsMain.class);
            for (Class testClass : tests) {
                Class cls = Class.forName(testClass.getName());
                TestsMain test = (TestsMain) cls.newInstance();
                test.run();

                testsExecuted.add(testClass.getSimpleName());
            }
        } finally {
            String exe = testsExecuted.stream().collect(Collectors.joining("\n"));
            LOG.info("Executed following Tests \n{}\n", exe);
            LOG.info("Completed running TestsRunner, took {} seconds", (System.currentTimeMillis() - start) / 1000 );

            cleanUpAll();
            ESUtils.close();
        }
    }
}
