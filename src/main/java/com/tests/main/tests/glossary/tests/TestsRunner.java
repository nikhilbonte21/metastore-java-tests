package com.tests.main.tests.glossary.tests;

import com.tests.main.utils.ESUtil;
import org.apache.commons.collections.CollectionUtils;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tests.main.utils.TestUtil.cleanUpAll;

public class TestsRunner {
    private static final Logger LOG = LoggerFactory.getLogger(TestsRunner.class);
    public static List<String> guidsToDelete = new ArrayList<>();


    public static void main(String[] args) throws Exception {
        LOG.info("Started running TestsRunner");
        Set<String> testsExecuted = new HashSet<>();
        Set<String> allTestsClass = new HashSet<>();

        long start = System.currentTimeMillis();

        try {
            Reflections reflections = new Reflections();
            Set<Class<? extends TestsMain>> tests = reflections.getSubTypesOf(TestsMain.class);
            LOG.info("Found {} tests class", tests.size());
            allTestsClass = tests.stream().map(x -> x.getSimpleName()).collect(Collectors.toSet());
            LOG.info("\n{}\n", allTestsClass.stream().collect(Collectors.joining("\n")));

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

            String pendingLists = CollectionUtils.subtract(allTestsClass, testsExecuted).stream().collect(Collectors.joining("\n")).toString();
            //LOG.info("Pending following Tests \n{}\n", pendingLists.stream().collect(Collectors.joining("\n")));
            LOG.info("Pending following Tests \n{}\n", pendingLists);

            cleanUpAll();
            ESUtil.close();
        }
    }
}
