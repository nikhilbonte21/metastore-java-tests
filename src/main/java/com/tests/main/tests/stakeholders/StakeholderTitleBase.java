package com.tests.main.tests.stakeholders;


import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getDomainEntity;
import static com.tests.main.tests.stakeholders.StakeholderUtils.ATTR_DOMAIN_QUALIFIED_NAMES;
import static com.tests.main.tests.stakeholders.StakeholderUtils.PATTERN_QUALIFIED_NAME_ALL_DOMAINS;
import static com.tests.main.tests.stakeholders.StakeholderUtils.PATTERN_QUALIFIED_NAME_DOMAIN;
import static com.tests.main.tests.stakeholders.StakeholderUtils.REL_ATTR_STAKEHOLDERS;
import static com.tests.main.tests.stakeholders.StakeholderUtils.STAR;
import static com.tests.main.tests.stakeholders.StakeholderUtils.getStakeholderTitle;
import static com.tests.main.tests.stakeholders.StakeholderUtils.getTitleUUID;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.runAsAdmin;
import static com.tests.main.utils.TestUtil.runAsGod;
import static com.tests.main.utils.TestUtil.setAtlasClient;
import static com.tests.main.utils.TestUtil.sleep;
import static org.junit.Assert.*;


public class StakeholderTitleBase implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(StakeholderTitleBase.class);

    public static void main(String[] args) throws Exception {
        try {
            new StakeholderTitleBase().run();
        } finally {
            runAsGod();
            cleanUpAll();
            ESUtil.close();
            runAsAdmin();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running StakeholderTitleBase tests");

        long start = System.currentTimeMillis();
        try {
            //setAtlasClient(ADMIN);

            createTitleWithoutDomainAttribute();

            createStakeholderTitleForAllDomains();

            createStakeholderTitleForASpecificDomain();

            updateStakeholder_ADD_and_REMOVE_domains();



        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running StakeholderTitleBase tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void createTitleWithoutDomainAttribute() throws Exception {
        LOG.info(">> createTitleWithoutDomainAttribute");

        AtlasEntity title = getStakeholderTitle("stakeholder_title_0");

        boolean failed = false;
        try {
            createEntity(title).getCreatedEntities().get(0).getGuid();
        } catch (Exception exception) {
            //TODO
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("Please provide attribute stakeholderTitleDomainQualifiedNames"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }


        LOG.info("<< createTitleWithoutDomainAttribute");
    }

    private static void createStakeholderTitleForAllDomains() throws Exception {
        LOG.info(">> createStakeholderTitleForAllDomains");

        AtlasEntity title = getStakeholderTitle("stakeholder_title_0", STAR);
        String titleGuid = createEntity(title).getCreatedEntities().get(0).getGuid();

        sleep(2);
        title = getEntity(titleGuid);

        String titleQN = (String) title.getAttribute(QUALIFIED_NAME);
        String stakeholderUUID = getTitleUUID(titleQN);


        assertEquals(PATTERN_QUALIFIED_NAME_ALL_DOMAINS + stakeholderUUID, titleQN);

        List<String> qNames = (List<String>) title.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);

        assertEquals(1, qNames.size());
        assertEquals(STAR, qNames.get(0));

        LOG.info(">> createStakeholderTitleForAllDomains");
    }

    private static void updateStakeholder_ADD_and_REMOVE_domains() throws Exception {
        LOG.info(">> updateStakeholder_ADD_and_REMOVE_domains");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        AtlasEntity domain_1 = getDomainEntity("domain_1", null, null);
        String domain_1Guid = createEntity(domain_1).getCreatedEntities().get(0).getGuid();

        AtlasEntity domain_2 = getDomainEntity("domain_2", null, null);
        String domain_2Guid = createEntity(domain_2).getCreatedEntities().get(0).getGuid();

        sleep(2);
        domain = getEntity(domainGuid);
        domain_1 = getEntity(domain_1Guid);
        domain_2 = getEntity(domain_2Guid);
        String domainQn = (String) domain.getAttribute(QUALIFIED_NAME);
        String domain_1Qn = (String) domain_1.getAttribute(QUALIFIED_NAME);
        String domain_2Qn = (String) domain_2.getAttribute(QUALIFIED_NAME);

        AtlasEntity title = getStakeholderTitle("stakeholder_title_0", domainQn, domain_1Qn);
        String titleGuid = createEntity(title).getCreatedEntities().get(0).getGuid();

        sleep(2);
        title = getEntity(titleGuid);

        String titleQN = (String) title.getAttribute(QUALIFIED_NAME);
        String stakeholderUUID = getTitleUUID(titleQN);

        assertEquals(PATTERN_QUALIFIED_NAME_DOMAIN + stakeholderUUID, titleQN);

        List<String> qNames = (List<String>) title.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
        assertEquals(2, qNames.size());
        assertTrue(qNames.contains(domainQn));
        assertTrue(qNames.contains(domain_1Qn));

        //-------
        title.setAttribute(ATTR_DOMAIN_QUALIFIED_NAMES, Arrays.asList(domain_1Qn));
        title.getRelationshipAttributes().remove(REL_ATTR_STAKEHOLDERS);
        createEntity(title);

        sleep(2);
        title = getEntity(titleGuid);

        qNames = (List<String>) title.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
        title.getRelationshipAttributes().remove(REL_ATTR_STAKEHOLDERS);
        assertEquals(1, qNames.size());
        assertTrue(qNames.contains(domain_1Qn));

        //-------
        title.setAttribute(ATTR_DOMAIN_QUALIFIED_NAMES, Arrays.asList(domainQn, domain_2Qn));
        title.getRelationshipAttributes().remove(REL_ATTR_STAKEHOLDERS);
        createEntity(title);

        sleep(2);
        title = getEntity(titleGuid);

        qNames = (List<String>) title.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
        assertEquals(2, qNames.size());
        assertTrue(qNames.contains(domainQn));
        assertTrue(qNames.contains(domain_2Qn));

        LOG.info(">> updateStakeholder_ADD_and_REMOVE_domains");
    }

    private static void createStakeholderTitleForASpecificDomain() throws Exception {
        LOG.info(">> createStakeholderTitleForASpecificDomain");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        sleep(2);
        domain = getEntity(domainGuid);
        String domainQn = (String) domain.getAttribute(QUALIFIED_NAME);

        AtlasEntity title = getStakeholderTitle("stakeholder_title_0", domainQn);
        String titleGuid = createEntity(title).getCreatedEntities().get(0).getGuid();

        sleep(2);
        title = getEntity(titleGuid);
        domain = getEntity(domainGuid);

        String titleQN = (String) title.getAttribute(QUALIFIED_NAME);
        String stakeholderUUID = getTitleUUID(titleQN);

        assertEquals(PATTERN_QUALIFIED_NAME_DOMAIN + stakeholderUUID, titleQN);

        List<String> qNames = (List<String>) title.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);

        assertEquals(1, qNames.size());
        assertEquals(domainQn, qNames.get(0));

        LOG.info(">> createStakeholderTitleForASpecificDomain");
    }


}