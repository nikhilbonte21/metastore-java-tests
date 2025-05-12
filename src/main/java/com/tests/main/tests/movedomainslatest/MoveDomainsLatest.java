package com.tests.main.tests.movedomainslatest;

import com.tests.main.CustomException;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelationship;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tests.main.tests.glossary.tests.TestsRunner.guidsToDelete;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.DATA_DOMAIN;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.DATA_DOMAIN_TYPE;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.DATA_PRODUCTS;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.PARENT_DOMAIN;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.PARENT_DOMAIN_QN;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.SUB_DOMAINS;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.SUPER_DOMAIN_QN;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.chain;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getDomainAsRelation;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getDomainEntity;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getNanoId;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getPersona;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getPersonaPolicy;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getProductEntity;
import static com.tests.main.tests.stakeholders.StakeholderUtils.ATTR_DOMAIN_QUALIFIED_NAME;
import static com.tests.main.tests.stakeholders.StakeholderUtils.ATTR_DOMAIN_QUALIFIED_NAMES;
import static com.tests.main.tests.stakeholders.StakeholderUtils.getStakeholder;
import static com.tests.main.tests.stakeholders.StakeholderUtils.getStakeholderTitle;
import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.cleanUpAllForEach;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.createRelationship;
import static com.tests.main.utils.TestUtil.deleteEntitySoft;
import static com.tests.main.utils.TestUtil.getESAlias;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.isRunAsMember;
import static com.tests.main.utils.TestUtil.runAsAdmin;
import static com.tests.main.utils.TestUtil.runAsGod;
import static com.tests.main.utils.TestUtil.runAsMember;
import static com.tests.main.utils.TestUtil.sleep;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class MoveDomainsLatest implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(MoveDomainsLatest.class);

    public static void main(String[] args) throws Exception {
        long start = System.currentTimeMillis();
        try {
            new MoveDomainsLatest().run();
        } finally {
            runAsGod();
            cleanUpAllForEach();
            ESUtil.close();
            runAsAdmin();
            LOG.info("Completed running MoveDomains tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running MoveDomains tests");

        long start = System.currentTimeMillis();
        try {

            verifyPersonaESAlias();

            moveSuperDomainToNewParentDomain();

            createSuperDomainWithdeNorAttrs();

            //can not be tested now with automation
            //addParentAsRelationshipJPMCcase();

            oneLevelParentToOneLevelParent();

            oneLevelParentToOneLevelParentViaParentDomainUpdate();

            sourceNestedDomainToNewNestedDomain();

            domainWithSameNameAtSameLevel();

            domainWithSameNameAtDifferentLevel();

            createWithoutDenorAttrs();

            createDupSuperDomain();

            sourceDomainToNewDomainViaRelationAPI();

            parentToNoParent();

            verifyStakeholderTitles();

            verifyStakeholdes();

            moveWithOnlyRequiredDomainPolicyPermissions();

            restoreMovedDomain();


        } finally {
            LOG.info("Completed running MoveDomains tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void moveSuperDomainToNewParentDomain() throws Exception {
        LOG.info(">> noParentToNewDomain();");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("target_domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        domain = getEntity(domainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", domainQN, domainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(domainGuid));
        createEntity(product).getCreatedEntities().get(0).getGuid();

        AtlasEntity domainToMove = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainToMove.setAttribute(NAME, domain.getAttribute(NAME));
        domainToMove.setAttribute(QUALIFIED_NAME, domainQN);
        domainToMove.setAttribute(PARENT_DOMAIN_QN, domain.getAttribute(PARENT_DOMAIN_QN));
        domainToMove.setAttribute(SUPER_DOMAIN_QN, domain.getAttribute(SUPER_DOMAIN_QN));

        domainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));

        sleep(1);

        boolean failed = false;
        try {
            createEntity(domainToMove);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("Cannot move Super Domain inside another domain"));
            failed = true;
        } finally {
            if (!failed) {
                throw new CustomException("This test should have failed");
            }
        }

        LOG.info(">> noParentToNewDomain();");
    }

    private static void createSuperDomainWithdeNorAttrs() throws Exception {
        LOG.info(">> createSuperDomainWithdeNorAttrs");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        domain = getEntity(domainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);

        AtlasEntity subDomain = getDomainEntity("sub_domain_", domainQN, domainQN);
        String subDomainGuid = createEntity(subDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        subDomain = getEntity(subDomainGuid);
        String subDomainQN = (String) subDomain.getAttribute(QUALIFIED_NAME);

        assertEquals(chain(getNanoId(subDomainQN)), subDomain.getAttribute(QUALIFIED_NAME));
        assertNull(subDomain.getAttribute(PARENT_DOMAIN_QN));
        assertNull(subDomain.getAttribute(SUPER_DOMAIN_QN));


        LOG.info(">> createSuperDomainWithdeNorAttrs");
    }

    private static void addParentAsRelationshipJPMCcase() throws Exception {
        LOG.info(">> addParentAsRelationshipJPMCcase");

        AtlasEntity targetDomain = getDomainEntity("target_domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);
        targetDomain = getEntity(targetDomainGuid);
        String targetDomainQN = (String) targetDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity domain = getDomainEntity("domain_0", targetDomainQN, targetDomainQN);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();
        sleep(1);

        domain = getEntity(domainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", domainQN, domainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(domainGuid));
        createEntity(product).getCreatedEntities().get(0).getGuid();

        AtlasEntity domainToMove = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainToMove.setAttribute(NAME, domain.getAttribute(NAME));
        domainToMove.setAttribute(QUALIFIED_NAME, domainQN);
        domainToMove.setAttribute(PARENT_DOMAIN_QN, domain.getAttribute(PARENT_DOMAIN_QN));
        domainToMove.setAttribute(SUPER_DOMAIN_QN, domain.getAttribute(SUPER_DOMAIN_QN));

        domainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));

        createEntity(domainToMove);
        sleep(1);

        domainToMove = getEntity(domainGuid);

        assertEquals(targetDomainQN + "/" + domainQN , domainToMove.getAttribute(QUALIFIED_NAME));


        LOG.info(">> addParentAsRelationshipJPMCcase");
    }

    private static void oneLevelParentToOneLevelParent() throws Exception {
        LOG.info(">> oneLevelParentToOneLevelParent();");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        domain = getEntity(domainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);

        AtlasEntity subDomain = getDomainEntity("sub_domain_0", domainQN, domainQN);
        subDomain.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(domainGuid));
        String subDomainGuid = createEntity(subDomain).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("target_domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        subDomain = getEntity(subDomainGuid);
        String subDomainQN = (String) subDomain.getAttribute(QUALIFIED_NAME);

        targetDomain = getEntity(targetDomainGuid);
        String targetDomainQN = (String) targetDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", subDomainQN, subDomainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(subDomainGuid));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();


        AtlasEntity subDomainToMove = new AtlasEntity(DATA_DOMAIN_TYPE);
        subDomainToMove.setAttribute(NAME, subDomain.getAttribute(NAME));
        subDomainToMove.setAttribute(QUALIFIED_NAME, subDomainQN);
        subDomainToMove.setAttribute(PARENT_DOMAIN_QN, subDomain.getAttribute(PARENT_DOMAIN_QN));
        subDomainToMove.setAttribute(SUPER_DOMAIN_QN, subDomain.getAttribute(SUPER_DOMAIN_QN));

        subDomainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));

        createEntity(subDomainToMove);

        sleep(1);

        subDomainToMove = getEntity(subDomainGuid);

        product = getEntity(productGuid);
        String productQN = (String) product.getAttribute(QUALIFIED_NAME);

        assertEquals(chain(getNanoId(targetDomainQN), getNanoId(subDomainQN)), subDomainToMove.getAttribute(QUALIFIED_NAME));
        assertEquals(targetDomainQN, subDomainToMove.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(targetDomainQN, subDomainToMove.getAttribute(SUPER_DOMAIN_QN));

        assertNotNull(subDomainToMove.getRelationshipAttribute(PARENT_DOMAIN));
        assertEquals(targetDomainGuid, ((HashMap) subDomainToMove.getRelationshipAttribute(PARENT_DOMAIN)).get("guid"));


        assertEquals(chain(getNanoId(targetDomainQN), getNanoId(subDomainQN)) + "/product/" + getNanoId(productQN), product.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainQN), getNanoId(subDomainQN)), product.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(targetDomainQN, product.getAttribute(SUPER_DOMAIN_QN));

        assertNotNull(product.getRelationshipAttribute(DATA_DOMAIN));
        assertEquals(subDomainGuid, ((HashMap) product.getRelationshipAttribute(DATA_DOMAIN)).get("guid"));

        LOG.info(">> oneLevelParentToOneLevelParent();");
    }

    private static void oneLevelParentToOneLevelParentViaParentDomainUpdate() throws Exception {
        LOG.info(">> oneLevelParentToOneLevelParentViaParentDomainUpdate();");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        domain = getEntity(domainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);

        AtlasEntity subDomain = getDomainEntity("sub_domain_0", domainQN, domainQN);
        subDomain.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(domainGuid));
        String subDomainGuid = createEntity(subDomain).getCreatedEntities().get(0).getGuid();
        String subDomainName = (String) subDomain.getAttribute(NAME);

        AtlasEntity targetDomain = getDomainEntity("target_domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();
        String targetDomainName = (String) targetDomain.getAttribute(NAME);

        sleep(1);

        subDomain = getEntity(subDomainGuid);
        String subDomainQN = (String) subDomain.getAttribute(QUALIFIED_NAME);

        targetDomain = getEntity(targetDomainGuid);
        String targetDomainQN = (String) targetDomain.getAttribute(QUALIFIED_NAME);


        AtlasEntity product = getProductEntity("prod_0", subDomainQN, subDomainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(subDomainGuid));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();


        AtlasEntity domainToUpdate = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainToUpdate.setAttribute(NAME, targetDomain.getAttribute(NAME));
        domainToUpdate.setAttribute(QUALIFIED_NAME, targetDomainQN);

        domainToUpdate.setRelationshipAttribute(SUB_DOMAINS, Arrays.asList(getDomainAsRelation(subDomainGuid)));

        boolean failed = false;
        try {
            createEntity(domainToUpdate);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("Cannot update Domain's subDomains or dataProducts relations"));
            failed = true;
        } finally {
            if (!failed) {
                throw new CustomException("This test should have failed");
            }
        }

        LOG.info(">> oneLevelParentToOneLevelParentViaParentDomainUpdate();");
    }

    private static void sourceNestedDomainToNewNestedDomain() throws Exception {
        LOG.info(">> sourceNestedDomainToNewNestedDomain");

        /*
        *
        * source_domain_parent_1 -> source_domain_nested_0 -> source_domain_nested_1 -> star_domain -> ....

            star_domain -> sub_star_domain_1 -> sub_star_product_1
                                             -> sub_star_product_2
                        -> sub_star_domain_2 -> sub_star_product_3
                                             -> sub_star_product_4
                        -> star_product_1
                        -> star_product_2


            target_domain_parent_1 -> target_domain_nested_0 -> target_domain_nested_1 -> star_domain -> ....
        *
        * */

        AtlasEntity sourceDomainParent = getDomainEntity("source_domain_parent_1", null, null);
        String sourceDomainParentGuid = createEntity(sourceDomainParent).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomainParent = getDomainEntity("target_domain_parent_1", null, null);
        String targetDomainParentGuid = createEntity(targetDomainParent).getCreatedEntities().get(0).getGuid();

        sleep(1);

        sourceDomainParent = getEntity(sourceDomainParentGuid);
        targetDomainParent = getEntity(targetDomainParentGuid);

        String sourceDomainParentName = (String) sourceDomainParent.getAttribute(NAME);
        String sourceDomainParentQN = (String) sourceDomainParent.getAttribute(QUALIFIED_NAME);
        String targetDomainParentName = (String) targetDomainParent.getAttribute(NAME);
        String targetDomainParentQN = (String) targetDomainParent.getAttribute(QUALIFIED_NAME);

        AtlasEntity sourceDomainNested = getDomainEntity("source_domain_nested_0", sourceDomainParentQN, sourceDomainParentQN);
        sourceDomainNested.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(sourceDomainParentGuid));
        String sourceDomainNestedGuid = createEntity(sourceDomainNested).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomainNested = getDomainEntity("target_domain_nested_0", targetDomainParentQN, targetDomainParentQN);
        targetDomainNested.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainParentGuid));
        String targetDomainNestedGuid = createEntity(targetDomainNested).getCreatedEntities().get(0).getGuid();

        sleep(1);

        sourceDomainNested = getEntity(sourceDomainNestedGuid);
        targetDomainNested = getEntity(targetDomainNestedGuid);

        String sourceDomainNestedName = (String) sourceDomainNested.getAttribute(NAME);
        String targetDomainNestedName = (String) targetDomainNested.getAttribute(NAME);
        String sourceDomainNestedQN = (String) sourceDomainNested.getAttribute(QUALIFIED_NAME);
        String targetDomainNestedQN = (String) targetDomainNested.getAttribute(QUALIFIED_NAME);


        AtlasEntity sourceDomainNested1 = getDomainEntity("source_domain_nested_1", sourceDomainNestedQN, sourceDomainParentQN);
        sourceDomainNested1.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(sourceDomainNestedGuid));
        String sourceDomainNestedGuid1 = createEntity(sourceDomainNested1).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomainNested1 = getDomainEntity("target_domain_nested_1", targetDomainNestedQN, targetDomainParentQN);
        targetDomainNested1.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainNestedGuid));
        String targetDomainNestedGuid1 = createEntity(targetDomainNested1).getCreatedEntities().get(0).getGuid();

        sleep(1);

        sourceDomainNested1 = getEntity(sourceDomainNestedGuid1);
        targetDomainNested1 = getEntity(targetDomainNestedGuid1);

        String sourceDomainNestedName1 = (String) sourceDomainNested1.getAttribute(NAME);
        String targetDomainNestedName1 = (String) targetDomainNested1.getAttribute(NAME);
        String sourceDomainNestedQN1 = (String) sourceDomainNested1.getAttribute(QUALIFIED_NAME);
        String targetDomainNestedQN1 = (String) targetDomainNested1.getAttribute(QUALIFIED_NAME);


        AtlasEntity starDomain = getDomainEntity("star_domain", sourceDomainNestedQN1, sourceDomainParentQN);
        starDomain.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(sourceDomainNestedGuid1));
        String starDomainGuid = createEntity(starDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        starDomain = getEntity(starDomainGuid);
        String starDomainName = (String) starDomain.getAttribute(NAME);
        String starDomainQN = (String) starDomain.getAttribute(QUALIFIED_NAME);


        AtlasEntity subStarDomain1 = getDomainEntity("sub_star_domain_1", starDomainQN, sourceDomainParentQN);
        subStarDomain1.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(starDomainGuid));
        String subStarDomain1Guid = createEntity(subStarDomain1).getCreatedEntities().get(0).getGuid();

        AtlasEntity subStarDomain2 = getDomainEntity("sub_star_domain_2", starDomainQN, sourceDomainParentQN);
        subStarDomain2.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(starDomainGuid));
        String subStarDomain2Guid = createEntity(subStarDomain2).getCreatedEntities().get(0).getGuid();

        AtlasEntity starProduct1 = getProductEntity("star_product_1", starDomainQN, sourceDomainParentQN);
        starProduct1.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(starDomainGuid));
        String starProduct1Guid = createEntity(starProduct1).getCreatedEntities().get(0).getGuid();

        AtlasEntity starProduct2 = getProductEntity("star_product_2", starDomainQN, sourceDomainParentQN);
        starProduct2.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(starDomainGuid));
        String starProduct2Guid = createEntity(starProduct2).getCreatedEntities().get(0).getGuid();


        sleep(1);

        subStarDomain1 = getEntity(subStarDomain1Guid);
        subStarDomain2 = getEntity(subStarDomain2Guid);
        starProduct1 = getEntity(starProduct1Guid);
        starProduct2 = getEntity(starProduct2Guid);

        String subStarDomain1Name = (String) subStarDomain1.getAttribute(NAME);
        String subStarDomain2Name = (String) subStarDomain2.getAttribute(NAME);
        String starProduct1Name = (String) starProduct1.getAttribute(NAME);
        String starProduct2Name = (String) starProduct2.getAttribute(NAME);
        String subStarDomain1QN = (String) subStarDomain1.getAttribute(QUALIFIED_NAME);
        String subStarDomain2QN = (String) subStarDomain2.getAttribute(QUALIFIED_NAME);
        String starProduct1QN = (String) starProduct1.getAttribute(QUALIFIED_NAME);
        String starProduct2QN = (String) starProduct2.getAttribute(QUALIFIED_NAME);


        AtlasEntity subStarProduct1 = getProductEntity("sub_star_product_1", subStarDomain1QN, sourceDomainParentQN);
        subStarProduct1.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(subStarDomain1Guid));
        String subStarProduct1Guid = createEntity(subStarProduct1).getCreatedEntities().get(0).getGuid();


        AtlasEntity subStarProduct2 = getProductEntity("sub_star_product_2", subStarDomain1QN, sourceDomainParentQN);
        subStarProduct2.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(subStarDomain1Guid));
        String subStarProduct2Guid = createEntity(subStarProduct2).getCreatedEntities().get(0).getGuid();


        AtlasEntity subStarProduct3 = getProductEntity("sub_star_product_3", subStarDomain2QN, sourceDomainParentQN);
        subStarProduct3.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(subStarDomain2Guid));
        String subStarProduct3Guid = createEntity(subStarProduct3).getCreatedEntities().get(0).getGuid();


        AtlasEntity subStarProduct4 = getProductEntity("sub_star_product_4", subStarDomain2QN, sourceDomainParentQN);
        subStarProduct4.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(subStarDomain2Guid));
        String subStarProduct4Guid = createEntity(subStarProduct4).getCreatedEntities().get(0).getGuid();



        sleep(1);

        subStarProduct1 = getEntity(subStarProduct1Guid);
        subStarProduct2 = getEntity(subStarProduct2Guid);
        subStarProduct3 = getEntity(subStarProduct3Guid);
        subStarProduct4 = getEntity(subStarProduct4Guid);

        String subStarProduct1QN = (String) subStarProduct1.getAttribute(QUALIFIED_NAME);
        String subStarProduct2QN = (String) subStarProduct2.getAttribute(QUALIFIED_NAME);
        String subStarProduct3QN = (String) subStarProduct3.getAttribute(QUALIFIED_NAME);
        String subStarProduct4QN = (String) subStarProduct4.getAttribute(QUALIFIED_NAME);


        AtlasEntity domainToMove = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainToMove.setAttribute(NAME, starDomain.getAttribute(NAME));
        domainToMove.setAttribute(QUALIFIED_NAME, starDomainQN);
        domainToMove.setAttribute(PARENT_DOMAIN_QN, starDomain.getAttribute(PARENT_DOMAIN_QN));
        domainToMove.setAttribute(SUPER_DOMAIN_QN, starDomain.getAttribute(SUPER_DOMAIN_QN));

        domainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainNestedGuid1));

        createEntity(domainToMove);

        sleep(1);

        sourceDomainNested1 = getEntity(sourceDomainNestedGuid1);
        assertEquals(chain(getNanoId(sourceDomainParentQN), getNanoId(sourceDomainNestedQN), getNanoId(sourceDomainNestedQN1)), sourceDomainNested1.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(sourceDomainParentQN), getNanoId(sourceDomainNestedQN)), sourceDomainNested1.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(sourceDomainParentQN)), sourceDomainNested1.getAttribute(SUPER_DOMAIN_QN));
        assertEquals("ACTIVE", ((Map<String, Object>) sourceDomainNested1.getRelationshipAttribute(PARENT_DOMAIN)).get("relationshipStatus"));
        assertEquals(sourceDomainNestedGuid, ((Map<String, Object>) sourceDomainNested1.getRelationshipAttribute(PARENT_DOMAIN)).get("guid"));
        List<Map<String, Object>> subDomains = (List<Map<String, Object>>) sourceDomainNested1.getRelationshipAttribute(SUB_DOMAINS);
        assertEquals(0, subDomains.size());
        //assertEquals("DELETED", subDomains.get(0).get("relationshipStatus"));


        starDomain = getEntity(starDomainGuid);
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN)), starDomain.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1)), starDomain.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainParentQN)), starDomain.getAttribute(SUPER_DOMAIN_QN));
        subDomains = (List<Map<String, Object>>) starDomain.getRelationshipAttribute(SUB_DOMAINS);
        assertEquals(2, subDomains.size());
        assertEquals("ACTIVE", subDomains.get(0).get("relationshipStatus"));
        assertEquals("ACTIVE", subDomains.get(1).get("relationshipStatus"));
        List<Map<String, Object>> subProducts = (List<Map<String, Object>>) starDomain.getRelationshipAttribute(DATA_PRODUCTS);
        assertEquals(2, subProducts.size());
        assertEquals("ACTIVE", subProducts.get(0).get("relationshipStatus"));
        assertEquals("ACTIVE", subProducts.get(1).get("relationshipStatus"));


        subStarDomain1 = getEntity(subStarDomain1Guid);
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN), getNanoId(subStarDomain1QN)), subStarDomain1.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN)), subStarDomain1.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainParentQN)), subStarDomain1.getAttribute(SUPER_DOMAIN_QN));
        assertEquals(0, ((List) subStarDomain1.getRelationshipAttribute(SUB_DOMAINS)).size());
        subProducts = (List<Map<String, Object>>) subStarDomain1.getRelationshipAttribute(DATA_PRODUCTS);
        assertEquals(2, subProducts.size());
        assertEquals("ACTIVE", subProducts.get(0).get("relationshipStatus"));
        assertEquals("ACTIVE", subProducts.get(1).get("relationshipStatus"));

        subStarDomain2 = getEntity(subStarDomain2Guid);
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN), getNanoId(subStarDomain2QN)), subStarDomain2.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN)), subStarDomain2.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainParentQN)), subStarDomain2.getAttribute(SUPER_DOMAIN_QN));
        assertEquals(0, ((List) subStarDomain2.getRelationshipAttribute(SUB_DOMAINS)).size());
        subProducts = (List<Map<String, Object>>) subStarDomain2.getRelationshipAttribute(DATA_PRODUCTS);
        assertEquals(2, subProducts.size());
        assertEquals("ACTIVE", subProducts.get(0).get("relationshipStatus"));
        assertEquals("ACTIVE", subProducts.get(1).get("relationshipStatus"));


        starProduct1 = getEntity(starProduct1Guid);
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN)) + "/product/" + getNanoId(starProduct1QN), starProduct1.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN)), starProduct1.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainParentQN)), starProduct1.getAttribute(SUPER_DOMAIN_QN));
        Map<String, Object> parentDomain = (Map<String, Object>) starProduct1.getRelationshipAttribute(DATA_DOMAIN);
        assertEquals(starDomainGuid, parentDomain.get("guid"));


        starProduct2 = getEntity(starProduct2Guid);
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN)) + "/product/" + getNanoId(starProduct2QN), starProduct2.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN)), starProduct2.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainParentQN)), starProduct2.getAttribute(SUPER_DOMAIN_QN));
        parentDomain = (Map<String, Object>) starProduct2.getRelationshipAttribute(DATA_DOMAIN);
        assertEquals(starDomainGuid, parentDomain.get("guid"));




        subStarProduct1 = getEntity(subStarProduct1Guid);
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN), getNanoId(subStarDomain1QN)) + "/product/" + getNanoId(subStarProduct1QN), subStarProduct1.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN), getNanoId(subStarDomain1QN)), subStarProduct1.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainParentQN)), subStarProduct1.getAttribute(SUPER_DOMAIN_QN));
        parentDomain = (Map<String, Object>) subStarProduct1.getRelationshipAttribute(DATA_DOMAIN);
        assertEquals(subStarDomain1Guid, parentDomain.get("guid"));


        subStarProduct2 = getEntity(subStarProduct2Guid);
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN), getNanoId(subStarDomain1QN)) + "/product/" + getNanoId(subStarProduct2QN), subStarProduct2.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN), getNanoId(subStarDomain1QN)), subStarProduct2.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainParentQN)), subStarProduct2.getAttribute(SUPER_DOMAIN_QN));
        parentDomain = (Map<String, Object>) subStarProduct2.getRelationshipAttribute(DATA_DOMAIN);
        assertEquals(subStarDomain1Guid, parentDomain.get("guid"));


        subStarProduct3 = getEntity(subStarProduct3Guid);
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN), getNanoId(subStarDomain2QN)) + "/product/" + getNanoId(subStarProduct3QN), subStarProduct3.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN), getNanoId(subStarDomain2QN)), subStarProduct3.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainParentQN)), subStarProduct3.getAttribute(SUPER_DOMAIN_QN));
        parentDomain = (Map<String, Object>) subStarProduct3.getRelationshipAttribute(DATA_DOMAIN);
        assertEquals(subStarDomain2Guid, parentDomain.get("guid"));


        subStarProduct4 = getEntity(subStarProduct4Guid);
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN), getNanoId(subStarDomain2QN)) + "/product/" + getNanoId(subStarProduct4QN), subStarProduct4.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1), getNanoId(starDomainQN), getNanoId(subStarDomain2QN)), subStarProduct4.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainParentQN)), subStarProduct4.getAttribute(SUPER_DOMAIN_QN));
        parentDomain = (Map<String, Object>) subStarProduct4.getRelationshipAttribute(DATA_DOMAIN);
        assertEquals(subStarDomain2Guid, parentDomain.get("guid"));

        LOG.info(">> sourceNestedDomainToNewNestedDomain");
    }

    private static void domainWithSameNameAtSameLevel() throws Exception {
        LOG.info(">> domainWithSameNameAtSameLevel();");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("target_domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        domain = getEntity(domainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);

        targetDomain = getEntity(targetDomainGuid);
        String targetDomainQN = (String) targetDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity subDomain = getDomainEntity("sub_domain", domainQN, domainQN);
        subDomain.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(domainGuid));
        String subDomainGuid = createEntity(subDomain).getCreatedEntities().get(0).getGuid();

        AtlasEntity subDomain_0 = getDomainEntity("sub_domain_0", targetDomainQN, targetDomainQN);
        subDomain_0.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));
        String subDomain_0Guid = createEntity(subDomain_0).getCreatedEntities().get(0).getGuid();



        sleep(1);

        subDomain = getEntity(subDomainGuid);
        subDomain_0 = getEntity(subDomain_0Guid);

        String subDomain_0QN = (String) subDomain_0.getAttribute(QUALIFIED_NAME);
        String subDomainQN = (String) subDomain.getAttribute(QUALIFIED_NAME);

        // domain -> subDomain
        // targetDomain -> subDomain_0
        // subDomain == subDomain_0

        AtlasEntity subDomainToMove = new AtlasEntity(DATA_DOMAIN_TYPE);
        subDomainToMove.setAttribute(NAME, subDomain_0.getAttribute(NAME));
        subDomainToMove.setAttribute(QUALIFIED_NAME, subDomainQN);
        subDomainToMove.setAttribute(PARENT_DOMAIN_QN, subDomain.getAttribute(PARENT_DOMAIN_QN));
        subDomainToMove.setAttribute(SUPER_DOMAIN_QN, subDomain.getAttribute(SUPER_DOMAIN_QN));

        subDomainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));

        boolean failed = false;
        try {
            createEntity(subDomainToMove);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("already exists"));
            failed = true;
        } finally {
            if (!failed) {
                throw new CustomException("This test should have failed");
            }
        }

        LOG.info(">> domainWithSameNameAtSameLevel();");
    }

    private static void domainWithSameNameAtDifferentLevel() throws Exception {
        LOG.info(">> domainWithSameNameAtDifferentLevel();");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("target_domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        domain = getEntity(domainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);

        targetDomain = getEntity(targetDomainGuid);
        String targetDomainQN = (String) targetDomain.getAttribute(QUALIFIED_NAME);
        String targetDomainName = (String) targetDomain.getAttribute(NAME);


        AtlasEntity subDomain = getDomainEntity("sub_domain", domainQN, domainQN);
        subDomain.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(domainGuid));
        String subDomainGuid = createEntity(subDomain).getCreatedEntities().get(0).getGuid();

        AtlasEntity subDomain_0 = getDomainEntity("sub_domain_0", targetDomainQN, targetDomainQN);
        subDomain_0.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));
        String subDomain_0Guid = createEntity(subDomain_0).getCreatedEntities().get(0).getGuid();

        sleep(1);
        subDomain_0 = getEntity(subDomain_0Guid);
        String subDomain_0QN = (String) subDomain_0.getAttribute(QUALIFIED_NAME);

        AtlasEntity subDomain_1 = getDomainEntity("sub_domain_1", subDomain_0QN, targetDomainQN);
        subDomain_1.setAttribute(NAME, subDomain.getAttribute(NAME));
        subDomain_1.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(subDomain_0Guid));
        String subDomain_1Guid = createEntity(subDomain_1).getCreatedEntities().get(0).getGuid();

        sleep(1);

        subDomain = getEntity(subDomainGuid);

        String subDomainQN = (String) subDomain.getAttribute(QUALIFIED_NAME);
        String subDomainName = (String) subDomain.getAttribute(NAME);


        AtlasEntity subDomainToMove = new AtlasEntity(DATA_DOMAIN_TYPE);
        subDomainToMove.setAttribute(NAME, subDomain.getAttribute(NAME));
        subDomainToMove.setAttribute(QUALIFIED_NAME, subDomainQN);
        subDomainToMove.setAttribute(PARENT_DOMAIN_QN, subDomain.getAttribute(PARENT_DOMAIN_QN));
        subDomainToMove.setAttribute(SUPER_DOMAIN_QN, subDomain.getAttribute(SUPER_DOMAIN_QN));

        subDomainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));

        createEntity(subDomainToMove);

        sleep(1);

        subDomainToMove = getEntity(subDomainGuid);

        assertEquals(chain(getNanoId(targetDomainQN), getNanoId(subDomainQN)), subDomainToMove.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainQN)), subDomainToMove.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainQN)), subDomainToMove.getAttribute(SUPER_DOMAIN_QN));

        assertNotNull(subDomainToMove.getRelationshipAttribute(PARENT_DOMAIN));
        assertEquals(targetDomainGuid, ((HashMap) subDomainToMove.getRelationshipAttribute(PARENT_DOMAIN)).get("guid"));

        LOG.info(">> domainWithSameNameAtDifferentLevel();");
    }

    private static void createWithoutDenorAttrs() throws Exception {
        LOG.info(">> createWithoutDenorAttrs");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();
        sleep(1);

        domain = getEntity(domainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);

        AtlasEntity subDomain = getDomainEntity("subDomain_0", null, null);
        String subDomainName = (String) domain.getAttribute(NAME);
        subDomain.setAttribute(QUALIFIED_NAME, domainQN + "/domain/" +  subDomainName);
        subDomain.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(domainGuid));
        String subDomainGuid = createEntity(subDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        subDomain = getEntity(subDomainGuid);
        String subDomainQN = (String) subDomain.getAttribute(QUALIFIED_NAME);

        assertEquals(chain(getNanoId(domainQN), getNanoId(subDomainQN)), subDomain.getAttribute(QUALIFIED_NAME));
        assertNotNull(subDomain.getRelationshipAttribute(PARENT_DOMAIN));
        assertEquals(chain(getNanoId(domainQN)), subDomain.getAttribute(SUPER_DOMAIN_QN));
        assertEquals(chain(getNanoId(domainQN)), subDomain.getAttribute(PARENT_DOMAIN_QN));

        LOG.info(">> createWithoutDenorAttrs");
    }

    private static void createDupSuperDomain() throws Exception {
        LOG.info(">> createDupSuperDomain();");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);

        sleep(1);

        AtlasEntity anotherDomain = getDomainEntity("another_domain_0", null, null);
        anotherDomain.setAttribute(NAME, domain.getAttribute(NAME));


        boolean failed = false;
        try {
            createEntity(anotherDomain);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("already exists"));
            failed = true;
        } finally {
            if (!failed) {
                throw new CustomException("This test should have failed");
            }
        }

        LOG.info(">> createDupSuperDomain();");
    }

    private static void verifyPersonaESAlias() throws Exception {
        LOG.info(">> verifyPersonaESAlias();");

        AtlasEntity sourceDomain = getDomainEntity("source_domain_0", null, null);
        String sourceDomainGuid = createEntity(sourceDomain).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("target_domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();


        sleep(1);

        sourceDomain = getEntity(sourceDomainGuid);
        String sourceDomainQN = (String) sourceDomain.getAttribute(QUALIFIED_NAME);

        targetDomain = getEntity(targetDomainGuid);
        String targetDomainQN = (String) targetDomain.getAttribute(QUALIFIED_NAME);
        String targetDomainName = (String) targetDomain.getAttribute(NAME);

        AtlasEntity subDomain = getDomainEntity("sub_domain_0", sourceDomainQN, sourceDomainQN);
        subDomain.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(sourceDomainGuid));
        String subDomainGuid = createEntity(subDomain).getCreatedEntities().get(0).getGuid();
        String subDomainName = (String) subDomain.getAttribute(NAME);


        runAsAdmin();
        AtlasEntity persona = getPersona("persona_0");
        String personaGuid = createEntity(persona).getCreatedEntities().get(0).getGuid();

        sleep(1);
        subDomain = getEntity(subDomainGuid);
        String subDomainQN = (String) subDomain.getAttribute(QUALIFIED_NAME);

        persona = getEntity(personaGuid);

        String personaQN = (String) persona.getAttribute(QUALIFIED_NAME);
        String personaUUID = personaQN.split("/")[1];

        AtlasEntity policy = getPersonaPolicy("policy_0", subDomainQN ,personaGuid);
        String policyGuid = createEntity(policy).getCreatedEntities().get(0).getGuid();
        guidsToDelete.remove(policyGuid);
        sleep(5);


        runAsMember();

        String filterQualifiedName = getPersonaESFilterQualifiedName(personaUUID);

        assertEquals(subDomainQN, filterQualifiedName);

        AtlasEntity domainToMove = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainToMove.setAttribute(NAME, subDomain.getAttribute(NAME));
        domainToMove.setAttribute(QUALIFIED_NAME, subDomainQN);

        domainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));

        createEntity(domainToMove);
        sleep(1);

        policy = getEntity(policyGuid);
        domainToMove = getEntity(subDomainGuid);

        List<String> policyResources = (List<String>) policy.getAttribute("policyResources");

        assertEquals("entity:" + domainToMove.getAttribute(QUALIFIED_NAME), policyResources.get(0));

        assertEquals(chain(getNanoId(targetDomainQN), getNanoId(subDomainQN)), domainToMove.getAttribute(QUALIFIED_NAME));

        filterQualifiedName = getPersonaESFilterQualifiedName(personaUUID);

        assertEquals(domainToMove.getAttribute(QUALIFIED_NAME), filterQualifiedName);

        LOG.info(">> verifyPersonaESAlias();");
    }

    private static void parentToNoParent() throws Exception {
        LOG.info(">> parentToNoParent();");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        domain = getEntity(domainGuid);
        String domainName = (String) domain.getAttribute(NAME);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);

        AtlasEntity subDomain = getDomainEntity("sub_domain_0", domainQN, domainQN);
        subDomain.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(domainGuid));
        String subDomainGuid = createEntity(subDomain).getCreatedEntities().get(0).getGuid();


        sleep(1);

        subDomain = getEntity(subDomainGuid);
        String subDomainQN = (String) subDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", domainQN, domainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(domainGuid));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();

        AtlasEntity product_1 = getProductEntity("prod_1", subDomainQN, subDomainQN);
        product_1.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(subDomainGuid));
        String product_1_Guid = createEntity(product_1).getCreatedEntities().get(0).getGuid();

        sleep(1);

        /*
        * domain -> subDomain -> product_1
        *        -> product
        *
        * Move subDomain to remove parent
        *
        * */

        AtlasEntity domainToMove = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainToMove.setAttribute(NAME, subDomain.getAttribute(NAME));
        domainToMove.setAttribute(QUALIFIED_NAME, subDomainQN);
        domainToMove.setAttribute(PARENT_DOMAIN_QN, subDomain.getAttribute(PARENT_DOMAIN_QN));
        domainToMove.setAttribute(SUPER_DOMAIN_QN, subDomain.getAttribute(SUPER_DOMAIN_QN));

        domainToMove.setRelationshipAttribute(PARENT_DOMAIN, null);

        createEntity(domainToMove);

        sleep(1);

        subDomain = getEntity(subDomainGuid);
        product = getEntity(productGuid);
        product_1 = getEntity(product_1_Guid);

        String productQN = (String) product.getAttribute(QUALIFIED_NAME);
        String product_1QN = (String) product_1.getAttribute(QUALIFIED_NAME);

        assertEquals(chain(getNanoId(subDomainQN)), subDomain.getAttribute(QUALIFIED_NAME));
        assertNull(subDomain.getAttribute(PARENT_DOMAIN_QN));
        assertNull(subDomain.getAttribute(SUPER_DOMAIN_QN));
        assertNull(subDomain.getRelationshipAttribute(PARENT_DOMAIN));


        assertEquals(chain(getNanoId(domainQN)) + "/product/" + getNanoId(productQN), product.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(domainQN)), product.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(domainQN)), product.getAttribute(SUPER_DOMAIN_QN));
        assertNotNull(product.getRelationshipAttribute(DATA_DOMAIN));
        assertEquals(domainGuid, ((HashMap) product.getRelationshipAttribute(DATA_DOMAIN)).get("guid"));


        System.out.println(chain(getNanoId(subDomainQN)) + "/product/" + getNanoId(product_1QN));
        System.out.println(product_1.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(subDomainQN)) + "/product/" + getNanoId(product_1QN), product_1.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(subDomainQN)), product_1.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(subDomainQN)), product_1.getAttribute(SUPER_DOMAIN_QN));
        assertNotNull(product_1.getRelationshipAttribute(DATA_DOMAIN));
        assertEquals(subDomainGuid, ((HashMap) product_1.getRelationshipAttribute(DATA_DOMAIN)).get("guid"));


        LOG.info(">> parentToNoParent();");
    }

    private static void verifyStakeholderTitles() throws Exception {
        LOG.info(">> verifyStakeholderTitles");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        domain = getEntity(domainGuid);
        targetDomain = getEntity(targetDomainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);
        String targetDomainQN = (String) targetDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity subDomain = getDomainEntity("sub_domain_0", domainQN, domainQN);
        subDomain.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(domainGuid));
        String subDomainGuid = createEntity(subDomain).getCreatedEntities().get(0).getGuid();


        sleep(1);

        subDomain = getEntity(subDomainGuid);
        String subDomainQN = (String) subDomain.getAttribute(QUALIFIED_NAME);


        AtlasEntity domainTitle = getStakeholderTitle("domain_title_0", domainQN);
        String domainTitleGuid = createEntity(domainTitle).getCreatedEntities().get(0).getGuid();

        AtlasEntity subDomainTitle = getStakeholderTitle("sub_domain_title_0", subDomainQN);
        String subDomainTitleGuid = createEntity(subDomainTitle).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomainTitle = getStakeholderTitle("target_domain_title_0", targetDomainQN);
        String targetDomainTitleGuid = createEntity(targetDomainTitle).getCreatedEntities().get(0).getGuid();

        AtlasEntity title_4 = getStakeholderTitle("title_4", targetDomainQN, subDomainQN, domainQN);
        String title_4Guid = createEntity(title_4).getCreatedEntities().get(0).getGuid();

        AtlasEntity title_5 = getStakeholderTitle("title_5", targetDomainQN, domainQN);
        String title_5Guid = createEntity(title_5).getCreatedEntities().get(0).getGuid();

        sleep(1);

        AtlasEntity domainToMove = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainToMove.setAttribute(NAME, subDomain.getAttribute(NAME));
        domainToMove.setAttribute(QUALIFIED_NAME, subDomainQN);
        domainToMove.setAttribute(PARENT_DOMAIN_QN, subDomain.getAttribute(PARENT_DOMAIN_QN));
        domainToMove.setAttribute(SUPER_DOMAIN_QN, subDomain.getAttribute(SUPER_DOMAIN_QN));

        domainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));

        createEntity(domainToMove);

        sleep(1);

        subDomain = getEntity(subDomainGuid);
        domainTitle = getEntity(domainTitleGuid);
        subDomainTitle = getEntity(subDomainTitleGuid);
        targetDomainTitle = getEntity(targetDomainTitleGuid);
        title_4 = getEntity(title_4Guid);
        title_5 = getEntity(title_5Guid);

        List<String> qNames = (List<String>) domainTitle.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
        assertEquals(1, qNames.size());
        assertTrue(qNames.contains(domainQN));

        qNames = (List<String>) targetDomainTitle.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
        assertEquals(1, qNames.size());
        assertTrue(qNames.contains(targetDomainQN));

        qNames = (List<String>) title_5.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
        assertEquals(2, qNames.size());
        assertTrue(qNames.contains(targetDomainQN));
        assertTrue(qNames.contains(domainQN));

        qNames = (List<String>) title_4.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
        assertEquals(3, qNames.size());
        assertTrue(qNames.contains((String) subDomain.getAttribute(QUALIFIED_NAME)));
        assertTrue(qNames.contains(targetDomainQN));
        assertTrue(qNames.contains(domainQN));


        LOG.info(">> verifyStakeholderTitles");
    }

    private static void verifyStakeholdes() throws Exception {
        LOG.info(">> verifyStakeholdeTitles");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        domain = getEntity(domainGuid);
        targetDomain = getEntity(targetDomainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);
        String targetDomainQN = (String) targetDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity subDomain = getDomainEntity("sub_domain_0", domainQN, domainQN);
        subDomain.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(domainGuid));
        String subDomainGuid = createEntity(subDomain).getCreatedEntities().get(0).getGuid();


        sleep(1);

        subDomain = getEntity(subDomainGuid);
        String subDomainQN = (String) subDomain.getAttribute(QUALIFIED_NAME);


        AtlasEntity domainTitle = getStakeholderTitle("domain_title_0", domainQN);
        String domainTitleGuid = createEntity(domainTitle).getCreatedEntities().get(0).getGuid();

        AtlasEntity subDomainTitle = getStakeholderTitle("sub_domain_title_0", subDomainQN);
        String subDomainTitleGuid = createEntity(subDomainTitle).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomainTitle = getStakeholderTitle("target_domain_title_0", targetDomainQN);
        String targetDomainTitleGuid = createEntity(targetDomainTitle).getCreatedEntities().get(0).getGuid();

        AtlasEntity title_4 = getStakeholderTitle("title_4", targetDomainQN, subDomainQN, domainQN);
        String title_4Guid = createEntity(title_4).getCreatedEntities().get(0).getGuid();

        AtlasEntity title_5 = getStakeholderTitle("title_5", targetDomainQN, domainQN);
        String title_5Guid = createEntity(title_5).getCreatedEntities().get(0).getGuid();

        sleep(1);

        AtlasEntity domainStakeholder = getStakeholder("stakeholder_0", domainTitleGuid, domainGuid);
        String domainStakeholderGuid = createEntity(domainStakeholder).getCreatedEntities().get(0).getGuid();

        AtlasEntity domainStakeholderSub0 = getStakeholder("stakeholder_sub_0", title_4Guid, subDomainGuid);
        String domainStakeholderSub0Guid = createEntity(domainStakeholderSub0).getCreatedEntities().get(0).getGuid();

        AtlasEntity domainStakeholderSub1 = getStakeholder("stakeholder_sub_1", subDomainTitleGuid, subDomainGuid);
        String domainStakeholderSub1Guid = createEntity(domainStakeholderSub1).getCreatedEntities().get(0).getGuid();

        AtlasEntity domainStakeholderTarget = getStakeholder("stakeholder_target", targetDomainTitleGuid, targetDomainGuid);
        String domainStakeholderTargetGuid = createEntity(domainStakeholderTarget).getCreatedEntities().get(0).getGuid();


        AtlasEntity domainToMove = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainToMove.setAttribute(NAME, subDomain.getAttribute(NAME));
        domainToMove.setAttribute(QUALIFIED_NAME, subDomainQN);
        domainToMove.setAttribute(PARENT_DOMAIN_QN, subDomain.getAttribute(PARENT_DOMAIN_QN));
        domainToMove.setAttribute(SUPER_DOMAIN_QN, subDomain.getAttribute(SUPER_DOMAIN_QN));

        domainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));

        createEntity(domainToMove);

        sleep(3);

        subDomain = getEntity(subDomainGuid);

        domainTitle = getEntity(domainTitleGuid);
        subDomainTitle = getEntity(subDomainTitleGuid);
        targetDomainTitle = getEntity(targetDomainTitleGuid);
        title_4 = getEntity(title_4Guid);
        title_5 = getEntity(title_5Guid);

        domainStakeholder = getEntity(domainStakeholderGuid);
        domainStakeholderSub0 = getEntity(domainStakeholderSub0Guid);
        domainStakeholderSub1 = getEntity(domainStakeholderSub1Guid);
        domainStakeholderTarget = getEntity(domainStakeholderTargetGuid);

        String subDomainNewQualifiedName = (String) subDomain.getAttribute(QUALIFIED_NAME);

        List<String> qNames = (List<String>) domainTitle.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
        assertEquals(1, qNames.size());
        assertTrue(qNames.contains(domainQN));

        qNames = (List<String>) targetDomainTitle.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
        assertEquals(1, qNames.size());
        assertTrue(qNames.contains(targetDomainQN));

        qNames = (List<String>) title_5.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
        assertEquals(2, qNames.size());
        assertTrue(qNames.contains(targetDomainQN));
        assertTrue(qNames.contains(domainQN));

        qNames = (List<String>) title_4.getAttribute(ATTR_DOMAIN_QUALIFIED_NAMES);
        assertEquals(3, qNames.size());
        assertTrue(qNames.contains(subDomain.getAttribute(QUALIFIED_NAME)));
        assertTrue(qNames.contains(targetDomainQN));
        assertTrue(qNames.contains(domainQN));

        String stakeholderDomainQualifiedName = (String) domainStakeholder.getAttribute(ATTR_DOMAIN_QUALIFIED_NAME);
        assertEquals(domainQN, stakeholderDomainQualifiedName);

        stakeholderDomainQualifiedName = (String) domainStakeholderSub0.getAttribute(ATTR_DOMAIN_QUALIFIED_NAME);
        assertEquals(subDomainNewQualifiedName, stakeholderDomainQualifiedName);

        stakeholderDomainQualifiedName = (String) domainStakeholderSub1.getAttribute(ATTR_DOMAIN_QUALIFIED_NAME);
        assertEquals(subDomainNewQualifiedName, stakeholderDomainQualifiedName);

        stakeholderDomainQualifiedName = (String) domainStakeholderTarget.getAttribute(ATTR_DOMAIN_QUALIFIED_NAME);
        assertEquals(targetDomainQN, stakeholderDomainQualifiedName);


        LOG.info(">> verifyStakeholdeTitles");
    }

    private static String getPersonaESFilterQualifiedName (String personaUUID) {
        Map<String, Object> filter = getESAlias(personaUUID);
        Map<String, Object> bool = (Map<String, Object>) filter.get("bool");
        ArrayList<Map<String, Object>> should = (ArrayList<Map<String, Object>>) bool.get("should");


        for (Map<String, Object> objectMap : should) {
            if (objectMap.containsKey("terms")) {
                Map<String, Object> m = (Map<String, Object>) objectMap.get("terms");
                List<String> qNames = (List<String>) m.get("qualifiedName");
                return qNames.get(0);
            }
        }

        return null;
    }

    private static void sourceDomainToNewDomainViaRelationAPI() throws Exception {
        LOG.info(">> sourceDomainToNewDomainViaRelationAPI");


        AtlasEntity sourceDomain = getDomainEntity("source_domain_0", null, null);
        String sourceDomainGuid = createEntity(sourceDomain).getCreatedEntities().get(0).getGuid();
        String sourceDomainQN = (String) sourceDomain.getAttribute(QUALIFIED_NAME);

        sleep(1);

        AtlasEntity subDomain = getDomainEntity("sub_domain_0", sourceDomainQN, sourceDomainQN);
        subDomain.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(sourceDomainGuid));
        String subDomainGuid = createEntity(subDomain).getCreatedEntities().get(0).getGuid();
        String subDomainQN = (String) subDomain.getAttribute(QUALIFIED_NAME);
        String subDomainName = (String) subDomain.getAttribute(NAME);

        AtlasEntity targetDomain = getDomainEntity("target_domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();
        String targetDomainQN = (String) targetDomain.getAttribute(QUALIFIED_NAME);
        String targetDomainName = (String) targetDomain.getAttribute(NAME);

        sleep(1);

        AtlasRelationship relationship = new AtlasRelationship();
        relationship.setTypeName("parent_domain_sub_domains");
        relationship.setEnd1(new AtlasObjectId(targetDomainGuid, DATA_DOMAIN_TYPE));
        relationship.setEnd2(new AtlasObjectId(subDomainGuid, DATA_DOMAIN_TYPE));


        boolean failed = false;
        try {
            createRelationship(relationship);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("Mutating relationship of type parent_domain_sub_domains is not supported via relationship APIs, please use entity APIs"));
            failed = true;
        } finally {
            if (!failed) {
                throw new CustomException("This test should have failed");
            }
        }
    }

    private static void moveWithOnlyRequiredDomainPolicyPermissions () throws Exception {
        LOG.info(">> moveWithOnlyRequiredDomainPolicyPermissions");

        /*
        * Source policy - Read + Update sub-domains
        * Target policy - Read + Create sub-domains
        *
        * */

        AtlasEntity sourceDomain = getDomainEntity("source_domain_0", null, null);
        String sourceDomainGuid = createEntity(sourceDomain).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("target_domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();


        sleep(1);

        sourceDomain = getEntity(sourceDomainGuid);
        String sourceDomainQN = (String) sourceDomain.getAttribute(QUALIFIED_NAME);

        targetDomain = getEntity(targetDomainGuid);
        String targetDomainQN = (String) targetDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity subDomainToMove = getDomainEntity("sub_domain_to_move_0", sourceDomainQN, sourceDomainQN);
        subDomainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(sourceDomainGuid));
        String subDomainToMoveGuid = createEntity(subDomainToMove).getCreatedEntities().get(0).getGuid();


        runAsAdmin();
        AtlasEntity persona = getPersona("persona_0");
        persona.setAttribute("personaUsers", Arrays.asList("nikhil.member"));
        String personaGuid = createEntity(persona).getCreatedEntities().get(0).getGuid();

        sleep(1);
        subDomainToMove = getEntity(subDomainToMoveGuid);
        String subDomainQN = (String) subDomainToMove.getAttribute(QUALIFIED_NAME);

        persona = getEntity(personaGuid);

        AtlasEntity sourcePolicy = getPersonaPolicy("source_policy_0", sourceDomainQN, personaGuid);
        sourcePolicy.setAttribute("policyActions", Arrays.asList("persona-domain-read", "persona-domain-sub-domain-update"));
        String sourcePolicyGuid = createEntity(sourcePolicy).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetPolicy = getPersonaPolicy("target_policy_0", targetDomainQN, personaGuid);
        targetPolicy.setAttribute("policyActions", Arrays.asList("persona-domain-read", "persona-domain-sub-domain-create"));
        String targetPolicyGuid = createEntity(targetPolicy).getCreatedEntities().get(0).getGuid();

        guidsToDelete.remove(sourcePolicyGuid);
        guidsToDelete.remove(targetPolicyGuid);

        sleep(20);

        isRunAsMember();

        AtlasEntity domainToMove = getDomainEntity("source_domain_0", sourceDomainQN, sourceDomainQN);
        domainToMove.setAttribute(NAME, subDomainToMove.getAttribute(NAME));
        domainToMove.setAttribute(QUALIFIED_NAME, subDomainQN);

        LOG.info("Moving Domain {} from {} to {}", subDomainToMove.getAttribute(QUALIFIED_NAME), sourceDomainQN, targetDomainQN);
        domainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));

        createEntity(domainToMove);

        runAsMember();

        LOG.info("<< moveWithOnlyRequiredDomainPolicyPermissions");
    }

    private static void restoreMovedDomain() throws Exception {
        LOG.info(">> restoreMovedDomain");

        AtlasEntity sourceDomain = getDomainEntity("source_domain_1", null, null);
        String sourceDomainGuid = createEntity(sourceDomain).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("target_domain_1", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);
        sourceDomain = getEntity(sourceDomainGuid);
        targetDomain = getEntity(targetDomainGuid);
        String sourceDomainQn = (String) sourceDomain.getAttribute(QUALIFIED_NAME);
        String targetDomainQn = (String) targetDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity subDomainToMove = getDomainEntity("sub-to-move-domain_0", sourceDomainQn, sourceDomainQn);
        subDomainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(sourceDomainGuid));
        String subDomainToMoveGuid = createEntity(subDomainToMove).getCreatedEntities().get(0).getGuid();

        AtlasEntity subDomainUnderMoved = getDomainEntity("sub-under-moved-domain_0", sourceDomainQn, sourceDomainQn);
        subDomainUnderMoved.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(subDomainToMoveGuid));
        String subDomainUnderMovedGuid = createEntity(subDomainUnderMoved).getCreatedEntities().get(0).getGuid();

        sleep(1);
        subDomainToMove = getEntity(subDomainToMoveGuid);
        subDomainUnderMoved = getEntity(subDomainUnderMovedGuid);
        String subDomainToMoveQn = (String) subDomainToMove.getAttribute(QUALIFIED_NAME);
        String subDomainUnderMovedQn = (String) subDomainUnderMoved.getAttribute(QUALIFIED_NAME);

        AtlasEntity productUnderMoved = getProductEntity("prod_under_moved_0", subDomainToMoveQn, subDomainToMoveQn);
        productUnderMoved.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(subDomainToMoveGuid));
        String productUnderMovedGuid = createEntity(productUnderMoved).getCreatedEntities().get(0).getGuid();

        AtlasEntity subProductUnderMoved = getProductEntity("sub_prod_under_moved_0", subDomainUnderMovedQn, subDomainUnderMovedQn);
        subProductUnderMoved.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(subDomainUnderMovedGuid));
        String subProductUnderMovedGuid = createEntity(subProductUnderMoved).getCreatedEntities().get(0).getGuid();


        AtlasEntity subDomainPermanent = getDomainEntity("sub-permanent-domain_0", sourceDomainQn, sourceDomainQn);
        subDomainPermanent.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(sourceDomainGuid));
        String subDomainPermanentGuid = createEntity(subDomainPermanent).getCreatedEntities().get(0).getGuid();

        sleep(1);
        subDomainPermanent = getEntity(subDomainPermanentGuid);
        String subDomainPermanentQn = (String) subDomainPermanent.getAttribute(QUALIFIED_NAME);

        AtlasEntity productPermamnent = getProductEntity("prod_1", sourceDomainQn, sourceDomainQn);
        productPermamnent.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(sourceDomainGuid));
        String productPermamnentGuid = createEntity(productPermamnent).getCreatedEntities().get(0).getGuid();

        sleep(1);

        AtlasEntity domainToMove = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainToMove.setAttribute(NAME, subDomainToMove.getAttribute(NAME));
        domainToMove.setAttribute(QUALIFIED_NAME, subDomainToMove.getAttribute(QUALIFIED_NAME));
        domainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));

        createEntity(domainToMove);
        sleep(1);

        deleteEntitySoft(subDomainToMoveGuid);
        sleep(1);
        subDomainToMove = getEntity(subDomainToMoveGuid);

        assertEquals(DELETED, subDomainToMove.getStatus());

        AtlasEntity domainToActivate = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainToActivate.setAttribute(NAME, subDomainToMove.getAttribute(NAME));
        domainToActivate.setAttribute(QUALIFIED_NAME, subDomainToMove.getAttribute(QUALIFIED_NAME));
        domainToActivate.setStatus(AtlasEntity.Status.ACTIVE);

        createEntity(domainToActivate);

        sleep(1);

        sourceDomain = getEntity(sourceDomainGuid);
        targetDomain = getEntity(targetDomainGuid);

        subDomainToMove = getEntity(subDomainToMoveGuid);
        subDomainUnderMoved = getEntity(subDomainUnderMovedGuid);

        subDomainPermanent = getEntity(subDomainPermanentGuid);

        assertEquals(ACTIVE, subDomainToMove.getStatus());

        assertEquals(chain(getNanoId(targetDomainQn)) + "/domain/" + getNanoId(subDomainToMoveQn), subDomainToMove.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainQn)), subDomainToMove.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainQn)), subDomainToMove.getAttribute(SUPER_DOMAIN_QN));
        assertNotNull(subDomainToMove.getRelationshipAttribute(PARENT_DOMAIN));
        Map<String, Object> relation = (Map) subDomainToMove.getRelationshipAttribute(PARENT_DOMAIN);
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relation.get("relationshipStatus"));
        assertEquals(targetDomainGuid, relation.get("guid"));

        List<Map<String, Object>> relations = (List<Map<String, Object>>) subDomainToMove.getRelationshipAttribute(SUB_DOMAINS);
        assertEquals(1, relations.size());
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relations.get(0).get("relationshipStatus"));
        assertEquals(subDomainUnderMovedGuid, relations.get(0).get("guid"));

        relations = (List<Map<String, Object>>) subDomainToMove.getRelationshipAttribute(DATA_PRODUCTS);
        assertEquals(1, relations.size());
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relations.get(0).get("relationshipStatus"));
        assertEquals(productUnderMovedGuid, relations.get(0).get("guid"));

        assertTrue(CollectionUtils.isEmpty((List) subDomainUnderMoved.getRelationshipAttribute(SUB_DOMAINS)));

        relations = (List<Map<String, Object>>) subDomainUnderMoved.getRelationshipAttribute(DATA_PRODUCTS);
        assertEquals(1, relations.size());
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relations.get(0).get("relationshipStatus"));
        assertEquals(subProductUnderMovedGuid, relations.get(0).get("guid"));

        assertTrue(CollectionUtils.isEmpty((List) subDomainPermanent.getRelationshipAttribute(SUB_DOMAINS)));
        assertTrue(CollectionUtils.isEmpty((List) subDomainPermanent.getRelationshipAttribute(DATA_PRODUCTS)));



        relations = (List<Map<String, Object>>) sourceDomain.getRelationshipAttribute(DATA_PRODUCTS);
        assertEquals(1, relations.size());
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relations.get(0).get("relationshipStatus"));
        assertEquals(productPermamnentGuid, relations.get(0).get("guid"));

        relations = (List<Map<String, Object>>) sourceDomain.getRelationshipAttribute(SUB_DOMAINS);
        assertEquals(1, relations.size());
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relations.get(0).get("relationshipStatus"));
        assertEquals(subDomainPermanentGuid, relations.get(0).get("guid"));

        assertNotNull(targetDomain.getRelationshipAttribute(SUB_DOMAINS));
        relations = (List) targetDomain.getRelationshipAttribute(SUB_DOMAINS);
        assertEquals(1, relations.size());
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relations.get(0).get("relationshipStatus"));
        assertEquals(subDomainToMoveGuid, relations.get(0).get("guid"));


        // move back to source

        domainToMove = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainToMove.setAttribute(NAME, subDomainToMove.getAttribute(NAME));
        domainToMove.setAttribute(QUALIFIED_NAME, subDomainToMove.getAttribute(QUALIFIED_NAME));
        domainToMove.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(sourceDomainGuid));

        createEntity(domainToMove);
        sleep(1);

        deleteEntitySoft(subDomainToMoveGuid);
        sleep(1);
        subDomainToMove = getEntity(subDomainToMoveGuid);

        assertEquals(DELETED, subDomainToMove.getStatus());

        domainToActivate = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainToActivate.setAttribute(NAME, subDomainToMove.getAttribute(NAME));
        domainToActivate.setAttribute(QUALIFIED_NAME, subDomainToMove.getAttribute(QUALIFIED_NAME));
        domainToActivate.setStatus(AtlasEntity.Status.ACTIVE);

        createEntity(domainToActivate);

        sleep(1);
        subDomainToMove = getEntity(subDomainToMoveGuid);
        sourceDomain = getEntity(sourceDomainGuid);
        targetDomain = getEntity(targetDomainGuid);
        subDomainUnderMoved = getEntity(subDomainUnderMovedGuid);
        subDomainPermanent = getEntity(subDomainPermanentGuid);

        assertEquals(ACTIVE, subDomainToMove.getStatus());

        assertEquals(chain(getNanoId(sourceDomainQn)) + "/domain/" + getNanoId(subDomainToMoveQn), subDomainToMove.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(sourceDomainQn)), subDomainToMove.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(sourceDomainQn)), subDomainToMove.getAttribute(SUPER_DOMAIN_QN));
        assertNotNull(subDomainToMove.getRelationshipAttribute(PARENT_DOMAIN));
        relation = (Map) subDomainToMove.getRelationshipAttribute(PARENT_DOMAIN);
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relation.get("relationshipStatus"));
        assertEquals(sourceDomainGuid, relation.get("guid"));

        relations = (List<Map<String, Object>>) subDomainToMove.getRelationshipAttribute(SUB_DOMAINS);
        assertEquals(1, relations.size());
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relations.get(0).get("relationshipStatus"));
        assertEquals(subDomainUnderMovedGuid, relations.get(0).get("guid"));

        relations = (List<Map<String, Object>>) subDomainToMove.getRelationshipAttribute(DATA_PRODUCTS);
        assertEquals(1, relations.size());
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relations.get(0).get("relationshipStatus"));
        assertEquals(productUnderMovedGuid, relations.get(0).get("guid"));

        assertTrue(CollectionUtils.isEmpty((List) subDomainUnderMoved.getRelationshipAttribute(SUB_DOMAINS)));

        relations = (List<Map<String, Object>>) subDomainUnderMoved.getRelationshipAttribute(DATA_PRODUCTS);
        assertEquals(1, relations.size());
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relations.get(0).get("relationshipStatus"));
        assertEquals(subProductUnderMovedGuid, relations.get(0).get("guid"));

        assertTrue(CollectionUtils.isEmpty((List) subDomainPermanent.getRelationshipAttribute(SUB_DOMAINS)));
        assertTrue(CollectionUtils.isEmpty((List) subDomainPermanent.getRelationshipAttribute(DATA_PRODUCTS)));



        relations = (List<Map<String, Object>>) sourceDomain.getRelationshipAttribute(DATA_PRODUCTS);
        assertEquals(1, relations.size());
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relations.get(0).get("relationshipStatus"));
        assertEquals(productPermamnentGuid, relations.get(0).get("guid"));

        relations = (List<Map<String, Object>>) sourceDomain.getRelationshipAttribute(SUB_DOMAINS);
        assertEquals(2, relations.size());
        assertEquals(2, relations.stream().filter(x -> AtlasRelationship.Status.ACTIVE.name().equals(x.get("relationshipStatus"))).count());
        Set<String> guids = relations.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());
        assertTrue(guids.contains(subDomainPermanentGuid));
        assertTrue(guids.contains(subDomainToMoveGuid));

        assertTrue(CollectionUtils.isEmpty((Collection) targetDomain.getRelationshipAttribute(SUB_DOMAINS)));

        LOG.info(">> restoreMovedDomain");
    }
}