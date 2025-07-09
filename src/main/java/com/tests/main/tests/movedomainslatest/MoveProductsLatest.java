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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.tests.main.tests.glossary.tests.TestsRunner.guidsToDelete;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.DATA_DOMAIN;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.DATA_DOMAIN_TYPE;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.DATA_PRODUCTS;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.DATA_PRODUCT_TYPE;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.PARENT_DOMAIN;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.PARENT_DOMAIN_QN;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.SUPER_DOMAIN_QN;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.chain;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getDomainAsRelation;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getDomainEntity;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getProductAsRelation;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getProductEntity;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getNanoId;
import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.createRelationship;
import static com.tests.main.utils.TestUtil.deleteEntitySoft;
import static com.tests.main.utils.TestUtil.deleteRelationshipByGuid;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.runAsAdmin;
import static com.tests.main.utils.TestUtil.runAsGod;
import static com.tests.main.utils.TestUtil.sleep;
import static com.tests.main.utils.TestUtil.updateRelationship;
import static org.apache.atlas.model.instance.AtlasEntity.Status.ACTIVE;
import static org.apache.atlas.model.instance.AtlasEntity.Status.DELETED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class MoveProductsLatest implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(MoveProductsLatest.class);

    public static void main(String[] args) throws Exception {
        try {
            new MoveProductsLatest().run();
        } finally {
            runAsGod();
            cleanUpAll();
            ESUtil.close();
            runAsAdmin();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running MoveProducts tests");

        long start = System.currentTimeMillis();
        try {
            //NOT for tenant
            noParentDomain();

            sourceDomainToNoDomain();

            sourceDomainToNewDomain();

            sourceNestedDomainToNewNestedDomain();

            sourceDomainToNewDomainViaDomainUpdate();

            domainWithSameNameAtSameLevel();

            domainWithSameNameAtDifferentLevel();

            createProductWithoutDenorAttrs();

            //Parent Domain Qualified Name cannot be empty or null
            sourceDomainToNewDomainViaRelationAPI();

            restoreMovedProduct();

        } finally {
            LOG.info("Completed running MoveProducts tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private static void noParentDomain() throws Exception {
        LOG.info(">> noParentDomain");


        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        domain = getEntity(domainGuid);
        String domainQn = (String) domain.getAttribute(QUALIFIED_NAME);

        AtlasEntity orphanProduct = getProductEntity("prod_0", domainQn, domainQn);

        boolean failed = false;
        try {
            createEntity(orphanProduct).getCreatedEntities().get(0).getGuid();
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 409);
            assertTrue(exception.getMessage().contains("Operation not supported: Cannot create a Product without a Domain Relationship"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        //sleep(2);

        orphanProduct.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(domainGuid));
        String productGuid = createEntity(orphanProduct).getCreatedEntities().get(0).getGuid();

        sleep(1);

        orphanProduct = getEntity(productGuid);
        String productQn = (String) orphanProduct.getAttribute(QUALIFIED_NAME);

        assertEquals(chain(getNanoId(domainQn)).concat("/product/").concat(getNanoId(productQn)) , orphanProduct.getAttribute(QUALIFIED_NAME));

        LOG.info(">> noParentDomain");
    }

    private static void sourceDomainToNoDomain() throws Exception {
        LOG.info(">> sourceDomainToNoDomain");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();
        String domainName = (String) domain.getAttribute(NAME);

        sleep(1);
        domain = getEntity(domainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", domainQN, domainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(domainGuid));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();
        String productName = (String) product.getAttribute(NAME);

        sleep(1);
        product = getEntity(productGuid);
        String productQn = (String) product.getAttribute(QUALIFIED_NAME);

        assertEquals(chain(getNanoId(domainQN)) + "/product/" + getNanoId(productQn) , product.getAttribute(QUALIFIED_NAME));


        AtlasEntity orphanProduct = new AtlasEntity(DATA_PRODUCT_TYPE);
        orphanProduct.setAttribute(NAME, productName);
        orphanProduct.setAttribute(QUALIFIED_NAME, productQn);
        orphanProduct.setAttribute(PARENT_DOMAIN_QN, null);
        orphanProduct.setAttribute(SUPER_DOMAIN_QN, null);

        orphanProduct.setRelationshipAttribute(DATA_DOMAIN, null);


        boolean failed = false;
        try {
            createEntity(orphanProduct);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("DataProduct can only be moved to another Domain"));
            failed = true;
        } finally {
            if (!failed) {
                throw new CustomException("This test should have failed");
            }
        }

        assertEquals(chain(getNanoId(domainQN)) + "/product/" + getNanoId(productQn) , product.getAttribute(QUALIFIED_NAME));

        LOG.info(">> sourceDomainToNoDomain");
    }

    private static void sourceDomainToNewDomain() throws Exception {
        LOG.info(">> sourceDomainToNewDomain");

        AtlasEntity sourceDomain = getDomainEntity("source_domain_1", null, null);
        String sourceDomainGuid = createEntity(sourceDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);
        sourceDomain = getEntity(sourceDomainGuid);
        String sourceDomainQn = (String) sourceDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_1", sourceDomainQn, sourceDomainQn);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(sourceDomainGuid));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();


        AtlasEntity targetDomain = getDomainEntity("target_domain_1", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();


        sleep(1);
        product = getEntity(productGuid);
        targetDomain = getEntity(targetDomainGuid);

        String productQN = (String) product.getAttribute(QUALIFIED_NAME);
        String targetDomainQn = (String) targetDomain.getAttribute(QUALIFIED_NAME);

        assertEquals(chain(getNanoId(sourceDomainQn)) + "/product/" + getNanoId(productQN) , product.getAttribute(QUALIFIED_NAME));


        AtlasEntity newProduct = new AtlasEntity(DATA_PRODUCT_TYPE);
        newProduct.setAttribute(NAME, product.getAttribute(NAME));
        newProduct.setAttribute(QUALIFIED_NAME, product.getAttribute(QUALIFIED_NAME));
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(targetDomainGuid));

        createEntity(product);
        sleep(1);

        product = getEntity(productGuid);
        sourceDomain = getEntity(sourceDomainGuid);
        targetDomain = getEntity(targetDomainGuid);

        assertEquals(chain(getNanoId(targetDomainQn)) + "/product/" + getNanoId(productQN) , product.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainQn)), product.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainQn)), product.getAttribute(SUPER_DOMAIN_QN));

        assertTrue(CollectionUtils.isEmpty((Collection) sourceDomain.getRelationshipAttribute(DATA_PRODUCTS)));

        Map<String, Object> rel = (Map<String, Object>) product.getRelationshipAttribute(DATA_DOMAIN);

        assertEquals(AtlasRelationship.Status.ACTIVE.name(), rel.get("relationshipStatus"));

        LOG.info(">> sourceDomainToNewDomain");
    }

    private static void sourceNestedDomainToNewNestedDomain() throws Exception {
        LOG.info(">> sourceDomainToNewDomain");

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

        //createEntity(sourceDomainNested);
        //createEntity(targetDomainNested);

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

        AtlasEntity product = getProductEntity("prod_1", sourceDomainNestedQN1, sourceDomainParentQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(sourceDomainNestedGuid1));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();

        sleep(1);
        product = getEntity(productGuid);
        String productQN = (String) product.getAttribute(QUALIFIED_NAME);


        assertEquals(chain(getNanoId(sourceDomainParentQN), getNanoId(sourceDomainNestedQN), getNanoId(sourceDomainNestedQN1)) + "/product/" + getNanoId(productQN),
                product.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(sourceDomainParentQN), getNanoId(sourceDomainNestedQN), getNanoId(sourceDomainNestedQN1)), product.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(sourceDomainParentQN)), product.getAttribute(SUPER_DOMAIN_QN));

        /*
         *  sourceDomainParent -> sourceDomainNested -> sourceDomainNested1 -> product
         *
         *
         *  targetDomainParent -> targetDomainNested -> targetDomainNested1
         *
         *  Move product to targetDomainNested1
         *
         * */

        AtlasEntity newProduct = new AtlasEntity(DATA_PRODUCT_TYPE);
        newProduct.setAttribute(NAME, product.getAttribute(NAME));
        newProduct.setAttribute(QUALIFIED_NAME, product.getAttribute(QUALIFIED_NAME));
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(targetDomainNestedGuid1));

        createEntity(product);
        sleep(1);

        product = getEntity(productGuid);
        sourceDomainNested1 = getEntity(sourceDomainNestedGuid1);

        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1)) + "/product/" + getNanoId(productQN),
                product.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainParentQN), getNanoId(targetDomainNestedQN), getNanoId(targetDomainNestedQN1)),
                product.getAttribute(PARENT_DOMAIN_QN));

        assertEquals(chain(getNanoId(targetDomainParentQN)), product.getAttribute(SUPER_DOMAIN_QN));

        assertTrue(CollectionUtils.isEmpty((Collection) sourceDomainNested1.getRelationshipAttribute(DATA_PRODUCTS)));

        assertNotNull(product.getRelationshipAttribute(DATA_DOMAIN));
        Map<String, Object> rel = (Map<String, Object>) product.getRelationshipAttribute(DATA_DOMAIN);

        assertEquals(AtlasRelationship.Status.ACTIVE.name(), rel.get("relationshipStatus"));

        LOG.info(">> sourceDomainToNewDomain");
    }

    private static void sourceDomainToNewDomainViaDomainUpdate() throws Exception {
        LOG.info(">> sourceDomainToNewDomainViaDomainUpdate");

        AtlasEntity sourceDomain = getDomainEntity("source_domain_1", null, null);
        String sourceDomainGuid = createEntity(sourceDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);
        sourceDomain = getEntity(sourceDomainGuid);
        String sourceDomainQn = (String) sourceDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_1", sourceDomainQn, sourceDomainQn);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(sourceDomainGuid));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("target_domain_1", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();


        sleep(2);

        product = getEntity(productGuid);
        targetDomain = getEntity(targetDomainGuid);

        String targetDomainQn = (String) targetDomain.getAttribute(QUALIFIED_NAME);
        String productQn = (String) product.getAttribute(QUALIFIED_NAME);

        assertEquals(chain(getNanoId(sourceDomainQn)) + "/product/" + getNanoId(productQn) , product.getAttribute(QUALIFIED_NAME));


        AtlasEntity newDomain = new AtlasEntity(DATA_DOMAIN_TYPE);
        newDomain.setAttribute(NAME, targetDomain.getAttribute(NAME));
        newDomain.setAttribute(QUALIFIED_NAME, targetDomainQn);
        newDomain.setRelationshipAttribute(DATA_PRODUCTS, getProductAsRelation(product.getGuid()));

        boolean failed = false;
        try {
            createEntity(newDomain);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("Cannot update Domain's subDomains or dataProducts relations"));
            failed = true;
        } finally {
            if (!failed) {
                throw new CustomException("This test should have failed");
            }
        }

        LOG.info(">> sourceDomainToNewDomainViaDomainUpdate");
    }

    private static void sourceDomainToNewDomainViaRelationAPI() throws Exception {
        LOG.info(">> sourceDomainToNewDomainViaRelationAPI");

        AtlasEntity sourceDomain = getDomainEntity("source_domain_0", null, null);
        String sourceDomainGuid = createEntity(sourceDomain).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("sourcetarget_domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);
        sourceDomain = getEntity(sourceDomainGuid);
        String sourceDomainQn = (String) sourceDomain.getAttribute(QUALIFIED_NAME);


        AtlasEntity product = getProductEntity("prod_0", sourceDomainQn, sourceDomainQn);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(sourceDomainGuid));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();

        sleep(1);
        product = getEntity(productGuid);

        AtlasRelationship relationship = new AtlasRelationship();
        relationship.setTypeName("data_domain_data_products");
        relationship.setEnd1(new AtlasObjectId(targetDomainGuid, DATA_DOMAIN_TYPE));
        relationship.setEnd2(new AtlasObjectId(productGuid, DATA_PRODUCT_TYPE));


        boolean failed = false;
        try {
            createRelationship(relationship);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("Mutating relationship of type data_domain_data_products is not supported via relationship APIs, please use entity APIs"));
            failed = true;
        } finally {
            if (!failed) {
                throw new CustomException("This test should have failed");
            }
        }


        //----------------


        sourceDomain = getDomainEntity("source_domain_1", null, null);
        sourceDomainGuid = createEntity(sourceDomain).getCreatedEntities().get(0).getGuid();
        sourceDomainQn = (String) sourceDomain.getAttribute(QUALIFIED_NAME);

        product = getProductEntity("prod_1", sourceDomainQn, sourceDomainQn);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(sourceDomainGuid));
        productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();

        sleep(1);

        product = getEntity(productGuid);
        String relGuid = (String) ((HashMap<String, Object>) product.getRelationshipAttribute(DATA_DOMAIN)).get("relationshipGuid");

        relationship = new AtlasRelationship();
        relationship.setGuid(relGuid);
        relationship.setTypeName("data_domain_data_products");
        relationship.setEnd1(new AtlasObjectId(sourceDomainGuid, DATA_DOMAIN_TYPE));
        relationship.setEnd2(new AtlasObjectId(productGuid, DATA_PRODUCT_TYPE));


        failed = false;
        try {
            updateRelationship(relationship);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("Mutating relationship of type data_domain_data_products is not supported via relationship APIs, please use entity APIs"));
            failed = true;
        } finally {
            if (!failed) {
                throw new CustomException("This test should have failed");
            }
        }

        //sleep(1);

        product = getEntity(productGuid);

        assertNotNull(product.getRelationshipAttribute(DATA_DOMAIN));
        assertEquals(sourceDomainGuid, ((HashMap) product.getRelationshipAttribute(DATA_DOMAIN)).get("guid"));

        failed = false;
        try {
            deleteRelationshipByGuid((String) ((HashMap) product.getRelationshipAttribute(DATA_DOMAIN)).get("relationshipGuid"));
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("Mutating relationship of type data_domain_data_products is not supported via relationship APIs, please use entity APIs"));
            failed = true;
        } finally {
            if (!failed) {
                throw new CustomException("This test should have failed");
            }
        }

        //sleep(2);

        product = getEntity(productGuid);

        assertNotNull(product.getRelationshipAttribute(DATA_DOMAIN));
        assertEquals(sourceDomainGuid, ((HashMap) product.getRelationshipAttribute(DATA_DOMAIN)).get("guid"));

        LOG.info(">> sourceDomainToNewDomainViaRelationAPI");
    }


    private static void domainWithSameNameAtSameLevel() throws Exception {
        LOG.info(">> domainWithSameNameAtSameLevel();");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("target_domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);

        domain = getEntity(domainGuid);
        targetDomain = getEntity(targetDomainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);
        String targetDomainQN = (String) targetDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity sourceProduct = getProductEntity("source_prod_0", domainQN, domainQN);
        sourceProduct.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(domainGuid));
        String sourceProductGuid = createEntity(sourceProduct).getCreatedEntities().get(0).getGuid();


        AtlasEntity targetProduct = getProductEntity("target_prod_0", targetDomainQN, targetDomainQN);
        targetProduct.setAttribute(NAME, sourceProduct.getAttribute(NAME));
        targetProduct.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(targetDomainGuid));
        String targetProductGuid = createEntity(targetProduct).getCreatedEntities().get(0).getGuid();


        sleep(1);
        sourceProduct = getEntity(sourceProductGuid);
        targetProduct = getEntity(targetProductGuid);

        String sourceProductQN = (String) sourceProduct.getAttribute(QUALIFIED_NAME);

        AtlasEntity productToMove = new AtlasEntity(DATA_PRODUCT_TYPE);
        productToMove.setAttribute(NAME, sourceProduct.getAttribute(NAME));
        productToMove.setAttribute(QUALIFIED_NAME, sourceProductQN);

        productToMove.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(targetDomainGuid));

        sleep(1);

        boolean failed = false;
        try {
            createEntity(productToMove);
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
        String targetDomainName = (String) targetDomain.getAttribute(NAME);

        sleep(1);
        domain = getEntity(domainGuid);
        targetDomain = getEntity(targetDomainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);
        String targetDomainQN = (String) targetDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity subDomain = getDomainEntity("sub_domain", domainQN, domainQN);
        subDomain.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(domainGuid));
        String subDomainGuid = createEntity(subDomain).getCreatedEntities().get(0).getGuid();

        AtlasEntity subDomain_0 = getDomainEntity("sub_domain_0", targetDomainQN, targetDomainQN);
        subDomain_0.setRelationshipAttribute(PARENT_DOMAIN, getDomainAsRelation(targetDomainGuid));
        String subDomain_0Guid = createEntity(subDomain_0).getCreatedEntities().get(0).getGuid();

        AtlasEntity sourceProduct = getProductEntity("source_prod_0", domainQN, domainQN);
        sourceProduct.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(domainGuid));
        String sourceProductGuid = createEntity(sourceProduct).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetProduct = getProductEntity("target_prod_0", targetDomainQN, targetDomainQN);
        targetProduct.setAttribute(NAME, sourceProduct.getAttribute(NAME));
        targetProduct.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(targetDomainGuid));
        String targetProductGuid = createEntity(targetProduct).getCreatedEntities().get(0).getGuid();

        sleep(2);
        subDomain = getEntity(subDomainGuid);
        subDomain_0 = getEntity(subDomain_0Guid);
        sourceProduct = getEntity(sourceProductGuid);
        targetProduct = getEntity(targetProductGuid);

        String subDomainQN = (String) subDomain.getAttribute(QUALIFIED_NAME);
        String subDomainName = (String) subDomain.getAttribute(NAME);
        String subDomain_0QN = (String) subDomain_0.getAttribute(QUALIFIED_NAME);
        String subDomain_0Name = (String) subDomain_0.getAttribute(NAME);
        String sourceProductQN = (String) sourceProduct.getAttribute(QUALIFIED_NAME);
        String targetProductQN = (String) targetProduct.getAttribute(QUALIFIED_NAME);


        AtlasEntity productToMove = new AtlasEntity(DATA_PRODUCT_TYPE);
        productToMove.setAttribute(NAME, sourceProduct.getAttribute(NAME));
        productToMove.setAttribute(QUALIFIED_NAME, sourceProductQN);
        productToMove.setAttribute(PARENT_DOMAIN_QN, sourceProduct.getAttribute(PARENT_DOMAIN_QN));
        productToMove.setAttribute(SUPER_DOMAIN_QN, sourceProduct.getAttribute(SUPER_DOMAIN_QN));

        productToMove.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(subDomain_0Guid));

        createEntity(productToMove);

        sleep(1);

        productToMove = getEntity(sourceProductGuid);

        assertEquals(chain(getNanoId(targetDomainQN), getNanoId(subDomain_0QN)) + "/product/" + getNanoId(sourceProductQN), productToMove.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainQN), getNanoId(subDomain_0QN)), productToMove.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainQN)), productToMove.getAttribute(SUPER_DOMAIN_QN));

        assertNotNull(productToMove.getRelationshipAttribute(DATA_DOMAIN));
        assertEquals(subDomain_0Guid, ((HashMap) productToMove.getRelationshipAttribute(DATA_DOMAIN)).get("guid"));

        LOG.info(">> domainWithSameNameAtDifferentLevel();");
    }

    private static void createProductWithoutDenorAttrs() throws Exception {
        LOG.info(">> createProductWithoutDenorAttrs();");

        AtlasEntity domain = getDomainEntity("domain_0", null, null);
        String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

        sleep(1);
        domain = getEntity(domainGuid);
        String domainQN = (String) domain.getAttribute(QUALIFIED_NAME);

        AtlasEntity prod = getProductEntity("prod_0", null, null);
        String newProddQN = domainQN + "/" + prod.getAttribute(QUALIFIED_NAME);
        prod.setAttribute(QUALIFIED_NAME, newProddQN);
        prod.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(domainGuid));
        String sourceProductGuid = createEntity(prod).getCreatedEntities().get(0).getGuid();

        sleep(1);
        prod = getEntity(sourceProductGuid);
        String prodQN = (String) prod.getAttribute(QUALIFIED_NAME);

        assertEquals(chain(getNanoId(domainQN))  + "/product/" + getNanoId(prodQN) , prod.getAttribute(QUALIFIED_NAME));
        assertNotNull(prod.getRelationshipAttribute(DATA_DOMAIN));
        assertEquals(domainQN, prod.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(domainQN, prod.getAttribute(SUPER_DOMAIN_QN));


        LOG.info(">> createProductWithoutDenorAttrs();");
    }

    private static void restoreMovedProduct() throws Exception {
        LOG.info(">> sourceDomainToNewDomain");

        AtlasEntity sourceDomain = getDomainEntity("source_domain_1", null, null);
        String sourceDomainGuid = createEntity(sourceDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);
        sourceDomain = getEntity(sourceDomainGuid);
        String sourceDomainQn = (String) sourceDomain.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_1", sourceDomainQn, sourceDomainQn);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(sourceDomainGuid));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();


        AtlasEntity targetDomain = getDomainEntity("target_domain_1", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);
        product = getEntity(productGuid);
        targetDomain = getEntity(targetDomainGuid);

        String productQN = (String) product.getAttribute(QUALIFIED_NAME);
        String targetDomainQn = (String) targetDomain.getAttribute(QUALIFIED_NAME);

        assertEquals(chain(getNanoId(sourceDomainQn)) + "/product/" + getNanoId(productQN) , product.getAttribute(QUALIFIED_NAME));


        AtlasEntity productToMove = new AtlasEntity(DATA_PRODUCT_TYPE);
        productToMove.setAttribute(NAME, product.getAttribute(NAME));
        productToMove.setAttribute(QUALIFIED_NAME, product.getAttribute(QUALIFIED_NAME));
        productToMove.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(targetDomainGuid));

        createEntity(productToMove);
        sleep(1);

        deleteEntitySoft(productGuid);
        sleep(1);
        product = getEntity(productGuid);

        assertEquals(DELETED, product.getStatus());

        AtlasEntity productToActivate = new AtlasEntity(DATA_PRODUCT_TYPE);
        productToActivate.setAttribute(NAME, product.getAttribute(NAME));
        productToActivate.setAttribute(QUALIFIED_NAME, product.getAttribute(QUALIFIED_NAME));
        productToActivate.setStatus(AtlasEntity.Status.ACTIVE);

        createEntity(productToActivate);
        sleep(1);

        product = getEntity(productGuid);
        sourceDomain = getEntity(sourceDomainGuid);
        targetDomain = getEntity(targetDomainGuid);

        assertEquals(ACTIVE, productToActivate.getStatus());

        assertEquals(chain(getNanoId(targetDomainQn)) + "/product/" + getNanoId(productQN) , product.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(targetDomainQn)), product.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(targetDomainQn)), product.getAttribute(SUPER_DOMAIN_QN));

        assertTrue(CollectionUtils.isEmpty((Collection) sourceDomain.getRelationshipAttribute(DATA_PRODUCTS)));

        assertNotNull(targetDomain.getRelationshipAttribute(DATA_PRODUCTS));
        Map<String, Object> rel = (Map<String, Object>) ((List) targetDomain.getRelationshipAttribute(DATA_PRODUCTS)).get(0);
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), rel.get("relationshipStatus"));

        assertNotNull(product.getRelationshipAttribute(DATA_DOMAIN));
        Map<String, Object> relations = (Map) product.getRelationshipAttribute(DATA_DOMAIN);
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relations.get("relationshipStatus"));

        // move back to source

        productToMove = new AtlasEntity(DATA_PRODUCT_TYPE);
        productToMove.setAttribute(NAME, product.getAttribute(NAME));
        productToMove.setAttribute(QUALIFIED_NAME, product.getAttribute(QUALIFIED_NAME));
        productToMove.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(sourceDomainGuid));

        createEntity(productToMove);
        sleep(1);

        deleteEntitySoft(productGuid);
        sleep(1);
        product = getEntity(productGuid);

        assertEquals(DELETED, product.getStatus());

        productToActivate = new AtlasEntity(DATA_PRODUCT_TYPE);
        productToActivate.setAttribute(NAME, product.getAttribute(NAME));
        productToActivate.setAttribute(QUALIFIED_NAME, product.getAttribute(QUALIFIED_NAME));
        productToActivate.setStatus(AtlasEntity.Status.ACTIVE);

        createEntity(productToActivate);
        sleep(1);

        product = getEntity(productGuid);
        sourceDomain = getEntity(sourceDomainGuid);
        targetDomain = getEntity(targetDomainGuid);

        assertEquals(ACTIVE, productToActivate.getStatus());

        product = getEntity(productGuid);
        sourceDomain = getEntity(sourceDomainGuid);
        targetDomain = getEntity(targetDomainGuid);

        assertEquals(chain(getNanoId(sourceDomainQn)) + "/product/" + getNanoId(productQN) , product.getAttribute(QUALIFIED_NAME));
        assertEquals(chain(getNanoId(sourceDomainQn)), product.getAttribute(PARENT_DOMAIN_QN));
        assertEquals(chain(getNanoId(sourceDomainQn)), product.getAttribute(SUPER_DOMAIN_QN));

        assertTrue(CollectionUtils.isEmpty((Collection) targetDomain.getRelationshipAttribute(DATA_PRODUCTS)));

        assertNotNull(sourceDomain.getRelationshipAttribute(DATA_PRODUCTS));
        rel = (Map<String, Object>) ((List) sourceDomain.getRelationshipAttribute(DATA_PRODUCTS)).get(0);
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), rel.get("relationshipStatus"));

        assertNotNull(product.getRelationshipAttribute(DATA_DOMAIN));
        relations = (Map) product.getRelationshipAttribute(DATA_DOMAIN);
        assertEquals(AtlasRelationship.Status.ACTIVE.name(), relations.get("relationshipStatus"));

        LOG.info(">> sourceDomainToNewDomain");
    }

    protected static void moveWithSpecificPermissions() throws Exception {
        LOG.info(">> moveWithSpecificPermissions");

        /*runAsGod();

        AtlasEntity sourcePersona = getPersona("persona_source_0");
        String sourcePersonaGuid = createEntity(sourcePersona).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetPersona = getPersona("persona_target_0");
        String targetPersonaGuid = createEntity(targetPersona).getCreatedEntities().get(0).getGuid();

        AtlasEntity sourceDomain = getDomainEntity("source_domain_0", null, null);
        String sourceDomainGuid = createEntity(sourceDomain).getCreatedEntities().get(0).getGuid();

        AtlasEntity targetDomain = getDomainEntity("target_domain_0", null, null);
        String targetDomainGuid = createEntity(targetDomain).getCreatedEntities().get(0).getGuid();

        sleep(1);
        sourceDomain = getEntity(sourceDomainGuid);
        targetDomain = getEntity(targetDomainGuid);

        AtlasEntity policy = getPersonaPolicy("policy_0", subDomainQN ,personaGuid);
        String policyGuid = createEntity(policy).getCreatedEntities().get(0).getGuid();
        guidsToDelete.remove(policyGuid);
        sleep(5);

        sleep(20);
        runAsMember();*/

        LOG.info("<< moveWithSpecificPermissions");
    }
}
