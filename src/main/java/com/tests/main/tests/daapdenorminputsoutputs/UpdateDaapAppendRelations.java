package com.tests.main.tests.daapdenorminputsoutputs;


import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.TestUtil;
import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.tests.main.tests.daapdenorminputsoutputs.DaapDenormAttrsUtils.ATTR_INPUTS;
import static com.tests.main.tests.daapdenorminputsoutputs.DaapDenormAttrsUtils.ATTR_OUTPUTS;
import static com.tests.main.tests.daapdenorminputsoutputs.DaapDenormAttrsUtils.REL_INPUTS;
import static com.tests.main.tests.daapdenorminputsoutputs.DaapDenormAttrsUtils.REL_INPUT_PRODUCTS;
import static com.tests.main.tests.daapdenorminputsoutputs.DaapDenormAttrsUtils.REL_OUTPUTS;
import static com.tests.main.tests.daapdenorminputsoutputs.DaapDenormAttrsUtils.REL_OUTPUT_PRODUCTS;
import static com.tests.main.tests.daapdenorminputsoutputs.DaapDenormAttrsUtils.copyOfProduct;
import static com.tests.main.tests.daapdenorminputsoutputs.DaapDenormAttrsUtils.createAndGetSuperDomain;
import static com.tests.main.tests.daapdenorminputsoutputs.DaapDenormAttrsUtils.getAssetsAsRelations;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.DATA_DOMAIN;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.DATA_PRODUCT_TYPE;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getDomainAsRelation;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getProductEntity;
import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.cleanUpAll;
import static com.tests.main.utils.TestUtil.cleanUpAllForEach;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getAtlasEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.getRandomName;
import static com.tests.main.utils.TestUtil.setAtlasClient;
import static com.tests.main.utils.TestUtil.sleep;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class UpdateDaapAppendRelations implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(UpdateDaapAppendRelations.class);

    private static AtlasEntity DOMAIN, TABLE_0, TABLE_1, TABLE_2, TABLE_3;

    public static void main(String[] args) throws Exception {
        try {
            new UpdateDaapAppendRelations().run();
        } finally {
            cleanUpAllForEach();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws Exception {
        LOG.info("Running UpdateDaapAppendRelations tests");

        long start = System.currentTimeMillis();
        try {
            //setAtlasClient(TestUtil.getClientLocal("service-account-atlan-argo"));
            seedData();

            createWithBothAndAgainUpdateWithoutRelations();
            createWithBothAndAgainUpdateWithSameRelations();

            updateProductWithBoth();

            updateToAddOutput();
            updateToRemoveOutput();

            updateToAddOutputAndRemoveInput();

            createAssetWithInputsOutputs();
            updateAssetWithInputsOutputs();

        } catch (Exception e) {
            throw e;
        } finally {
            LOG.info("Completed running UpdateDaapAppendRelations tests, took {} seconds", (System.currentTimeMillis() - start) / 1000);
        }
    }

    private void seedData() throws Exception {
        LOG.info(">> seedData");

        DOMAIN = createAndGetSuperDomain();

        TABLE_0 = getAtlasEntity(TYPE_TABLE, "table_0" + "_" +  getRandomName());
        String table_0_guid = createEntity(TABLE_0).getCreatedEntities().get(0).getGuid();

        TABLE_1 = getAtlasEntity(TYPE_TABLE, "table_1" + "_" +  getRandomName());
        String table_1_guid = createEntity(TABLE_1).getCreatedEntities().get(0).getGuid();

        TABLE_2 = getAtlasEntity(TYPE_TABLE, "table_2" + "_" +  getRandomName());
        String table_2_guid = createEntity(TABLE_2).getCreatedEntities().get(0).getGuid();

        TABLE_3 = getAtlasEntity(TYPE_TABLE, "table_3" + "_" +  getRandomName());
        String table_3_guid = createEntity(TABLE_3).getCreatedEntities().get(0).getGuid();

        TABLE_0 = getEntity(table_0_guid);
        TABLE_1 = getEntity(table_1_guid);
        TABLE_2 = getEntity(table_2_guid);
        TABLE_3 = getEntity(table_3_guid);

        LOG.info("<< seedData");
    }

    private static void createWithBothAndAgainUpdateWithoutRelations() throws Exception {
        LOG.info(">> createWithBothAndAgainUpdateWithoutRelations");

        String domainQN = (String) DOMAIN.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", domainQN, domainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(DOMAIN.getGuid()));
        product.setAppendRelationshipAttribute(REL_INPUTS, getAssetsAsRelations(TABLE_0, TABLE_1, TABLE_2));
        product.setAppendRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_2, TABLE_3));

        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();
        sleep(1);

        product = getEntity(productGuid);

        List<String> expectedInputGuids = Arrays.asList(TABLE_0.getGuid(), TABLE_1.getGuid(), TABLE_2.getGuid());
        List<String> expectedOutputGuids = Arrays.asList(TABLE_2.getGuid(), TABLE_3.getGuid());

        assertTrue(CollectionUtils.isEqualCollection(expectedInputGuids, (Collection) product.getAttribute(ATTR_INPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_INPUTS));
        assertEquals(3, ((List) product.getRelationshipAttribute(REL_INPUTS)).size());

        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));
        assertEquals(2, ((List) product.getRelationshipAttribute(REL_OUTPUTS)).size());

        //----

        AtlasEntity updateProduct = getProductEntity("prod_0", domainQN, domainQN);
        updateProduct.setGuid(product.getGuid());
        updateProduct.setAttribute(NAME, product.getAttribute(NAME));
        updateProduct.setAttribute(QUALIFIED_NAME, product.getAttribute(QUALIFIED_NAME));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        assertTrue(CollectionUtils.isEqualCollection(expectedInputGuids, (Collection) product.getAttribute(ATTR_INPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_INPUTS));
        assertEquals(3, ((List) product.getRelationshipAttribute(REL_INPUTS)).size());

        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));
        assertEquals(2, ((List) product.getRelationshipAttribute(REL_OUTPUTS)).size());

        LOG.info(">> createWithBothAndAgainUpdateWithoutRelations");
    }

    private static void createWithBothAndAgainUpdateWithSameRelations() throws Exception {
        LOG.info(">> createWithBothAndAgainUpdateWithSameRelations");
        String domainQN = (String) DOMAIN.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", domainQN, domainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(DOMAIN.getGuid()));
        product.setAppendRelationshipAttribute(REL_INPUTS, getAssetsAsRelations(TABLE_0, TABLE_1, TABLE_2));
        product.setAppendRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_2, TABLE_3));

        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();
        sleep(1);

        product = getEntity(productGuid);

        AtlasEntity updateProduct = getProductEntity("prod_0", domainQN, domainQN);
        updateProduct.setGuid(product.getGuid());
        updateProduct.setAttribute(NAME, product.getAttribute(NAME));
        updateProduct.setAttribute(QUALIFIED_NAME, product.getAttribute(QUALIFIED_NAME));
        updateProduct.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(DOMAIN.getGuid()));
        updateProduct.setAppendRelationshipAttribute(REL_INPUTS, getAssetsAsRelations(TABLE_0, TABLE_1, TABLE_2));
        updateProduct.setAppendRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_2, TABLE_3));

        updateProduct.setAttribute("userDescription", "edited userDescription");

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        List<String> expectedInputGuids = Arrays.asList(TABLE_0.getGuid(), TABLE_1.getGuid(), TABLE_2.getGuid());
        List<String> expectedOutputGuids = Arrays.asList(TABLE_2.getGuid(), TABLE_3.getGuid());

        assertTrue(CollectionUtils.isEqualCollection(expectedInputGuids, (Collection) product.getAttribute(ATTR_INPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_INPUTS));
        assertEquals(3, ((List) product.getRelationshipAttribute(REL_INPUTS)).size());

        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));
        assertEquals(2, ((List) product.getRelationshipAttribute(REL_OUTPUTS)).size());


        LOG.info(">> createWithBothAndAgainUpdateWithSameRelations");
    }

    private static void updateProductWithBoth() throws Exception {
        LOG.info(">> updateProductWithBoth");

        String domainQN = (String) DOMAIN.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", domainQN, domainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(DOMAIN.getGuid()));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();
        sleep(1);

        product = getEntity(productGuid);


        AtlasEntity updateProduct = copyOfProduct(product);
        updateProduct.setAppendRelationshipAttribute(REL_INPUTS, getAssetsAsRelations(TABLE_0, TABLE_1, TABLE_2));
        updateProduct.setAppendRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_2, TABLE_3));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        List<String> expectedInputGuids = Arrays.asList(TABLE_0.getGuid(), TABLE_1.getGuid(), TABLE_2.getGuid());
        List<String> expectedOutputGuids = Arrays.asList(TABLE_2.getGuid(), TABLE_3.getGuid());

        assertTrue(CollectionUtils.isEqualCollection(expectedInputGuids, (Collection) product.getAttribute(ATTR_INPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_INPUTS));
        assertEquals(3, ((List) product.getRelationshipAttribute(REL_INPUTS)).size());

        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));
        assertEquals(2, ((List) product.getRelationshipAttribute(REL_OUTPUTS)).size());

        LOG.info(">> updateProductWithBoth");
    }

    private static void updateToAddOutput() throws Exception {
        LOG.info(">> updateToAddOutput");

        String domainQN = (String) DOMAIN.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", domainQN, domainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(DOMAIN.getGuid()));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();
        sleep(1);

        product = getEntity(productGuid);

        AtlasEntity updateProduct = copyOfProduct(product);
        updateProduct.setAppendRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_0));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        List<String> expectedOutputGuids = Arrays.asList(TABLE_0.getGuid());
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));
        List<Map> outputs = (List<Map>) product.getRelationshipAttribute(REL_OUTPUTS);
        assertEquals(1, outputs.size());
        assertEquals(1, outputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size());


        updateProduct = copyOfProduct(product);
        updateProduct.setAppendRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_2));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        expectedOutputGuids = Arrays.asList(TABLE_0.getGuid(), TABLE_2.getGuid());
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));
        outputs = (List<Map>) product.getRelationshipAttribute(REL_OUTPUTS);
        assertEquals(2, outputs.size());
        assertEquals(2, outputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size());


        updateProduct = copyOfProduct(product);
        updateProduct.setAppendRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_3, TABLE_1));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        expectedOutputGuids = Arrays.asList(TABLE_0.getGuid(), TABLE_1.getGuid(), TABLE_2.getGuid(), TABLE_3.getGuid());
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));

        outputs = (List<Map>) product.getRelationshipAttribute(REL_OUTPUTS);
        assertEquals(4, outputs.size());
        assertEquals(4, outputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size());

        LOG.info(">> updateToAddOutput");
    }

    private static void updateToRemoveOutput() throws Exception {
        LOG.info(">> updateToRemoveOutput");

        String domainQN = (String) DOMAIN.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", domainQN, domainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(DOMAIN.getGuid()));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();
        sleep(1);

        product = getEntity(productGuid);

        AtlasEntity updateProduct = copyOfProduct(product);
        updateProduct.setRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_0, TABLE_2, TABLE_3, TABLE_1));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        List<String> expectedOutputGuids = Arrays.asList(TABLE_0.getGuid(), TABLE_1.getGuid(), TABLE_2.getGuid(), TABLE_3.getGuid());
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));

        List<Map> outputs = (List<Map>) product.getRelationshipAttribute(REL_OUTPUTS);
        assertEquals(4, outputs.size());
        assertEquals(4, outputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size());


        updateProduct = copyOfProduct(product);
        updateProduct.setRemoveRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_1, TABLE_3));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        expectedOutputGuids = Arrays.asList(TABLE_0.getGuid(), TABLE_2.getGuid());
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));

        outputs = (List<Map>) product.getRelationshipAttribute(REL_OUTPUTS);
        assertEquals(4, outputs.size());
        assertEquals(2, outputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size());


        updateProduct = copyOfProduct(product);
        updateProduct.setRemoveRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_2));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        expectedOutputGuids = Arrays.asList(TABLE_0.getGuid());
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));

        outputs = (List<Map>) product.getRelationshipAttribute(REL_OUTPUTS);
        assertEquals(4, outputs.size());
        assertEquals(1, outputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size());


        updateProduct = copyOfProduct(product);
        updateProduct.setRemoveRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_0));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        expectedOutputGuids = Collections.emptyList();
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));

        outputs = (List<Map>) product.getRelationshipAttribute(REL_OUTPUTS);
        assertEquals(4, outputs.size());
        assertEquals(0, outputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size());


        //Add one more again make empty

        updateProduct = copyOfProduct(product);
        updateProduct.setAppendRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_2));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        expectedOutputGuids = Arrays.asList(TABLE_2.getGuid());
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));

        outputs = (List<Map>) product.getRelationshipAttribute(REL_OUTPUTS);
        assertEquals(5, outputs.size());
        assertEquals(1, outputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size());


        updateProduct = copyOfProduct(product);
        updateProduct.setRemoveRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_2));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        expectedOutputGuids = Collections.emptyList();
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));

        outputs = (List<Map>) product.getRelationshipAttribute(REL_OUTPUTS);
        assertEquals(5, outputs.size());
        assertEquals(0, outputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size());

        LOG.info(">> updateToRemoveOutput");
    }

    private static void updateToAddOutputAndRemoveInput() throws Exception {
        LOG.info(">> updateToAddOutputAndRemoveInput");

        String domainQN = (String) DOMAIN.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", domainQN, domainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(DOMAIN.getGuid()));
        product.setRelationshipAttribute(REL_INPUTS, getAssetsAsRelations(TABLE_0, TABLE_1));
        product.setRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_0, TABLE_1));
        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();
        sleep(1);

        product = getEntity(productGuid);

        AtlasEntity updateProduct = copyOfProduct(product);

        updateProduct.setAppendRelationshipAttribute(REL_INPUTS, getAssetsAsRelations(TABLE_2, TABLE_3));
        updateProduct.setAppendRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_2, TABLE_3));

        updateProduct.setRemoveRelationshipAttribute(REL_INPUTS, getAssetsAsRelations(TABLE_0, TABLE_1));
        updateProduct.setRemoveRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_0, TABLE_1));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        List<String> expectedOutputGuids = Arrays.asList(TABLE_2.getGuid(), TABLE_3.getGuid());
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));

        List<Map> outputs = (List<Map>) product.getRelationshipAttribute(REL_OUTPUTS);
        assertEquals(4, outputs.size());
        assertEquals(2, outputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size());


        List<String> expectedInputGuids = Arrays.asList(TABLE_2.getGuid(), TABLE_3.getGuid());
        assertTrue(CollectionUtils.isEqualCollection(expectedInputGuids, (Collection) product.getAttribute(ATTR_INPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_INPUTS));

        List<Map> inputs = (List<Map>) product.getRelationshipAttribute(REL_INPUTS);
        assertEquals(4, inputs.size());
        assertEquals(2, inputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).collect(Collectors.toList()).size());

        //----------

        updateProduct = copyOfProduct(product);

        updateProduct.setAppendRelationshipAttribute(REL_INPUTS, getAssetsAsRelations(TABLE_2, TABLE_0));
        updateProduct.setAppendRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_2, TABLE_0));

        updateProduct.setRemoveRelationshipAttribute(REL_INPUTS, getAssetsAsRelations(TABLE_3));
        updateProduct.setRemoveRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_3));

        createEntity(updateProduct);
        sleep(1);

        product = getEntity(productGuid);

        expectedOutputGuids = Arrays.asList(TABLE_2.getGuid(), TABLE_0.getGuid());
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_OUTPUTS));

        outputs = (List<Map>) product.getRelationshipAttribute(REL_OUTPUTS);
        assertEquals(5, outputs.size());

        List<String> activeGuids = outputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).map(x -> (String) x.get("guid")).collect(Collectors.toList());
        assertEquals(2, activeGuids.size());
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, activeGuids));


        expectedInputGuids = Arrays.asList(TABLE_2.getGuid(), TABLE_0.getGuid());
        assertTrue(CollectionUtils.isEqualCollection(expectedInputGuids, (Collection) product.getAttribute(ATTR_INPUTS)));
        assertNotNull(product.getRelationshipAttribute(REL_INPUTS));

        inputs = (List<Map>) product.getRelationshipAttribute(REL_INPUTS);
        assertEquals(5, inputs.size());
        activeGuids = inputs.stream().filter(x -> "ACTIVE".equals(x.get("relationshipStatus"))).map(x -> (String) x.get("guid")).collect(Collectors.toList());
        assertEquals(2, activeGuids.size());
        assertTrue(CollectionUtils.isEqualCollection(expectedInputGuids, activeGuids));


        LOG.info(">> updateToAddOutputAndRemoveInput");
    }

    private static void createAssetWithInputsOutputs() throws Exception {
        LOG.info(">> createAssetWithInputsOutputs");
        String domainQN = (String) DOMAIN.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", domainQN, domainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(DOMAIN.getGuid()));

        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();
        sleep(1);


        product = getEntity(productGuid);

        // only output

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "table_0");
        table.setAppendRelationshipAttribute(REL_OUTPUT_PRODUCTS, Arrays.asList(new AtlasObjectId(product.getGuid(), DATA_PRODUCT_TYPE)));

        boolean failed = false;
        try {
            createEntity(table).getCreatedEntities().get(0).getGuid();
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("Can not update product relations while updating any asset"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        sleep(1);
        product = getEntity(productGuid);
        assertTrue(CollectionUtils.isEmpty((Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertTrue(CollectionUtils.isEmpty((Collection) product.getAttribute(ATTR_INPUTS)));
        assertTrue(CollectionUtils.isEmpty((Collection) product.getRelationshipAttribute(REL_OUTPUTS)));
        assertTrue(CollectionUtils.isEmpty((Collection) product.getRelationshipAttribute(REL_INPUTS)));

        // only input

        table = getAtlasEntity(TYPE_TABLE, "table_1");
        table.setAppendRelationshipAttribute(REL_INPUT_PRODUCTS, Arrays.asList(new AtlasObjectId(product.getGuid(), DATA_PRODUCT_TYPE)));

        failed = false;
        try {
            createEntity(table).getCreatedEntities().get(0).getGuid();
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("Can not update product relations while updating any asset"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        sleep(1);
        product = getEntity(productGuid);
        assertTrue(CollectionUtils.isEmpty((Collection) product.getAttribute(ATTR_OUTPUTS)));
        assertTrue(CollectionUtils.isEmpty((Collection) product.getAttribute(ATTR_INPUTS)));
        assertTrue(CollectionUtils.isEmpty((Collection) product.getRelationshipAttribute(REL_OUTPUTS)));
        assertTrue(CollectionUtils.isEmpty((Collection) product.getRelationshipAttribute(REL_INPUTS)));


        LOG.info(">> createAssetWithInputsOutputs");
    }

    private static void updateAssetWithInputsOutputs() throws Exception {
        LOG.info(">> updateAssetWithInputsOutputs");
        String domainQN = (String) DOMAIN.getAttribute(QUALIFIED_NAME);

        AtlasEntity product = getProductEntity("prod_0", domainQN, domainQN);
        product.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation(DOMAIN.getGuid()));
        product.setRelationshipAttribute(REL_OUTPUTS, getAssetsAsRelations(TABLE_0, TABLE_1));
        product.setRelationshipAttribute(REL_INPUTS, getAssetsAsRelations(TABLE_2, TABLE_3));

        String productGuid = createEntity(product).getCreatedEntities().get(0).getGuid();
        sleep(1);


        product = getEntity(productGuid);

        // only output

        AtlasEntity table = getAtlasEntity(TYPE_TABLE, "table_0");
        table.setAttribute(NAME, TABLE_0.getAttribute(NAME));
        table.setAttribute(QUALIFIED_NAME, TABLE_0.getAttribute(QUALIFIED_NAME));
        table.setRemoveRelationshipAttribute(REL_OUTPUT_PRODUCTS, Arrays.asList(new AtlasObjectId(product.getGuid(), DATA_PRODUCT_TYPE)));

        boolean failed = false;
        try {
            createEntity(table).getCreatedEntities().get(0).getGuid();
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("Can not update product relations while updating any asset"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        sleep(1);
        product = getEntity(productGuid);

        List<String> expectedInputGuids = Arrays.asList(TABLE_2.getGuid(), TABLE_3.getGuid());
        List<String> expectedOutputGuids = Arrays.asList(TABLE_0.getGuid(), TABLE_1.getGuid());

        assertTrue(CollectionUtils.isEqualCollection(expectedInputGuids, (Collection) product.getAttribute(ATTR_INPUTS)));
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));


        // only input

        table = getAtlasEntity(TYPE_TABLE, "table_0");
        table.setAttribute(NAME, TABLE_2.getAttribute(NAME));
        table.setAttribute(QUALIFIED_NAME, TABLE_2.getAttribute(QUALIFIED_NAME));
        table.setRemoveRelationshipAttribute(REL_INPUT_PRODUCTS, Arrays.asList(new AtlasObjectId(product.getGuid(), DATA_PRODUCT_TYPE)));

        failed = false;
        try {
            createEntity(table);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(), 400);
            assertTrue(exception.getMessage().contains("Can not update product relations while updating any asset"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        sleep(1);

        assertTrue(CollectionUtils.isEqualCollection(expectedInputGuids, (Collection) product.getAttribute(ATTR_INPUTS)));
        assertTrue(CollectionUtils.isEqualCollection(expectedOutputGuids, (Collection) product.getAttribute(ATTR_OUTPUTS)));


        LOG.info(">> updateAssetWithInputsOutputs");
    }

}