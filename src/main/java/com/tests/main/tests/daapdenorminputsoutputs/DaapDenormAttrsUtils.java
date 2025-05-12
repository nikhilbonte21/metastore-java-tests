package com.tests.main.tests.daapdenorminputsoutputs;


import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.DATA_DOMAIN;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getDomainAsRelation;
import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getDomainEntity;
import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static com.tests.main.utils.TestUtil.createEntity;
import static com.tests.main.utils.TestUtil.getEntity;
import static com.tests.main.utils.TestUtil.sleep;

public class DaapDenormAttrsUtils {

    public static final String ATTR_OUTPUTS = "daapOutputPortGuids";
    public static final String ATTR_INPUTS  = "daapInputPortGuids";

    public static final String REL_OUTPUTS  = "outputPorts";
    public static final String REL_INPUTS  = "inputPorts";

    public static final String REL_INPUT_PRODUCTS = "inputPortDataProducts";
    public static final String REL_OUTPUT_PRODUCTS = "outputPortDataProducts";

     static AtlasEntity createAndGetSuperDomain() throws Exception {
        AtlasEntity domain = getDomainEntity("domain_0", null, null);
         String domainGuid = createEntity(domain).getCreatedEntities().get(0).getGuid();

         sleep(1);
         return getEntity(domainGuid);
    }

    static List<AtlasRelatedObjectId> getAssetsAsRelations(AtlasEntity... tableGuids) {
        List<AtlasRelatedObjectId> ret = new ArrayList<>();

        for (AtlasEntity table : tableGuids) {
            AtlasRelatedObjectId objectId = new AtlasRelatedObjectId();
            objectId.setTypeName(TYPE_TABLE);
            objectId.setGuid(table.getGuid());

            ret.add(objectId);
        }

        return ret;
    }

    static AtlasEntity copyOfProduct(AtlasEntity sourceProduct) {
        AtlasEntity ret = new AtlasEntity();

        ret.setGuid(sourceProduct.getGuid());
        ret.setTypeName(sourceProduct.getTypeName());
        ret.setAttribute(NAME, sourceProduct.getAttribute(NAME));
        ret.setAttribute(QUALIFIED_NAME, sourceProduct.getAttribute(QUALIFIED_NAME));
        String domainGuid = (String) ((HashMap) sourceProduct.getRelationshipAttribute(DATA_DOMAIN)).get("guid");
        ret.setRelationshipAttribute(DATA_DOMAIN, getDomainAsRelation((domainGuid)));

        return ret;
    }
}
