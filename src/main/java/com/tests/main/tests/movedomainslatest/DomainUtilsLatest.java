package com.tests.main.tests.movedomainslatest;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.List;

import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.getRandomName;

public class DomainUtilsLatest {

    public static final String DATA_PRODUCT_TYPE = "DataProduct";
    public static final String DATA_DOMAIN_TYPE = "DataDomain";
    public static final String DATA_DOMAIN = "dataDomain";
    public static final String PARENT_DOMAIN = "parentDomain";
    public static final String SUB_DOMAINS = "subDomains";
    public static final String DATA_PRODUCTS = "dataProducts";

    public static final String PARENT_DOMAIN_QN = "parentDomainQualifiedName";
    public static final String SUPER_DOMAIN_QN = "superDomainQualifiedName";

    public static final String DOMAIN_DOMAIN_RELATION_TYPE = "parent_domain_sub_domains";
    public static final String DOMAIN_DOMAIN_RELATION_LABEL = "__DataDomain.subDomains";

    
    public static AtlasEntity getProductEntity(String productName, String parentQualifiedName, String superQualifiedName) {
        AtlasEntity entity = new AtlasEntity(DATA_PRODUCT_TYPE);
        productName = productName + "_" + getRandomName();
        entity.setAttribute(NAME, productName);

        if (StringUtils.isNotEmpty(superQualifiedName)) {
            entity.setAttribute("superDomainQualifiedName", superQualifiedName);
        }

        if (StringUtils.isNotEmpty(parentQualifiedName)) {
            productName = parentQualifiedName + "/product/" + productName;
            entity.setAttribute("parentDomainQualifiedName", parentQualifiedName);
        } else {
            productName = "product/" + productName;
        }

        entity.setAttribute(QUALIFIED_NAME, productName);

        return entity;
    }

    public static AtlasEntity getDomainEntity(String domainName, String parentQualifiedName, String superQualifiedName) {
        AtlasEntity entity = new AtlasEntity(DATA_DOMAIN_TYPE);
        domainName = domainName + "_" + getRandomName();
        entity.setAttribute(NAME, domainName);

        if (StringUtils.isNotEmpty(superQualifiedName)) {
            entity.setAttribute("superDomainQualifiedName", superQualifiedName);
        }
        if (StringUtils.isNotEmpty(parentQualifiedName)) {
            domainName = parentQualifiedName + "/domain/" + domainName;
            entity.setAttribute("parentDomainQualifiedName", parentQualifiedName);
        } else {
            domainName = "default/domain/" + domainName;
        }

        entity.setAttribute(QUALIFIED_NAME, domainName);

        return entity;
    }

    public static AtlasRelatedObjectId getDomainAsRelation(String domainGuid) {
        AtlasRelatedObjectId objectId = new AtlasRelatedObjectId();
        objectId.setTypeName(DATA_DOMAIN_TYPE);
        objectId.setGuid(domainGuid);

        return objectId;
    }

    public static List<AtlasRelatedObjectId> getProductAsRelation(String productGuid) {
        AtlasRelatedObjectId objectId = new AtlasRelatedObjectId();
        objectId.setTypeName(DATA_PRODUCT_TYPE);
        objectId.setGuid(productGuid);

        return Arrays.asList(objectId);
    }

    public static String chain(String... names) {
        StringBuilder sb = new StringBuilder();
        sb.append("default/domain/").append(names[0]).append("/super");

        for (int i = 1; i < names.length; i++) {
            sb.append("/domain/").append(names[i]);
        }

        return sb.toString();
    }

    public static String getNanoId(String qualifiedName) {
        StringBuilder sb = new StringBuilder();

        String[] splitted = qualifiedName.split("/");

        String ret = splitted[splitted.length - 1];
        if (ret.endsWith("super"))
            ret = splitted[splitted.length - 2];

        return ret;
    }

    public static AtlasEntity getPersona(String personaName) {
        AtlasEntity persona = new AtlasEntity("Persona");


        persona.setAttribute(NAME, personaName + getRandomName());
        persona.setAttribute(QUALIFIED_NAME, personaName);
        persona.setAttribute("personaUsers", Arrays.asList("nikhil.bonte"));

        return persona;
    }

    public static AtlasEntity getPersonaPolicy(String policyName, String resourceQN, String personaGuid) {
        AtlasEntity policy = new AtlasEntity("AuthPolicy");

        policy.setAttribute(NAME, policyName + getRandomName());
        policy.setAttribute(QUALIFIED_NAME, policyName);

        policy.setAttribute("policyType", "allow");
        policy.setAttribute("policyCategory", "persona");
        policy.setAttribute("policySubCategory", "domain");
        policy.setAttribute("policyServiceName", "atlas");
        policy.setAttribute("policyResourceCategory", "CUSTOM");

        policy.setAttribute("policyActions", Arrays.asList("persona-domain-read"));
        policy.setAttribute("policyResources", Arrays.asList("entity:" + resourceQN));

        policy.setRelationshipAttribute("accessControl", new AtlasObjectId(personaGuid, "Persona"));

        return policy;
    }
}
