package com.tests.main.tests.stakeholders;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.atlas.model.instance.AtlasRelatedObjectId;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.tests.main.tests.movedomainslatest.DomainUtilsLatest.getDomainAsRelation;
import static com.tests.main.utils.TestUtil.NAME;
import static com.tests.main.utils.TestUtil.QUALIFIED_NAME;
import static com.tests.main.utils.TestUtil.getRandomName;

public class StakeholderUtils {

    public static final String PATTERN_QUALIFIED_NAME_ALL_DOMAINS = "stakeholderTitle/domain/default/";
    public static final String PATTERN_QUALIFIED_NAME_DOMAIN = "stakeholderTitle/domain/";


    public static final String STAR = "*/super";
    public static final String ATTR_DOMAIN_QUALIFIED_NAMES = "stakeholderTitleDomainQualifiedNames";

    public static final String REL_ATTR_STAKEHOLDERS = "stakeholders";


    public static final String ATTR_DOMAIN_QUALIFIED_NAME  = "stakeholderDomainQualifiedName";
    public static final String ATTR_STAKEHOLDER_TITLE_GUID = "stakeholderTitleGuid";

    public static final String REL_ATTR_STAKEHOLDER_TITLE = "stakeholderTitle";
    public static final String REL_ATTR_STAKEHOLDER_DOMAIN = "stakeholderDataDomain";

    public static AtlasEntity getStakeholder(String name, String titleGuid, String domainGuid) {
        AtlasEntity stakeholder = new AtlasEntity("Stakeholder");

        stakeholder.setAttribute(NAME, name + getRandomName());
        stakeholder.setAttribute(QUALIFIED_NAME, name);
        stakeholder.setAttribute("personaUsers", Arrays.asList("nikhil.bonte"));

        if (StringUtils.isNotEmpty(titleGuid)) {
            stakeholder.setRelationshipAttribute(REL_ATTR_STAKEHOLDER_TITLE, getTitleAsRelation(titleGuid));
        }

        if (StringUtils.isNotEmpty(domainGuid)) {
            stakeholder.setRelationshipAttribute(REL_ATTR_STAKEHOLDER_DOMAIN, getDomainAsRelation(domainGuid));
        }

        return stakeholder;
    }

    public static AtlasRelatedObjectId getTitleAsRelation(String guid) {
        AtlasRelatedObjectId objectId = new AtlasRelatedObjectId();
        objectId.setTypeName("StakeholderTitle");
        objectId.setGuid(guid);

        return objectId;
    }

    public static AtlasEntity getStakeholderTitle(String name, String... domainQualifiedNames) {
        AtlasEntity persona = new AtlasEntity("StakeholderTitle");


        persona.setAttribute(NAME, name + getRandomName());
        persona.setAttribute(QUALIFIED_NAME, name);
        if (domainQualifiedNames != null) {
            persona.setAttribute(ATTR_DOMAIN_QUALIFIED_NAMES, Arrays.asList(domainQualifiedNames));
        }

        return persona;
    }

    public static String getTitleUUID(String titleQualifiedName) {
        String[] splitted = titleQualifiedName.split("/");

        return splitted[splitted.length -1];
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

        List<String> permissions = new ArrayList<>();
        permissions.add("persona-domain-read");
        permissions.add("persona-domain-update");
        permissions.add("persona-domain-sub-domain-create");
        permissions.add("persona-domain-sub-domain-update");

        policy.setAttribute("policyActions", permissions);
        policy.setAttribute("policyResources", Arrays.asList("entity:" + resourceQN));

        policy.setRelationshipAttribute("accessControl", new AtlasObjectId(personaGuid, "Persona"));

        return policy;
    }
}
