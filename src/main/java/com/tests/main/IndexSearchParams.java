//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.tests.main;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.Map;
import java.util.Set;

import org.apache.atlas.model.discovery.SearchParams;
import org.apache.atlas.type.AtlasType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonInclude(Include.NON_EMPTY)
@JsonIgnoreProperties(
    ignoreUnknown = true
)
public class IndexSearchParams extends SearchParams {
    private static final Logger LOG = LoggerFactory.getLogger(IndexSearchParams.class);
    private Map dsl;
    private String purpose;
    private String persona;
    private String queryString;
    private boolean allowDeletedRelations;
    private boolean accessControlExclusive;
    private boolean includeRelationshipAttributes;
    private Set<String> relationAttributes;

    public IndexSearchParams() {
    }

    public String getQuery() {
        return this.queryString;
    }

    public Map getDsl() {
        return this.dsl;
    }

    public void setDsl(Map dsl) {
        this.dsl = dsl;
        this.queryString = AtlasType.toJson(dsl);
    }

    public long getQuerySize() {
        return this.dsl.get("size") != null ? ((Number)this.dsl.get("size")).longValue() : 10L;
    }

    public boolean isAllowDeletedRelations() {
        return this.allowDeletedRelations;
    }

    public boolean isAccessControlExclusive() {
        return this.accessControlExclusive;
    }

    public void setAccessControlExclusive(boolean accessControlExclusive) {
        this.accessControlExclusive = accessControlExclusive;
    }

    public void setAllowDeletedRelations(boolean allowDeletedRelations) {
        this.allowDeletedRelations = allowDeletedRelations;
    }

    public String getPurpose() {
        return this.purpose;
    }

    public void setPurpose(String purpose) {
        this.purpose = purpose;
    }

    public String getPersona() {
        return this.persona;
    }

    public void setPersona(String persona) {
        this.persona = persona;
    }

    public void setRelationAttributes(Set<String> relationAttributes) {
        this.relationAttributes = relationAttributes;
    }

    public boolean isIncludeRelationshipAttributes() {
        return this.includeRelationshipAttributes;
    }

    public void setIncludeRelationshipAttributes(boolean includeRelationshipAttributes) {
        this.includeRelationshipAttributes = includeRelationshipAttributes;
    }

    public String toString() {
        String var10000 = String.valueOf(this.dsl);
        return "IndexSearchParams{dsl=" + var10000 + ", purpose='" + this.purpose + "', persona='" + this.persona + "', queryString='" + this.queryString + "', allowDeletedRelations=" + this.allowDeletedRelations + ", accessControlExclusive=" + this.accessControlExclusive + ", includeRelationshipAttributes=" + this.includeRelationshipAttributes + ", utmTags=" + String.valueOf(this.getUtmTags()) + "}";
    }

    public void setQuery(String query) {
        this.queryString = query;
    }
}
