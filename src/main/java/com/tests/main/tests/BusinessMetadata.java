package com.tests.main.tests;

import org.apache.atlas.model.typedef.AtlasBusinessMetadataDef;
import org.apache.atlas.model.typedef.AtlasStructDef;

public class BusinessMetadata {

    private String name;
    private String displayName;
    private AtlasStructDef.AtlasAttributeDef stringAttr;
    private AtlasStructDef.AtlasAttributeDef intAttr;

    public BusinessMetadata(AtlasBusinessMetadataDef bmDef) {
        this.name = bmDef.getName();
        this.displayName = bmDef.getDisplayName();

        bmDef.getAttributeDefs().forEach(this::fillFields);
    }

    private void fillFields(AtlasStructDef.AtlasAttributeDef attributeDef) {
        switch (attributeDef.getTypeName()) {
            case "string": stringAttr = attributeDef; break;
            case "int": intAttr = attributeDef; break;
        }
    }

    public String getName() {
        return name;
    }

    public String getDisplayName() {
        return displayName;
    }

    public AtlasStructDef.AtlasAttributeDef getStringAttr() {
        return stringAttr;
    }

    public AtlasStructDef.AtlasAttributeDef getIntAttr() {
        return intAttr;
    }

    @Override
    public String toString() {
        return "BusinessMetadata{" +
                "name='" + name + '\'' +
                ", displayName='" + displayName + '\'' +
                ", stringAttr=" + stringAttr +
                ", intAttr=" + intAttr +
                '}';
    }
}
