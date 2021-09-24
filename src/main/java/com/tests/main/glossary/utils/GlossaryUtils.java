package com.tests.main.glossary.utils;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.apache.atlas.model.glossary.AtlasGlossaryTerm;
import org.apache.atlas.model.glossary.relations.AtlasGlossaryHeader;
import org.apache.atlas.model.glossary.relations.AtlasRelatedCategoryHeader;
import org.apache.commons.lang.RandomStringUtils;

import java.nio.charset.Charset;
import java.util.Random;

import static com.tests.main.glossary.GlossaryMain.atlasClient;

public class GlossaryUtils {

    public static AtlasGlossary createGlossary(AtlasGlossary glossary) throws AtlasServiceException {
        return atlasClient.createGlossary(glossary);
    }
    public static AtlasGlossary updateGlossary(String guid, AtlasGlossary glossary) throws AtlasServiceException {
        return atlasClient.updateGlossaryByGuid(guid, glossary);
    }

    public static AtlasGlossaryTerm createTerm(AtlasGlossaryTerm glossary) throws AtlasServiceException {
        return atlasClient.createGlossaryTerm(glossary);
    }

    public static AtlasGlossaryCategory createCategory(AtlasGlossaryCategory category) throws AtlasServiceException {
        return atlasClient.createGlossaryCategory(category);
    }

    public static AtlasGlossary getGlossaryModel() {
       return getGlossaryModel(getRandomName());
    }

    public static AtlasGlossary getGlossaryModel(String name) {
        AtlasGlossary glossary = new AtlasGlossary();
        glossary.setName(name);
        glossary.setShortDescription("Short description");
        glossary.setLongDescription("Long description");

        return glossary;
    }

    public static AtlasGlossaryTerm getTermModel(String glossaryGuid) {
        return getTermModel(getRandomName(), glossaryGuid);
    }

    public static AtlasGlossaryTerm getTermModel(String termName, String glossaryGuid) {
        AtlasGlossaryTerm term = new AtlasGlossaryTerm();
        term.setName(termName);
        term.setShortDescription("Short description");
        term.setLongDescription("Long description");
        term.setAnchor(getGlossaryHeader(glossaryGuid));

        return term;
    }


    public static AtlasRelatedCategoryHeader getCategoryParentModel(AtlasGlossaryCategory category) {
        AtlasRelatedCategoryHeader parentHeader = new AtlasRelatedCategoryHeader();
        parentHeader.setCategoryGuid(category.getGuid());

        return parentHeader;
    }

    public static AtlasGlossaryCategory getCategoryModelWithParent(String glossaryGuid, AtlasRelatedCategoryHeader parentCategory) {
        AtlasGlossaryCategory category = getCategoryModel(getRandomName(), glossaryGuid);
        category.setParentCategory(parentCategory);
        return category;
    }


    public static AtlasGlossaryCategory getCategoryModel(String glossaryGuid) {
        return getCategoryModel(getRandomName(), glossaryGuid);
    }

    public static AtlasGlossaryCategory getCategoryModel(String categoryName, String glossaryGuid) {
        AtlasGlossaryCategory category = new AtlasGlossaryCategory();
        category.setName(categoryName);
        category.setAnchor(getGlossaryHeader(glossaryGuid));

        return category;
    }

    public static AtlasGlossaryHeader getGlossaryHeader(String glossaryGuid) {
        AtlasGlossaryHeader glossaryHeader = new AtlasGlossaryHeader();
        glossaryHeader.setGlossaryGuid(glossaryGuid);

        return glossaryHeader;
    }

    public static String getRandomName() {
        return RandomStringUtils.randomAlphanumeric(8);
    }
}
