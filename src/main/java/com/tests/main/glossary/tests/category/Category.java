package com.tests.main.glossary.tests.category;

import org.apache.atlas.AtlasServiceException;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.glossary.utils.GlossaryUtils.*;
import static org.junit.Assert.assertEquals;

public class Category {
    private static final Logger LOG = LoggerFactory.getLogger(Category.class);

    public Category(){

    }

    public void runTests() throws AtlasServiceException {
        LOG.info("Running Category tests");

        try {
            testUpdateParent();
        } catch (Exception e){
            throw e;
        }


        LOG.info("Completed running Category tests");
    }

    private void testUpdateParent() throws AtlasServiceException{
        LOG.info(">> testUpdateParent");
        AtlasGlossary glossary = createGlossary(getGlossaryModel(getRandomName()));

        AtlasGlossaryCategory category_0 = createCategory(getCategoryModel(glossary.getGuid()));

        AtlasGlossaryCategory category_1 = createCategory(getCategoryModelWithParent(glossary.getGuid(), getCategoryParentModel(category_0)));
        assertEquals(category_1.getParentCategory().getCategoryGuid(), category_0.getGuid());

        AtlasGlossaryCategory category_2 = createCategory(getCategoryModelWithParent(glossary.getGuid(), getCategoryParentModel(category_1)));
        assertEquals(category_2.getParentCategory().getCategoryGuid(), category_1.getGuid());

        AtlasGlossaryCategory category_3 = createCategory(getCategoryModelWithParent(glossary.getGuid(), getCategoryParentModel(category_2)));
        assertEquals(category_3.getParentCategory().getCategoryGuid(), category_2.getGuid());

        LOG.info("<< testUpdateParent");
    }

}
