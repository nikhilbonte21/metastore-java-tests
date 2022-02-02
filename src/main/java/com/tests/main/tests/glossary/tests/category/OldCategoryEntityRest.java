package com.tests.main.tests.glossary.tests.category;


import org.apache.atlas.AtlasErrorCode;
import com.tests.main.client.AtlasServiceException;
import org.apache.atlas.model.glossary.AtlasGlossary;
import org.apache.atlas.model.glossary.AtlasGlossaryCategory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.tests.main.utils.TestUtils.*;
import static org.junit.Assert.*;

@Deprecated
public class OldCategoryEntityRest {
    private static final Logger LOG = LoggerFactory.getLogger(OldCategoryEntityRest.class);

    private static final int ALREADY_EXIST_STATUS_CODE = AtlasErrorCode.GLOSSARY_CATEGORY_ALREADY_EXISTS.getHttpCode().getStatusCode();
    private static final String ALREADY_EXIST_ERROR_CODE = AtlasErrorCode.GLOSSARY_CATEGORY_ALREADY_EXISTS.getErrorCode();

    public static void main(String[] args) throws Exception {
        run();
    }

    public static void run() throws Exception {
        LOG.info("Running CategoryEntityRest tests");

        long start = System.currentTimeMillis();
        try {

        } catch (Exception e){
            throw e;
        } finally {
            cleanUpAll();
            LOG.info("Completed running CategoryEntityRest tests, took {} seconds", (System.currentTimeMillis() - start) / 1000 );
        }
    }


    /*NOTE

    This will fail as we are not supporting changing of glossary for Terms & Categories
    But, https://linear.app/atlanproduct/issue/META-2694/use-uuid-in-qualifiedname-instead-of-name
    Feature has required changes for changing of acnhor & reevaluating qualifiedName for both Terms & Categories
    If chaging anchor is allowed, in that case this test must poss

    */
    private void changeAnchor() throws AtlasServiceException {
        LOG.info(">> changeAnchor");
        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel(getRandomName()));
        AtlasGlossary glossary_1 = createGlossary(getGlossaryModel(getRandomName()));

        AtlasGlossaryCategory category_0 = createCategory(getCategoryModel(glossary_0.getGuid()));
        AtlasGlossaryCategory category_1 = createCategory(getCategoryModelWithParent(glossary_0.getGuid(), getCategoryParentModel(category_0)));

        AtlasGlossaryCategory category_2 = createCategory(getCategoryModel(glossary_1.getGuid()));
        AtlasGlossaryCategory category_3 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_2)));
        AtlasGlossaryCategory category_4 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_3)));

        String category_0_nid = getNanoid(category_0.getQualifiedName());
        String category_1_nid = getNanoid(category_1.getQualifiedName());
        String category_2_nid = getNanoid(category_2.getQualifiedName());
        String category_3_nid = getNanoid(category_3.getQualifiedName());
        String category_4_nid = getNanoid(category_4.getQualifiedName());

        assertEquals(category_3.getQualifiedName(), category_2_nid + "." + category_3_nid + "@" + glossary_1.getQualifiedName());
        assertEquals(category_4.getQualifiedName(), category_2_nid + "." + category_3_nid + "." + category_4_nid +  "@" + glossary_1.getQualifiedName());

        //change category_3's anchor from glossary_1 to glossary_0
        AtlasGlossaryCategory category = getCategory(category_3.getGuid());
        category.setParentCategory(null);
        category.setAnchor(getGlossaryHeader(glossary_0.getGuid()));
        updateCategory(category.getGuid(), category);

        assertEquals(getCategory(category_3.getGuid()).getQualifiedName(), category_3_nid + "@" + glossary_0.getQualifiedName());
        assertEquals(getCategory(category_4.getGuid()).getQualifiedName(), category_3_nid + "." + category_4_nid +  "@" + glossary_0.getQualifiedName());

        assertEquals(getCategory(category_0.getGuid()).getQualifiedName(), category_0_nid + "@" + glossary_0.getQualifiedName());
        assertEquals(getCategory(category_1.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid +  "@" + glossary_0.getQualifiedName());


        LOG.info(">> changeAnchor");
    }

    /*NOTE

    This will fail as we are not supporting changing of glossary for Terms & Categories
    But, https://linear.app/atlanproduct/issue/META-2694/use-uuid-in-qualifiedname-instead-of-name
    Feature has required changes for changing of acnhor & reevaluating qualifiedName for both Terms & Categories
    If changing anchor is allowed, in that case this test must pass

    As of now This case fails:
    As category_4, category_5 & category_6 's qualifiedName is not getting properly evaluated due the category_3's glossary change
    If changing anchor is allowed, in that case this needs to be fixed
     */
    private void changeParentInAnotherAnchor() throws AtlasServiceException {
        LOG.info(">> changeParentInAnotherAnchor");
        AtlasGlossary glossary_0 = createGlossary(getGlossaryModel(getRandomName()));
        AtlasGlossary glossary_1 = createGlossary(getGlossaryModel(getRandomName()));

        AtlasGlossaryCategory category_0 = createCategory(getCategoryModel(glossary_0.getGuid()));
        AtlasGlossaryCategory category_1 = createCategory(getCategoryModelWithParent(glossary_0.getGuid(), getCategoryParentModel(category_0)));
        AtlasGlossaryCategory category_1_1 = createCategory(getCategoryModelWithParent(glossary_0.getGuid(), getCategoryParentModel(category_1)));

        AtlasGlossaryCategory category_2 = createCategory(getCategoryModel(glossary_1.getGuid()));
        AtlasGlossaryCategory category_3 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_2)));
        AtlasGlossaryCategory category_4 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_3)));
        AtlasGlossaryCategory category_5 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_4)));
        AtlasGlossaryCategory category_6 = createCategory(getCategoryModelWithParent(glossary_1.getGuid(), getCategoryParentModel(category_5)));

        String category_0_nid = getNanoid(category_0.getQualifiedName());
        String category_1_nid = getNanoid(category_1.getQualifiedName());
        String category_2_nid = getNanoid(category_2.getQualifiedName());
        String category_3_nid = getNanoid(category_3.getQualifiedName());
        String category_4_nid = getNanoid(category_4.getQualifiedName());
        String category_5_nid = getNanoid(category_5.getQualifiedName());
        String category_6_nid = getNanoid(category_6.getQualifiedName());
        String category_1_1_nid = getNanoid(category_1_1.getQualifiedName());

        assertEquals(category_1_1.getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_1_1_nid + "@" + glossary_0.getQualifiedName());
        assertEquals(category_3.getQualifiedName(), category_2_nid + "." + category_3_nid + "@" + glossary_1.getQualifiedName());
        assertEquals(category_4.getQualifiedName(), category_2_nid + "." + category_3_nid + "."+ category_4_nid + "@" + glossary_1.getQualifiedName());
        assertEquals(category_5.getQualifiedName(), category_2_nid + "." + category_3_nid + "."+ category_4_nid + "." + category_5_nid + "@" + glossary_1.getQualifiedName());
        assertEquals(category_6.getQualifiedName(), category_2_nid + "." + category_3_nid + "."+ category_4_nid + "." + category_5_nid + "." + category_6_nid + "@" + glossary_1.getQualifiedName());

/*        LOG.info("glossary_0: nanoId {}, guid {}, name {}", getNanoid(glossary_0.getQualifiedName()), glossary_0.getGuid(), glossary_0.getName());
        LOG.info("glossary_1: nanoId {}, guid {}, name {}", getNanoid(glossary_1.getQualifiedName()), glossary_1.getGuid(), glossary_1.getName());

        LOG.info("category_0: nanoId {}, guid {}, name {}", category_0_nid, category_0.getGuid(), category_0.getName());
        LOG.info("category_1: nanoId {}, guid {}, name {}", category_1_nid, category_1.getGuid(), category_1.getName());
        LOG.info("category_1_1: nanoId {}, guid {}, name {}", category_1_1_nid, category_1_1.getGuid(), category_1_1.getName());
        LOG.info("category_3: nanoId {}, guid {}, name {}", category_3_nid, category_3.getGuid(), category_3.getName());
        LOG.info("category_4: nanoId {}, guid {}, name {}", category_4_nid, category_4.getGuid(), category_4.getName());
        LOG.info("category_5: nanoId {}, guid {}, name {}", category_5_nid, category_5.getGuid(), category_5.getName());
        LOG.info("category_6: nanoId {}, guid {}, name {}", category_6_nid, category_6.getGuid(), category_6.getName());*/

        //change category_3's parent from glossary_1.category_2 to glossary_0.category_1
        /*  Before:
            glossary_0: category_0 -> category_1 -> category_1_1
            glossary_1: category_2 -> category_3 -> category_4 -> category_5 -> category_6

            After:
            glossary_0: category_0 -> category_1 -> category_3 -> category_4 -> category_5 -> category_6
            glossary_1: category_2
         */
        AtlasGlossaryCategory category = getCategory(category_3.getGuid());
        category.setAnchor(getGlossaryHeader(glossary_0.getGuid()));
        category.setParentCategory(getCategoryParentModel(category_1));
        updateCategory(category.getGuid(), category);

        assertEquals(getGlossary(glossary_0.getGuid()).getCategories().size(), 7);
        assertEquals(getGlossary(glossary_1.getGuid()).getCategories().size(), 1);

        assertEquals(getCategory(category_1_1.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_1_1_nid + "@" + glossary_0.getQualifiedName());
        assertEquals(getCategory(category_0.getGuid()).getQualifiedName(), category_0_nid + "@" + glossary_0.getQualifiedName());
        assertEquals(getCategory(category_1.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid +  "@" + glossary_0.getQualifiedName());


        assertEquals(getCategory(category_2.getGuid()).getQualifiedName(), category_2_nid + "@" + glossary_1.getQualifiedName());

        assertEquals(getCategory(category_3.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_3_nid + "@" + glossary_0.getQualifiedName());

        assertEquals(getCategory(category_4.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_3_nid + "." + category_4_nid +  "@" + glossary_0.getQualifiedName());
        assertEquals(getCategory(category_5.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_3_nid + "." + category_4_nid + "." + category_5_nid + "@" + glossary_0.getQualifiedName());
        assertEquals(getCategory(category_6.getGuid()).getQualifiedName(), category_0_nid + "." + category_1_nid + "." + category_3_nid + "." + category_4_nid + "." + category_5_nid + "." + category_6_nid + "@" + glossary_0.getQualifiedName());


        LOG.info("<< changeParentInAnotherAnchor");
    }


    private static String getNanoid(String qualifiedName){
        String[] at = qualifiedName.split("@");
        String[] dot = at[0].split("\\.");

        return dot[dot.length-1];
    }
}