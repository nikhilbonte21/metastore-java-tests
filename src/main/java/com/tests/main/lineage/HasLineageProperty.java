package com.tests.main.lineage;

import com.tests.main.tests.glossary.tests.TestsMain;
import com.tests.main.utils.ESUtil;
import com.tests.main.utils.TestUtil;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasEntityHeader;
import org.apache.atlas.model.instance.EntityMutationResponse;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

import static com.tests.main.utils.TestUtil.*;
import static com.tests.main.utils.TestUtil.verifyESHasLineage;
import static org.junit.Assert.*;

public class HasLineageProperty implements TestsMain {
    private static final Logger LOG = LoggerFactory.getLogger(HasLineageProperty.class);

    private static final String HAS_LINEAGE = "__hasLineage";

    public static void main(String[] args) throws Exception {
        try {
            new HasLineageProperty().run();
        } finally {
            runAsGod();
            cleanUpAll();
            ESUtil.close();
        }
    }

    @Override
    public void run() throws  Exception {
        LOG.info("Running Lineage tests");

        long start = System.currentTimeMillis();
        try {
            runAsGod();

            /*testHasLineage();
            testHasLineage_1();

            testHasLineage_not_connected_0();
            testHasLineage_not_connected_1();

            tableToView();
            tableToView_deleteProcess();
            tableToView_deleteRelationships();
            tableToView_deleteRelationships_singleRequest();*/
            tableToView_deleteTable();
            /*tableToView_deleteView();

            multipleInputs_singleOutput();
            multipleInputs_singleOutput_deleteTable();

            testHasLineage_deleteProcesses_singleRequest();
            testHasLineage_longLineage_deleteProcesses_singleRequest();

            //testHasLineageProperty_multipleInputsOutputs();
            //this has the following issue
            //Create/Update Process with Asset as input/output in relationshipAttributes


            testHasLineagePropertyBulk();
            //testHasLineageHideProcess(); // deprecated
*/
        } catch (Exception e){
            throw e;
        } finally {
            Thread.sleep(2000);
            cleanUpAll();
            ESUtil.close();
            LOG.info("Completed running Lineage tests, took {} seconds", (System.currentTimeMillis() - start) / 1000 );
        }
    }

    private static void testHasLineage() throws Exception {
        LOG.info(">> testHasLineage");


        EntityMutationResponse response;
        AtlasEntity table_1, table_2, process_1;

        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        table_2 = createCustomEntity(TYPE_TABLE, "table_2");

        /*
         *
         * table_1 -> process_0 -> table_2
         *
         * */

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);

        /*
        * table_1 -> process_1 -> table_2
        *
        * If Table2 gets deleted
        *   If Table1 is not connected to any other active asset via process
        *       __hasLineage for Table1 -> null
        *       __hasLineage for process_1 -> null
        *       __hasLineage for Table2 -> true
        * */
        deleteEntities(Collections.singletonList(table_2.getGuid()));

        Thread.sleep(2000);

        table_1 = getEntity(table_1.getGuid());
        table_2 = getEntity(table_2.getGuid());
        process_1 = getEntity(process_1.getGuid());


        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(process_1.getGuid(), false);
        verifyESHasLineage(table_2.getGuid(), true);

        LOG.info(">> testHasLineage");
    }

    private static void testHasLineage_1() throws Exception {
        LOG.info(">> testHasLineage_1");

        EntityMutationResponse response;
        AtlasEntity table_0, table_1, table_2, process_0, process_1;

        table_0 = createCustomEntity(TYPE_TABLE, "table_0");
        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        table_2 = createCustomEntity(TYPE_TABLE, "table_2");

        /*
         *
         * table_0 -> process_0 -> table_1 -> process_1 -> table_2
         *
         * */


        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_0");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_0 = getEntity(response.getCreatedEntities().get(0).getGuid());

        extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        /*
         * table_0 -> process_0 -> table_1 -> process_1 -> table_2
         *
         * If Table2 gets deleted
         *   If Table1 is connected to any active asset via process
         *       __hasLineage for Table1 -> true
         *       __hasLineage for Table2 -> true
         *       __hasLineage for process_0 -> null
         * */

        deleteEntities(Collections.singletonList(table_2.getGuid()));

        Thread.sleep(2000);

        table_0 = getEntity(table_0.getGuid());
        table_1 = getEntity(table_1.getGuid());
        table_2 = getEntity(table_2.getGuid());
        process_0 = getEntity(process_0.getGuid());
        process_1 = getEntity(process_1.getGuid());


        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);

        verifyESHasLineage(process_0.getGuid(), true);
        verifyESHasLineage(process_1.getGuid(), false);

        LOG.info(">> testHasLineage_1");
    }


    private static void testHasLineage_not_connected_0() throws Exception {
        LOG.info(">> testHasLineage_not_connected_0");


        EntityMutationResponse response;
        AtlasEntity table_1, table_2, process_1;

        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        table_2 = createCustomEntity(TYPE_TABLE, "table_2");

        /*
         *
         * table_1 -> process_0 -> table_2
         *
         * */

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        /*
         * table_1 -> process_1 -> table_2
         *
         * If ProcessA get deleted
         *   - If Table1 is not connected to any other active asset via process
         *       __hasLineage for Table1 -> null
         *   - If Table2 is not connected to any other active asset via process
         *       __hasLineage for Table2 -> null
         *
         *       __hasLineage for process_1 -> true
         *
         * */
        deleteEntities(Collections.singletonList(process_1.getGuid()));

        Thread.sleep(2000);

        table_1 = getEntity(table_1.getGuid());
        table_2 = getEntity(table_2.getGuid());
        process_1 = getEntity(process_1.getGuid());

        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(table_2.getGuid(), false);

        LOG.info(">> testHasLineage_not_connected_0");
    }


    private static void testHasLineage_not_connected_1() throws Exception {
        LOG.info(">> testHasLineage_not_connected_1");

        EntityMutationResponse response;
        AtlasEntity table_0, table_1, table_2, table_3, process_0, process_1, process_2;

        table_0 = createCustomEntity(TYPE_TABLE, "table_0");
        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        table_2 = createCustomEntity(TYPE_TABLE, "table_2");
        table_3 = createCustomEntity(TYPE_TABLE, "table_3");

        /*
         *
         * table_0 -> process_0 -> table_1 -> process_1 -> table_2 -> process_2 -> table_3
         *
         * */
        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_0");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_0 = getEntity(response.getCreatedEntities().get(0).getGuid());

        extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        extInfo = getAtlasEntity(TYPE_PROCESS, "process_2");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_3.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_2 = getEntity(response.getCreatedEntities().get(0).getGuid());

        /*
         * table_0 -> process_0 -> table_1 -> process_1 -> table_2 -> process_2 -> table_3
         *
         * If Process1 get deleted
         * - If Table1 is connected to any other active asset via process
         *      - __hasLineage for Table1 -> true
         * - If Table2 is connected to any other active asset via process
         *      - __hasLineage for Table2 -> true

         * */
        deleteEntities(Collections.singletonList(process_1.getGuid()));

        Thread.sleep(2000);

        table_0 = getEntity(table_0.getGuid());
        table_1 = getEntity(table_1.getGuid());
        table_2 = getEntity(table_2.getGuid());
        process_0 = getEntity(process_0.getGuid());
        process_1 = getEntity(process_1.getGuid());


        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);
        verifyESHasLineage(table_3.getGuid(), true);

        verifyESHasLineage(process_0.getGuid(), true);
        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(process_2.getGuid(), true);

        LOG.info(">> testHasLineage_not_connected_1");
    }

    private static void tableToView() throws Exception {
        LOG.info(">> tableToView");
        //Create Lineage with an input as a Table and an output as a View.

        //Now SOFT delete Process entity, __hasLineage will be false for Table, View and deleted Process.

        EntityMutationResponse response;
        AtlasEntity table_1, view_2, process_1;

        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        view_2 = createCustomEntity(TYPE_VIEW, "view_2");

        /*
         *
         * table_1 -> process_0 -> view_2
         *
         * */

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, view_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        /*
         * Now SOFT delete Process entity, __hasLineage will be false for Table, View and deleted Process.
         *  table_1 -> process_1 -> view_2
         *       __hasLineage for Table1 -> null
         *       __hasLineage for view_2 -> null
         *       __hasLineage for process_1 -> true
         *
         * */
        deleteEntities(Collections.singletonList(process_1.getGuid()));

        Thread.sleep(2000);

        table_1 = getEntity(table_1.getGuid());
        view_2 = getEntity(view_2.getGuid());
        process_1 = getEntity(process_1.getGuid());

        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(view_2.getGuid(), false);


        LOG.info(">> tableToView");
    }

    private static void tableToView_deleteRelationships() throws Exception {
        LOG.info(">> tableToView_deleteRelationships");
        //Create Lineage with one input as Table and output as View. All 3 three Assets , Table , View and Process, should be set with __hasLineage = true.

        //Now delete relationship between Table-Process and Process-View , __hasLineage will be false for Table and View

        EntityMutationResponse response;
        AtlasEntity table_1, view_2, process_1;

        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        view_2 = createCustomEntity(TYPE_VIEW, "view_2");

        /*
         *
         * table_1 -> process_0 -> view_2
         *
         * */

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, view_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        /*
         * Now delete relationship between Table_1 -> Process and Process -> View , __hasLineage will be false for Table and View
         *  table_1 -> process_1 -> view_2
         *       __hasLineage for Table1 -> null
         *       __hasLineage for Table2 -> null
         *       __hasLineage for process_1 -> true
         *
         * */

        List<HashMap> inputs = (List<HashMap>) process_1.getRelationshipAttribute(INPUTS);
        String relationshipGuid = (String) inputs.get(0).get("relationshipGuid");

        deleteRelationshipByGuid(relationshipGuid);
        Thread.sleep(2000);


        verifyESHasLineage(process_1.getGuid(), false);
        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(view_2.getGuid(), false);



        List<HashMap> outputs = (List<HashMap>) process_1.getRelationshipAttribute(OUTPUTS);
        relationshipGuid = (String) outputs.get(0).get("relationshipGuid");

        deleteRelationshipByGuid(relationshipGuid);


        verifyESHasLineage(process_1.getGuid(), false);
        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(view_2.getGuid(), false);

        LOG.info(">> tableToView_deleteRelationships");
    }

    private static void tableToView_deleteRelationships_singleRequest() throws Exception {
        LOG.info(">> tableToView_deleteRelationships_singleRequest");
        //Create Lineage with one input as Table and output as View. All 3 three Assets , Table , View and Process, should be set with __hasLineage = true.

        //Now delete relationship between Table-Process and Process-View , __hasLineage will be false for Table and View

        EntityMutationResponse response;
        AtlasEntity table_1, view_2, process_1;

        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        view_2 = createCustomEntity(TYPE_VIEW, "view_2");

        /*
         *
         * table_1 -> process_0 -> view_2
         *
         * */

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, view_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        /*
         * Now delete relationship between Table_1 -> Process and Process -> View , __hasLineage will be false for Table and View
         *  table_1 -> process_1 -> view_2
         *       __hasLineage for Table1 -> null
         *       __hasLineage for Table2 -> null
         *       __hasLineage for process_1 -> true
         *
         * */

        List<String> relGuids = new ArrayList<>();
        List<HashMap> inputs = (List<HashMap>) process_1.getRelationshipAttribute(INPUTS);
        relGuids.add((String) inputs.get(0).get("relationshipGuid"));

        List<HashMap> outputs = (List<HashMap>) process_1.getRelationshipAttribute(OUTPUTS);
        relGuids.add((String) outputs.get(0).get("relationshipGuid"));

        deleteRelationshipByGuids(relGuids);
        Thread.sleep(2000);


        verifyESHasLineage(process_1.getGuid(), false);
        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(view_2.getGuid(), false);

        LOG.info(">> tableToView_deleteRelationships_singleRequest");
    }

    private static void tableToView_deleteTable() throws Exception {
        LOG.info(">> tableToView_deleteTable");
        //Create Lineage with one input as Table and output as View. All 3 three Assets , Table , View and Process, should be set with __hasLineage = true.

        //Delete table entity, __hasLineage will remain same for table and turn to false for view

        EntityMutationResponse response;
        AtlasEntity table_1, table_2, view_2, process_1;

        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        table_2 = createCustomEntity(TYPE_TABLE, "table_2");
        view_2 = createCustomEntity(TYPE_VIEW, "view_2");

        /*
         *
         * table_1, table_2 -> process_0 -> view_2
         *
         * */

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid(), table_2.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, view_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        /*
         * Delete table_1 entity, __hasLineage will remain same for table and turn to false for view
         *  table_1, table_2 -> process_1 -> view_2
         *       __hasLineage for Table1 -> true
         *       __hasLineage for process_1 -> null
         *       __hasLineage for view2 -> null
         *
         * */
        deleteEntities(Collections.singletonList(table_1.getGuid()));
        Thread.sleep(2000);


        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);
        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(view_2.getGuid(), true);


        /*
         * Delete table_2 entity, __hasLineage will remain same for table and turn to false for view
         *  table_1, table_2 -> process_1 -> view_2
         *       __hasLineage for Table1 -> true
         *       __hasLineage for process_1 -> null
         *       __hasLineage for view2 -> null
         *
         * */
        deleteEntities(Collections.singletonList(table_2.getGuid()));
        Thread.sleep(2000);


        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);
        verifyESHasLineage(process_1.getGuid(), false);
        verifyESHasLineage(view_2.getGuid(), false);

        LOG.info(">> tableToView_deleteTable");
    }

    private static void tableToView_deleteView() throws Exception {
        LOG.info(">> tableToView_deleteView");
        //Create Lineage with one input as Table and output as View. All 3 three Assets , Table , View and Process, should be set with __hasLineage = true.

        //Delete View entity, __hasLineage will remain same for table and turn to false for view

        EntityMutationResponse response;
        AtlasEntity table_1, view_2, process_1;

        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        view_2 = createCustomEntity(TYPE_VIEW, "view_2");

        /*
         *
         * table_1 -> process_0 -> view_2
         *
         * */

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_v_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, view_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        /*
         * Delete view entity, __hasLineage will remain same for table and turn to false for view
         *  table_1 -> process_1 -> view_2
         *       __hasLineage for Table1 -> true
         *       __hasLineage for view2 -> null
         *       __hasLineage for process_1 -> null
         *
         * */
        deleteEntities(Collections.singletonList(view_2.getGuid()));
        Thread.sleep(2000);


        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(process_1.getGuid(), false);
        verifyESHasLineage(view_2.getGuid(), true);

        LOG.info(">> tableToView_deleteView");
    }

    private static void multipleInputs_singleOutput() throws Exception {
        LOG.info(">> multipleInputs_singleOutput");

        //Create Lineage with multiple tables with as input for a Process and a Table as Output.
        //Now remove one table from input relation will update entity, No change in hasLineage of any other asset.

        EntityMutationResponse response;
        AtlasEntity table_0, table_1, table_2, process_1;

        table_0 = createCustomEntity(TYPE_TABLE, "table_0");
        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        table_2 = createCustomEntity(TYPE_TABLE, "table_2");

        /*
         * table_0 ->
         * table_1 -> process_0 -> table_2
         *
         * */

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid(), table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());


        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);

        /*
         * Now remove table_0 from input relation will update entity, No change in hasLineage of any other asset.
         *
         * table_1 -> process_0 -> table_2
         *       __hasLineage for Table1 -> true
         *       __hasLineage for Table2 -> true
         *       __hasLineage for process_1 -> true
         *
         * */
        process_1.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        createEntity(process_1);

        Thread.sleep(2000);

        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(table_0.getGuid(), false);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);

        LOG.info(">> multipleInputs_singleOutput");
    }

    private static void multipleInputs_singleOutput_deleteTable() throws Exception {
        LOG.info(">> multipleInputs_singleOutput_deleteTable");

        //Create Lineage with multiple tables with as input for a Process and a Table as Output.
        //Now delete one table from input which is input to process, No change is hasLineage

        EntityMutationResponse response;
        AtlasEntity table_0, table_1, table_2, process_1;

        table_0 = createCustomEntity(TYPE_TABLE, "table_0");
        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        table_2 = createCustomEntity(TYPE_TABLE, "table_2");

        /*
         * table_0 ->
         * table_1 -> process_0 -> table_2
         *
         * */

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid(), table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());


        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);

        /*
         * Now delete one table from input which is input to process, No change is hasLineage
         *
         * table_1 -> process_0 -> table_2
         *       __hasLineage for Table1 -> true
         *       __hasLineage for Table2 -> true
         *       __hasLineage for process_1 -> true
         *
         * */
        deleteEntities(Collections.singletonList(table_0.getGuid()));

        Thread.sleep(2000);

        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);

        LOG.info(">> multipleInputs_singleOutput_deleteTable");
    }

    private static void tableToView_deleteProcess() throws Exception {
        LOG.info(">> tableToView_deleteProcess");

        //Create Lineage with one input as Table and output as View. All 3 three Assets , Table , View and Process, should be set with __hasLineage = true.
        //

        EntityMutationResponse response;
        AtlasEntity table_1, view_2, process_1;

        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        view_2 = createCustomEntity(TYPE_VIEW, "view_2");

        /*
         *
         * table_1 -> process_0 -> view_2
         *
         * */

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, view_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());


        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(view_2.getGuid(), true);

        /*
         * table_1 -> process_1 -> view_2
         *       __hasLineage for Table1 -> true
         *       __hasLineage for Table2 -> true
         *       __hasLineage for process_1 -> true
         *
         * */
        deleteEntities(Collections.singletonList(process_1.getGuid()));

        Thread.sleep(2000);

        table_1 = getEntity(table_1.getGuid());
        view_2 = getEntity(view_2.getGuid());
        process_1 = getEntity(process_1.getGuid());

        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(view_2.getGuid(), false);



        LOG.info(">> tableToView_deleteProcess");
    }

    private static void testHasLineageProperty_multipleInputsOutputs() throws Exception {
        LOG.info(">> testHasLineageProperty");

        EntityMutationResponse response;
        AtlasEntity table_0, table_1, table_3, table_4, col_5, col_6, col_7, col_8, process_0, column_process_1;

        table_0 = createCustomEntity(TYPE_TABLE, "table_0");
        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        table_3 = createCustomEntity(TYPE_TABLE, "table_3");
        table_4 = createCustomEntity(TYPE_TABLE, "table_4");

        col_5 = createCustomEntity(TYPE_COLUMN, "col_5");
        col_6 = createCustomEntity(TYPE_COLUMN, "col_6");
        col_7 = createCustomEntity(TYPE_COLUMN, "col_7");
        col_8 = createCustomEntity(TYPE_COLUMN, "col_8");


        /*
        * table_0 ->
        * table_1 -> process_0
        *
        * col_5 ->
        * col_6 -> column_process_1
        * */

        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_0");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid(), table_1.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_0 = getEntity(response.getCreatedEntities().get(0).getGuid());

        extInfo = getAtlasEntity(TYPE_COLUMN_PROCESS, "column_process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, col_5.getGuid(), col_6.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        column_process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        verifyESHasLineage(process_0.getGuid(), false);
        verifyESHasLineage(table_0.getGuid(), false);
        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(table_3.getGuid(), false);
        verifyESHasLineage(table_4.getGuid(), false);

        verifyESHasLineage(column_process_1.getGuid(), false);
        verifyESHasLineage(col_5.getGuid(), false);
        verifyESHasLineage(col_6.getGuid(), false);
        verifyESHasLineage(col_7.getGuid(), false);
        verifyESHasLineage(col_8.getGuid(), false);

        //--------------------
        /*
         * table_0 ->              table_3
         * table_1 -> process_0 -> table_4
         *
         * col_5 ->                     col_7
         * col_6 -> column_process_1 -> col_8
         *
         * */

        process_0 = getEntity(process_0.getGuid());
        process_0.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_3.getGuid(), table_4.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(process_0));

        column_process_1 = getEntity(column_process_1.getGuid());
        column_process_1.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_7.getGuid(), col_8.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(column_process_1));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid(), true);
        verifyESHasLineage(column_process_1.getGuid(), true);
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_3.getGuid(), true);
        verifyESHasLineage(table_4.getGuid(), true);
        verifyESHasLineage(col_5.getGuid(), true);
        verifyESHasLineage(col_6.getGuid(), true);
        verifyESHasLineage(col_7.getGuid(), true);
        verifyESHasLineage(col_8.getGuid(), true);

        table_0 = getEntity(table_0.getGuid());


        //--------------------
        /*
         * table_0 ->              table_3
         * table_1 -> process_0 -> table_4
         *
         * col_5 ->                     col_7
         * col_6 -> column_process_1 -> col_8
         *
         * To
         *
         *              table_3
         * process_0 -> table_4
         *
         *                     col_7
         * column_process_1 -> col_8
         *
         *
         * */
        //remove inputs
        process_0 = getEntity(process_0.getGuid());
        process_0.setRelationshipAttribute(INPUTS, new ArrayList<>());
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(process_0));

        column_process_1 = getEntity(column_process_1.getGuid());
        column_process_1.setRelationshipAttribute(INPUTS, new ArrayList<>());
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(column_process_1));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid(), false);
        verifyESHasLineage(column_process_1.getGuid(), false);
        verifyESHasLineage(table_0.getGuid(), false);
        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(table_3.getGuid(), false);
        verifyESHasLineage(table_4.getGuid(), false);
        verifyESHasLineage(col_5.getGuid(), false);
        verifyESHasLineage(col_6.getGuid(), false);
        verifyESHasLineage(col_7.getGuid(), false);
        verifyESHasLineage(col_8.getGuid(), false);


        //--------------------
        /*
         *              table_3
         * process_0 -> table_4
         *
         *                     col_7
         * column_process_1 -> col_8
         *
         *
         * To
         *
         * process_0
         *
         * column_process_1
         *
         *
         * */
        //remove outputs
        process_0 = getEntity(process_0.getGuid());
        process_0.setRelationshipAttribute(INPUTS, new ArrayList<>());
        process_0.setRelationshipAttribute(OUTPUTS, new ArrayList<>());
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(process_0));


        column_process_1 = getEntity(column_process_1.getGuid());
        column_process_1.setRelationshipAttribute(INPUTS, new ArrayList<>());
        column_process_1.setRelationshipAttribute(OUTPUTS, new ArrayList<>());
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(column_process_1));


        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid(), false);
        verifyESHasLineage(column_process_1.getGuid(), false);
        verifyESHasLineage(table_0.getGuid(), false);
        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(table_3.getGuid(), false);
        verifyESHasLineage(table_4.getGuid(), false);
        verifyESHasLineage(col_5.getGuid(), false);
        verifyESHasLineage(col_6.getGuid(), false);
        verifyESHasLineage(col_7.getGuid(), false);
        verifyESHasLineage(col_8.getGuid(), false);


        //---------------------------
        //table_0 & table_1 add process_0 again
        //col_5 & col_6  add column_process_1 again
        //--------------------
        /*
         * process_0
         *
         * column_process_1
         *
         * To

         * table_0 ->
         * table_1 -> process_0
         *
         * col_5 ->
         * col_6 -> column_process_1
         *
         * */


        table_0 = getEntity(table_0.getGuid());
        table_1 = getEntity(table_1.getGuid());
        table_0.setRelationshipAttribute(INPUTS_TO_P, getObjectIdsAsList(TYPE_PROCESS, process_0.getGuid()));
        table_1.setRelationshipAttribute(INPUTS_TO_P, getObjectIdsAsList(TYPE_PROCESS, process_0.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(table_0));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(table_1));

        col_5 = getEntity(col_5.getGuid());
        col_6 = getEntity(col_6.getGuid());
        col_5.setRelationshipAttribute(INPUTS_TO_P, getObjectIdsAsList(TYPE_PROCESS, column_process_1.getGuid()));
        col_6.setRelationshipAttribute(INPUTS_TO_P, getObjectIdsAsList(TYPE_PROCESS, column_process_1.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(col_5));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(col_6));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid(), false);
        verifyESHasLineage(column_process_1.getGuid(), false);
        verifyESHasLineage(table_0.getGuid(), false);
        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(table_3.getGuid(), false);
        verifyESHasLineage(table_4.getGuid(), false);
        verifyESHasLineage(col_5.getGuid(), false);
        verifyESHasLineage(col_6.getGuid(), false);
        verifyESHasLineage(col_7.getGuid(), false);
        verifyESHasLineage(col_8.getGuid(), false);

        //---------------------------
        //table_0 & table_1 add to process_0 again
        //col_5 & col_6 add to column_process_1 again
        /*
         * table_0 ->
         * table_1 -> process_0
         *
         * col_5 ->
         * col_6 -> column_process_1
         *
         * To
         *
         * table_0 ->           -> table_3
         * table_1 -> process_0 -> table_4
         *
         * col_5 ->                  -> col_7
         * col_6 -> column_process_1 -> col_8
         * */

        table_3 = getEntity(table_3.getGuid());
        table_4 = getEntity(table_4.getGuid());
        table_3.setRelationshipAttribute(OUTPUTS_FROM_P, getObjectIdsAsList(TYPE_PROCESS, process_0.getGuid()));
        table_4.setRelationshipAttribute(OUTPUTS_FROM_P, getObjectIdsAsList(TYPE_PROCESS, process_0.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(table_3));
        Thread.sleep(2000);
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(table_4));

        col_7 = getEntity(col_7.getGuid());
        col_8 = getEntity(col_8.getGuid());
        col_7.setRelationshipAttribute(OUTPUTS_FROM_P, getObjectIdsAsList(TYPE_PROCESS, column_process_1.getGuid()));
        col_8.setRelationshipAttribute(OUTPUTS_FROM_P, getObjectIdsAsList(TYPE_PROCESS, column_process_1.getGuid()));
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(col_7));
        Thread.sleep(2000);
        createEntity(new AtlasEntity.AtlasEntityWithExtInfo(col_8));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid(), true);
        verifyESHasLineage(column_process_1.getGuid(), true);
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_3.getGuid(), true);
        verifyESHasLineage(table_4.getGuid(), true);
        verifyESHasLineage(col_5.getGuid(), true);
        verifyESHasLineage(col_6.getGuid(), true);
        verifyESHasLineage(col_7.getGuid(), true);
        verifyESHasLineage(col_8.getGuid(), true);


        LOG.info("<< testHasLineageProperty");
    }

    private static void testHasLineage_deleteProcesses_singleRequest() throws Exception {
        LOG.info(">> testHasLineage_deleteProcesses_singleRequest");

        EntityMutationResponse response;
        AtlasEntity table_1, table_2, table_3, process_1, process_2;

        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        table_2 = createCustomEntity(TYPE_TABLE, "table_2");
        table_3 = createCustomEntity(TYPE_TABLE, "table_3");

        /*
         *
         * table_1 -> process_1 -> table_2 -> process_2 -> table_3
         *
         * */
        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        extInfo = getAtlasEntity(TYPE_PROCESS, "process_2");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_3.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_2 = getEntity(response.getCreatedEntities().get(0).getGuid());

        /*
         *  table_1 -> process_1 -> table_2 -> process_2 -> table_3
         *  Delete process_1, process_2 in single request

         * */
        deleteEntities(Arrays.asList(process_1.getGuid(), process_2.getGuid()));

        Thread.sleep(2000);

        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(table_2.getGuid(), false);
        verifyESHasLineage(table_3.getGuid(), false);

        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(process_2.getGuid(), true);

        LOG.info(">> testHasLineage_deleteProcesses_singleRequest");
    }

    private static void testHasLineage_longLineage_deleteProcesses_singleRequest() throws Exception {
        LOG.info(">> testHasLineage__longLineage_deleteProcesses_singleRequest");

        EntityMutationResponse response;
        AtlasEntity table_0, table_1, table_2, table_3, process_0, process_1, process_2;

        table_0 = createCustomEntity(TYPE_TABLE, "table_0");
        table_1 = createCustomEntity(TYPE_TABLE, "table_1");
        table_2 = createCustomEntity(TYPE_TABLE, "table_2");
        table_3 = createCustomEntity(TYPE_TABLE, "table_3");

        /*
         *
         * table_0 -> process_0 -> table_1 -> process_1 -> table_2 -> process_2 -> table_3
         *
         * */
        AtlasEntity.AtlasEntityWithExtInfo extInfo = getAtlasEntity(TYPE_PROCESS, "process_0");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_0 = getEntity(response.getCreatedEntities().get(0).getGuid());

        extInfo = getAtlasEntity(TYPE_PROCESS, "process_1");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_1 = getEntity(response.getCreatedEntities().get(0).getGuid());

        extInfo = getAtlasEntity(TYPE_PROCESS, "process_2");
        extInfo.getEntity().setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        extInfo.getEntity().setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_3.getGuid()));
        response = createEntity(extInfo);

        Thread.sleep(2000);
        process_2 = getEntity(response.getCreatedEntities().get(0).getGuid());

        /*
         * table_0 -> process_0 -> table_1 -> process_1 -> table_2 -> process_2 -> table_3
         *  Delete process_1, process_2 in single request

         * */
        deleteEntities(Arrays.asList(process_1.getGuid(), process_2.getGuid()));

        Thread.sleep(2000);

        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), false);
        verifyESHasLineage(table_3.getGuid(), false);

        verifyESHasLineage(process_0.getGuid(), true);
        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(process_2.getGuid(), true);

        LOG.info(">> testHasLineage__longLineage_deleteProcesses_singleRequest");
    }




    private static void testHasLineagePropertyBulk() throws Exception {
        LOG.info(">> testHasLineagePropertyBulk");

        EntityMutationResponse response;
        AtlasEntity.AtlasEntitiesWithExtInfo extInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        AtlasEntity table_0, table_1, table_2, col_0, col_1, process_0, process_1, column_process_3;

        table_0 = getAtlasEntity(TYPE_TABLE, "table_0").getEntity();
        table_0.setGuid("-1");
        table_1 = getAtlasEntity(TYPE_TABLE, "table_1").getEntity();
        table_1.setGuid("-2");
        table_2 = getAtlasEntity(TYPE_TABLE, "table_2").getEntity();
        table_2.setGuid("-3");
        col_0 = getAtlasEntity(TYPE_COLUMN, "col_0").getEntity();
        col_0.setGuid("-51");
        col_1 = getAtlasEntity(TYPE_COLUMN, "col_1").getEntity();
        col_1.setGuid("-52");
        extInfo.addEntity(table_0);
        extInfo.addEntity(table_1);
        extInfo.addEntity(table_2);
        extInfo.addEntity(col_0);
        extInfo.addEntity(col_1);

        process_0 = getAtlasEntity(TYPE_PROCESS, "process_0").getEntity();
        process_0.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid()));
        process_0.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        process_0.setGuid("-4");

        process_1 = getAtlasEntity(TYPE_PROCESS, "process_1").getEntity();
        process_1.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_1.getGuid()));
        process_1.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid()));
        process_1.setGuid("-5");

        column_process_3 = getAtlasEntity(TYPE_COLUMN_PROCESS, "column_process_3").getEntity();
        column_process_3.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_COLUMN, col_0.getGuid()));
        column_process_3.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_1.getGuid()));
        column_process_3.setGuid("-53");

        extInfo.addEntity(process_0);
        extInfo.addEntity(process_1);
        extInfo.addEntity(column_process_3);

        response = createEntitiesBulk(extInfo);

        table_0 = getEntity(response.getGuidAssignments().get("-1"));
        table_1 = getEntity(response.getGuidAssignments().get("-2"));
        table_2 = getEntity(response.getGuidAssignments().get("-3"));
        process_0 = getEntity(response.getGuidAssignments().get("-4"));
        process_1 = getEntity(response.getGuidAssignments().get("-5"));
        col_0 = getEntity(response.getGuidAssignments().get("-51"));
        col_1 = getEntity(response.getGuidAssignments().get("-52"));
        column_process_3 = getEntity(response.getGuidAssignments().get("-53"));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid(), true);
        verifyESHasLineage(process_1.getGuid(), true);
        verifyESHasLineage(column_process_3.getGuid(), true);
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);
        verifyESHasLineage(col_0.getGuid(), true);
        verifyESHasLineage(col_1.getGuid(), true);

        //remove inputs & outputs from process_0, process_1 & column_process_3
        extInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();

        process_0.setRelationshipAttribute(INPUTS, new ArrayList<>());
        process_0.setRelationshipAttribute(OUTPUTS, new ArrayList<>());
        process_1.setRelationshipAttribute(INPUTS, new ArrayList<>());
        process_1.setRelationshipAttribute(OUTPUTS, new ArrayList<>());
        column_process_3.setRelationshipAttribute(INPUTS, new ArrayList<>());
        column_process_3.setRelationshipAttribute(OUTPUTS, new ArrayList<>());

        extInfo.addEntity(process_0);
        extInfo.addEntity(process_1);
        extInfo.addEntity(column_process_3);

        response = createEntitiesBulk(extInfo);

        Thread.sleep(2000);
        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(table_2.getGuid(), false);
        verifyESHasLineage(table_0.getGuid(), false);
        verifyESHasLineage(col_0.getGuid(), false);
        verifyESHasLineage(col_1.getGuid(), false);
        verifyESHasLineage(process_1.getGuid(), false);
        verifyESHasLineage(process_0.getGuid(), false);
        verifyESHasLineage(column_process_3.getGuid(), false);


        //add table_0 as input to process_1
        //add col_0 as input to column_process_3
        extInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();

        process_1 = getEntity(process_1.getGuid());
        process_1.getAttributes().remove(OUTPUTS);
        process_1.getRelationshipAttributes().remove(OUTPUTS);
        process_1.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid()));
        extInfo.addEntity(process_1);

        column_process_3 = getEntity(column_process_3.getGuid());
        column_process_3.getAttributes().remove(OUTPUTS);
        column_process_3.getRelationshipAttributes().remove(OUTPUTS);
        column_process_3.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_COLUMN, col_0.getGuid()));
        extInfo.addEntity(column_process_3);

        response = createEntitiesBulk(extInfo);

        Thread.sleep(2000);
        verifyESHasLineage(table_1.getGuid(), false);
        verifyESHasLineage(table_2.getGuid(), false);
        verifyESHasLineage(table_0.getGuid(), false);
        verifyESHasLineage(col_0.getGuid(), false);
        verifyESHasLineage(col_1.getGuid(), false);
        verifyESHasLineage(process_1.getGuid(), false);
        verifyESHasLineage(process_0.getGuid(), false);
        verifyESHasLineage(column_process_3.getGuid(), false);


        LOG.info("<< testHasLineagePropertyBulk");
    }

    private static void testHasLineageHideProcess() throws Exception {
        LOG.info(">> testHasLineageHideProcess");

        EntityMutationResponse response;
        AtlasEntity.AtlasEntitiesWithExtInfo extInfo = new AtlasEntity.AtlasEntitiesWithExtInfo();
        AtlasEntity table_0, table_1, table_2, table_3, col_0, col_1, process_0, process_1, column_process_3;

        AtlasLineageInfo lineageInfo;
        Set<AtlasLineageInfo.LineageRelation> relations;
        Map<String, AtlasEntityHeader> guidEntityMap;

        table_0 = getAtlasEntity(TYPE_TABLE, "table_0").getEntity();
        table_0.setGuid("-1");
        table_1 = getAtlasEntity(TYPE_TABLE, "table_1").getEntity();
        table_1.setGuid("-2");
        table_2 = getAtlasEntity(TYPE_TABLE, "table_2").getEntity();
        table_2.setGuid("-3");
        table_3 = getAtlasEntity(TYPE_TABLE, "table_3").getEntity();
        table_3.setGuid("-4");
        col_0 = getAtlasEntity(TYPE_TABLE, "col_0").getEntity();
        col_0.setGuid("-51");
        col_1 = getAtlasEntity(TYPE_TABLE, "col_1").getEntity();
        col_1.setGuid("-52");

        extInfo.addEntity(table_0);
        extInfo.addEntity(table_1);
        extInfo.addEntity(table_2);
        extInfo.addEntity(table_3);
        extInfo.addEntity(col_0);
        extInfo.addEntity(col_1);

        process_0 = getAtlasEntity(TYPE_PROCESS, "process_0").getEntity();
        process_0.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_TABLE, table_0.getGuid(), table_1.getGuid()));
        process_0.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_TABLE, table_2.getGuid(), table_3.getGuid()));
        process_0.setGuid("-21");

        column_process_3 = getAtlasEntity(TYPE_COLUMN_PROCESS, "column_process_3").getEntity();
        column_process_3.setRelationshipAttribute(INPUTS, getObjectIdsAsList(TYPE_COLUMN, col_0.getGuid()));
        column_process_3.setRelationshipAttribute(OUTPUTS, getObjectIdsAsList(TYPE_COLUMN, col_1.getGuid()));
        column_process_3.setGuid("-53");

        process_1 = getAtlasEntity(TYPE_PROCESS, "process_1").getEntity();
        process_1.setGuid("-22");

        extInfo.addEntity(process_0);
        extInfo.addEntity(process_1);
        extInfo.addEntity(column_process_3);

        response = createEntitiesBulk(extInfo);

        table_0 = getEntity(response.getGuidAssignments().get("-1"));
        table_1 = getEntity(response.getGuidAssignments().get("-2"));
        table_2 = getEntity(response.getGuidAssignments().get("-3"));
        table_3 = getEntity(response.getGuidAssignments().get("-4"));
        col_0 = getEntity(response.getGuidAssignments().get("-51"));
        col_1 = getEntity(response.getGuidAssignments().get("-52"));
        process_0 = getEntity(response.getGuidAssignments().get("-21"));
        process_1 = getEntity(response.getGuidAssignments().get("-22"));
        column_process_3 = getEntity(response.getGuidAssignments().get("-53"));

        Thread.sleep(2000);
        verifyESHasLineage(process_0.getGuid(), true);
        verifyESHasLineage(process_1.getGuid(), false);
        verifyESHasLineage(column_process_3.getGuid(), true);
        verifyESHasLineage(table_0.getGuid(), true);
        verifyESHasLineage(table_1.getGuid(), true);
        verifyESHasLineage(table_2.getGuid(), true);
        verifyESHasLineage(table_3.getGuid(), true);
        verifyESHasLineage(col_0.getGuid(), true);
        verifyESHasLineage(col_1.getGuid(), true);

        lineageInfo = getLineageInfo(table_0.getGuid(), AtlasLineageInfo.LineageDirection.BOTH, 3, false);
        relations = lineageInfo.getRelations();
        guidEntityMap = lineageInfo.getGuidEntityMap();
        assertNotNull(relations);
        assertNotNull(guidEntityMap);

        assertEquals(3, relations.size());
        assertEquals(4, guidEntityMap.size());

        Set<String> guids = relations.stream().map(x-> x.getFromEntityId()).collect(Collectors.toSet());
        guids.addAll(relations.stream().map(x-> x.getToEntityId()).collect(Collectors.toSet()));

        assertTrue(guids.contains(process_0.getGuid()));
        assertTrue(guidEntityMap.keySet().contains(process_0.getGuid()));

        lineageInfo = getLineageInfo(col_0.getGuid(), AtlasLineageInfo.LineageDirection.BOTH, 3, false);
        relations = lineageInfo.getRelations();
        guidEntityMap = lineageInfo.getGuidEntityMap();
        assertNotNull(relations);
        assertNotNull(guidEntityMap);

        assertEquals(2, relations.size());
        assertEquals(3, guidEntityMap.size());

        guids = relations.stream().map(x-> x.getFromEntityId()).collect(Collectors.toSet());
        guids.addAll(relations.stream().map(x-> x.getToEntityId()).collect(Collectors.toSet()));

        assertTrue(guids.contains(column_process_3.getGuid()));
        assertTrue(guidEntityMap.keySet().contains(column_process_3.getGuid()));


        lineageInfo = getLineageInfo(table_0.getGuid(), AtlasLineageInfo.LineageDirection.BOTH, 3, true);
        relations = lineageInfo.getRelations();
        guidEntityMap = lineageInfo.getGuidEntityMap();
        assertNotNull(relations);
        assertEquals(2, relations.size());
        guids = relations.stream().map(x-> x.getFromEntityId()).collect(Collectors.toSet());
        guids.addAll(relations.stream().map(x-> x.getToEntityId()).collect(Collectors.toSet()));

        assertFalse(guids.contains(process_0.getGuid()));

        assertNotNull(guidEntityMap);
        assertEquals(3, guidEntityMap.size());
        assertFalse(guidEntityMap.keySet().contains(process_0.getGuid()));


        lineageInfo = getLineageInfo(col_0.getGuid(), AtlasLineageInfo.LineageDirection.BOTH, 3, true);
        relations = lineageInfo.getRelations();
        guidEntityMap = lineageInfo.getGuidEntityMap();
        assertNotNull(relations);
        assertEquals(1, relations.size());
        guids = relations.stream().map(x-> x.getFromEntityId()).collect(Collectors.toSet());
        guids.addAll(relations.stream().map(x-> x.getToEntityId()).collect(Collectors.toSet()));

        assertFalse(guids.contains(column_process_3.getGuid()));

        assertNotNull(guidEntityMap);
        assertEquals(2, guidEntityMap.size());
        assertFalse(guidEntityMap.keySet().contains(process_0.getGuid()));


        //fetch with process guid -> should fail
        boolean failed = false;
        try {
            lineageInfo = getLineageInfo(process_0.getGuid(), AtlasLineageInfo.LineageDirection.BOTH, 3, true);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(),404);
            assertTrue(exception.getMessage().contains("ATLAS-404-00-017"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }

        failed = false;
        try {
            lineageInfo = getLineageInfo(column_process_3.getGuid(), AtlasLineageInfo.LineageDirection.BOTH, 3, true);
        } catch (Exception exception) {
            //assertEquals(exception.getStatus().getStatusCode(),404);
            assertTrue(exception.getMessage().contains("ATLAS-404-00-017"));
            failed = true;
        } finally {
            if (!failed) {
                throw new Exception("This test should have failed");
            }
        }
        LOG.info("<< testHasLineageHideProcess");
    }

    //helper methods

    private static void sleep() throws InterruptedException {
        Thread.sleep(2000);
    }

    private static AtlasEntity createCustomEntity(String typeName, String entityName) throws Exception {

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = getAtlasEntity(typeName, entityName);

        return getEntity(TestUtil.createEntity(entityWithExtInfo).getCreatedEntities().get(0).getGuid());

    }

    private static AtlasEntity.AtlasEntityWithExtInfo getAtlasEntity(String typeName, String entityName) {
        AtlasEntity entity = new AtlasEntity(typeName);
        entityName = StringUtils.isNotEmpty(entityName) ? entityName : getRandomName();
        entity.setAttribute(NAME, entityName);
        entity.setAttribute(QUALIFIED_NAME, CONNECTION_PREFIX + entityName + "_" + getRandomName());
        entity.setAttribute(CONNECTION_QUALIFIED_NAME, CONNECTION_PREFIX);

        AtlasEntity.AtlasEntityWithExtInfo entityWithExtInfo = new AtlasEntity.AtlasEntityWithExtInfo();
        entityWithExtInfo.setEntity(entity);

        return entityWithExtInfo;
    }

    private static void verifyEntityHasLineage(String entityGuid, boolean expected) throws Exception {
        AtlasEntity tempEntity = getEntity(entityGuid);
        assertNotNull(tempEntity.getAttribute(HAS_LINEAGE));

        if (expected) {
            assertEquals("true", tempEntity.getAttribute("hasLineage").toString());
        } else {
            assertEquals("false", tempEntity.getAttribute("hasLineage").toString());
        }
    }

    private static void verifyESHasLineage(String entityGuid, boolean expected) {

        SearchHit[] searchHit = ESUtil.searchWithGuid(entityGuid).getHits().getHits();

        assertNotNull(searchHit);
        assertEquals(1, searchHit.length);

        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();

            assertNotNull(sourceAsMap.get(HAS_LINEAGE));
            boolean value = (boolean) sourceAsMap.get(HAS_LINEAGE);

            if (expected) {
                assertTrue(value);
            } else {
                assertFalse(value);
            }
        }
    }
}