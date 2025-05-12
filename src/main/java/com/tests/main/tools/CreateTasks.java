package com.tests.main.tools;

import com.tests.main.utils.ESUtil;
import org.apache.atlas.model.instance.AtlasClassification;
import org.apache.atlas.model.instance.ClassificationAssociateRequest;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.tests.main.utils.TestUtil.*;
import static org.junit.Assert.assertNotNull;

public class CreateTasks {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTasks.class);

    static int from = 0;
    static String tagName;
    static int fetchMax = 30;
    static int fetchMaxColumnPerReq = 30;

    public static void main(String[] args) throws Exception {

        tagName = createClassification("tag_0" + getRandomName());

        try {
            long start = System.currentTimeMillis();
            perform();
            LOG.info("Completed in {} seconds", (System.currentTimeMillis() - start)/1000);
        } finally {
            ESUtil.close();
        }
    }

    private static void perform() throws Exception {
        int fetched = 0;
        boolean hasNext = true;

        while (hasNext && fetched < fetchMax) {
            List<String> guids = getColumnGuids(from, fetchMaxColumnPerReq);
            LOG.info("Found {} guids", guids.size());

            if (CollectionUtils.isEmpty(guids)) {
                break;
            }

            ClassificationAssociateRequest request = new ClassificationAssociateRequest();
            AtlasClassification tag = new AtlasClassification(tagName);
            tag.setPropagate(true);
            tag.setRemovePropagationsOnEntityDelete(true);
            request.setClassification(tag);
            request.setEntityGuids(guids);

            addClassificationsBulk(request);

            if (guids.size() < fetchMaxColumnPerReq) {
                hasNext = false;
            }
            fetched += guids.size();

            LOG.info("Attached tag to {} entities", fetched);

            from += fetchMaxColumnPerReq;
        }

        LOG.info("Performed");
    }

    private static List<String> getColumnGuids(int from, int size) {
        List<String> guids = new ArrayList<>();

        SearchResponse response = ESUtil.searchWithTypeName(TYPE_COLUMN, from, size);
        SearchHit[] searchHit = response.getHits().getHits();
        assertNotNull(searchHit);

        for (SearchHit hit : searchHit) {
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            guids.add((String) sourceAsMap.get("__guid"));
        }

        return guids;
    }
}
