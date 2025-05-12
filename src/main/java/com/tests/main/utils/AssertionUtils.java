package com.tests.main.utils;

import com.tests.main.KafkaMessage;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.tests.main.utils.TestUtil.TYPE_TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AssertionUtils {
    public enum RelationType {
        RELATION,
        RELATION_ADDED,
        RELATION_REMOVED;
    }

    public static class KafkaEventListRelationshipValidator {
        private KafkaMessage event;
        private RelationType relationType;

        private String relationName;
        private String expectedRelationTypeName;
        private String[] expectedRelationGuids;

        public KafkaEventListRelationshipValidator event(KafkaMessage event) {
            this.event = event;
            return this;
        }

        public KafkaEventListRelationshipValidator relationType(RelationType relationType) {
            this.relationType = relationType;
            return this;
        }

        public KafkaEventListRelationshipValidator expectRelationGuids(String... expectedRelationGuids) {
            this.expectedRelationGuids = expectedRelationGuids;
            return this;
        }

        public KafkaEventListRelationshipValidator forRelationName(String relationName) {
            this.relationName = relationName;
            return this;
        }

        public KafkaEventListRelationshipValidator expecteRelationTypeName(String expectedRelationTypeName) {
            this.expectedRelationTypeName = expectedRelationTypeName;
            return this;
        }

        public void validate() {
            Set<String> allGuids; List<Map> mutatedRelations = Collections.emptyList();
            assertTrue(StringUtils.isNotEmpty(relationName));

            switch (relationType) {
                case RELATION: mutatedRelations = ((List) event.getMutatedDetails().getRelationshipAttribute(relationName)); break;
                case RELATION_ADDED: mutatedRelations = ((List) event.getMutatedDetails().getAddedRelationshipAttributes().get(relationName)); break;
                case RELATION_REMOVED: mutatedRelations = ((List) event.getMutatedDetails().getRemovedRelationshipAttributes().get(relationName)); break;
            }

            if (expectedRelationGuids == null || expectedRelationGuids.length == 0) {
                assertNull(mutatedRelations);
            } else {
                assertEquals(expectedRelationGuids.length, mutatedRelations.size());

                allGuids = mutatedRelations.stream().map(x -> (String) x.get("guid")).collect(Collectors.toSet());

                Collection diff = CollectionUtils.subtract(allGuids, Arrays.asList(expectedRelationGuids));
                assertEquals(diff.toString(), 0, diff.size());
                assertEquals(expectedRelationGuids.length, mutatedRelations.stream().filter(x -> expectedRelationTypeName.equals(x.get("typeName"))).count());
            }
        }
    }

    public static class KafkaEventRelationshipValidator {
        private KafkaMessage event;
        private RelationType relationType;

        private String relationName;
        private String expectedRelationTypeName;
        private String expectedRelationGuid;

        public KafkaEventRelationshipValidator event(KafkaMessage event) {
            this.event = event;
            return this;
        }

        public KafkaEventRelationshipValidator relationType(RelationType relationType) {
            this.relationType = relationType;
            return this;
        }

        public KafkaEventRelationshipValidator expectedRelationGuid(String expectedRelationGuid) {
            this.expectedRelationGuid = expectedRelationGuid;
            return this;
        }

        public KafkaEventRelationshipValidator forRelationName(String relationName) {
            this.relationName = relationName;
            return this;
        }

        public KafkaEventRelationshipValidator expecteRelationTypeName(String expectedRelationTypeName) {
            this.expectedRelationTypeName = expectedRelationTypeName;
            return this;
        }

        public void validate() {
            Map mutatedRelation = Collections.emptyMap();
            assertTrue(StringUtils.isNotEmpty(relationName));

            switch (relationType) {
                case RELATION: mutatedRelation = ((Map) event.getMutatedDetails().getRelationshipAttribute(relationName)); break;
                case RELATION_ADDED: mutatedRelation = ((Map) event.getMutatedDetails().getAddedRelationshipAttributes().get(relationName)); break;
                case RELATION_REMOVED: mutatedRelation = ((Map) event.getMutatedDetails().getRemovedRelationshipAttributes().get(relationName)); break;
            }

            if (StringUtils.isEmpty(expectedRelationGuid)) {
                assertNull(mutatedRelation);
            } else {
                assertEquals(expectedRelationGuid, mutatedRelation.get("guid"));
                assertEquals(expectedRelationTypeName, mutatedRelation.get("typeName"));
            }
        }
    }
}
