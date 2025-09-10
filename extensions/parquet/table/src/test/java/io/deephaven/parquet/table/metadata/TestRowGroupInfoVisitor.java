//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.metadata;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;

import static java.util.Map.entry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

/**
 * Test serialization/deserialization of {@link RowGroupInfo} implementations. This test is intentionally in a different
 * package than {@link RowGroupInfo} so that we're able to verify we have access (not private, not package private) to
 * each method we would need for serialization/deserialization
 */
public class TestRowGroupInfoVisitor {
    /**
     * Assist in JSON serialization/deserialization of a {@link RowGroupInfo}
     */
    private static class RowGroup2JsonVisitor implements RowGroupInfo.Visitor<String> {
        /**
         * identify methods which will assist in deserialization of a {@link RowGroupInfo} instance based on a
         * {@link ObjectNode}, which has been previously serialized from a different {@link RowGroupInfo} instance
         */
        private static final Map<String, Function<ObjectNode, RowGroupInfo>> DESER_MAP = Map.ofEntries(
                entry(RowGroupInfo.SingleGroup.class.getSimpleName(), (node) -> RowGroupInfo.singleGroup()),
                entry(RowGroupInfo.MaxRows.class.getSimpleName(), RowGroup2JsonVisitor::constructMaxRows),
                entry(RowGroupInfo.MaxGroups.class.getSimpleName(), RowGroup2JsonVisitor::constructMaxGroups),
                entry(RowGroupInfo.ByGroups.class.getSimpleName(), RowGroup2JsonVisitor::constructByGroups));
        private static final String NAME_ATTR = "name";

        private final ObjectMapper mapper;

        private RowGroup2JsonVisitor() {
            this.mapper = new ObjectMapper();
        }

        @Override
        public String visit(final @NotNull RowGroupInfo.SingleGroup single) {
            final String jsonStr;
            try {
                // no properties to read; start with an empty node and add the type-specific name
                final ObjectNode jsonNode = mapper.createObjectNode();
                jsonNode.put(NAME_ATTR, single.getClass().getSimpleName());
                jsonStr = mapper.writeValueAsString(jsonNode);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }

            return jsonStr;
        }

        @Override
        public String visit(final @NotNull RowGroupInfo.MaxGroups maxGroups) {
            final String jsonStr;
            try {
                // read type-specific properties and add the type-specific name
                final ObjectNode jsonNode = mapper.valueToTree(maxGroups);
                jsonNode.put(NAME_ATTR, maxGroups.getClass().getSimpleName());
                jsonStr = mapper.writeValueAsString(jsonNode);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return jsonStr;
        }

        @Override
        public String visit(final @NotNull RowGroupInfo.MaxRows maxRows) {
            final String jsonStr;
            try {
                // read type-specific properties and add the type-specific name
                final ObjectNode jsonNode = mapper.valueToTree(maxRows);
                jsonNode.put(NAME_ATTR, maxRows.getClass().getSimpleName());
                jsonStr = mapper.writeValueAsString(jsonNode);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return jsonStr;
        }

        @Override
        public String visit(final @NotNull RowGroupInfo.ByGroups byGroups) {
            final String jsonStr;
            try {
                // read type-specific properties and add the type-specific name
                final ObjectNode jsonNode = mapper.createObjectNode();
                final ArrayNode groups = mapper.createArrayNode();
                byGroups.getGroups().forEach(groups::add);
                jsonNode.set("groups", groups);
                final OptionalLong maxRows = byGroups.getMaxRows();
                if (maxRows.isPresent())  {
                    jsonNode.put("maxRows", maxRows.getAsLong());
                }
                jsonNode.put(NAME_ATTR, byGroups.getClass().getSimpleName());
                jsonStr = mapper.writeValueAsString(jsonNode);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return jsonStr;
        }


        /**
         * Attempts to deserialize our serialized JSON string into a {@link RowGroupInfo} instance
         *
         * @param ser the serialized JSON string
         *
         * @return a new {@link RowGroupInfo} instance, which has been deserialized
         * @throws IOException should not happen in this test
         */
        private RowGroupInfo deserialize(final @NotNull String ser) throws IOException {
            final ObjectNode serNode = (ObjectNode) mapper.readTree(ser);
            final String name = serNode.get(NAME_ATTR).asText();
            final Function<ObjectNode, RowGroupInfo> constructor = DESER_MAP.get(name);
            assertNotNull(String.format("Know type <%s>", name), constructor);

            return constructor.apply(serNode);
        }

        /**
         * Deserialize a new {@link RowGroupInfo.MaxRows} instance, based on the serialized {@link ObjectNode}
         *
         * @param node a serialized {@link RowGroupInfo}, represented as an {@link ObjectNode}
         * @return a new deserialized {@link RowGroupInfo} instance
         */
        private static RowGroupInfo constructMaxRows(final @NotNull ObjectNode node) {
            return RowGroupInfo.maxRows(node.get("maxRows").asLong());
        }

        /**
         * Deserialize a new {@link RowGroupInfo.MaxGroups} instance, based on the serialized {@link ObjectNode}
         *
         * @param node a serialized {@link RowGroupInfo}, represented as an {@link ObjectNode}
         * @return a new deserialized {@link RowGroupInfo} instance
         */
        private static RowGroupInfo constructMaxGroups(final @NotNull ObjectNode node) {
            return RowGroupInfo.maxGroups(node.get("numRowGroups").asLong());
        }

        /**
         * Deserialize a new {@link RowGroupInfo.ByGroups} instance, based on the serialized {@link ObjectNode}
         *
         * @param node a serialized {@link RowGroupInfo}, represented as an {@link ObjectNode}
         * @return a new deserialized {@link RowGroupInfo} instance
         */
        private static RowGroupInfo constructByGroups(final @NotNull ObjectNode node) {
            final List<String> groups = new ArrayList<>();
            final Iterator<JsonNode> groupsNode = node.get("groups").elements();
            while (groupsNode.hasNext()) {
                groups.add(groupsNode.next().asText());
            }

            if (node.has("maxRows")) {
                return RowGroupInfo.byGroups(node.get("maxRows").asLong(), groups.toArray(new String[0]));
            } else {
                // could use the 2-param constructor, but it's nice to use both in the test ...
                return RowGroupInfo.byGroups(groups.toArray(new String[0]));
            }

        }
    }

    /**
     * A helper method to verify that ... we get an equivalent {@link RowGroupInfo} instance using the
     * {@link RowGroup2JsonVisitor}
     *
     * @param jsonVisitor a pre-constructed {@link RowGroup2JsonVisitor} instance
     * @param orig a pre-constructed {@link RowGroupInfo} instance
     */
    private void verifyJsonVisitor(final @NotNull RowGroup2JsonVisitor jsonVisitor, final @NotNull RowGroupInfo orig)
            throws IOException {
        // `orig.walk(...)` will serialize (to JSON, in this case). `jsonVisitor.deserialize(...)` will deserialize
        // that JSON, and construct a new `RowGroupInfo` instance, which should be equal to `orig`
        final RowGroupInfo deserialized = jsonVisitor.deserialize(orig.walk(jsonVisitor));
        assertEquals(String.format("%s serializes/deserializes", orig), orig, deserialized);
    }

    /**
     * Verify that ... each defined {@link RowGroupInfo} implementation is able to self-copy itself to a new instance,
     * which is equal to the original instance using JSON serialization/deserialization. Additionally, we ensure that we
     * are not able to copy a "custom" {@link RowGroupInfo} implementation
     */
    @Test
    public void testJsonVisitor() throws IOException {
        final RowGroup2JsonVisitor jsonVisitor = new RowGroup2JsonVisitor();

        verifyJsonVisitor(jsonVisitor, RowGroupInfo.singleGroup());
        verifyJsonVisitor(jsonVisitor, RowGroupInfo.maxRows(42));
        verifyJsonVisitor(jsonVisitor, RowGroupInfo.maxGroups(13));
        verifyJsonVisitor(jsonVisitor, RowGroupInfo.byGroups("a", "b"));
        verifyJsonVisitor(jsonVisitor, RowGroupInfo.byGroups(99, "c", "d"));

        final UnsupportedOperationException uoe = assertThrowsExactly(UnsupportedOperationException.class,
                () -> verifyJsonVisitor(jsonVisitor, new UnsupportedImpl()));
        assertEquals(String.format("Unknown %s type", RowGroupInfo.class.getCanonicalName()), uoe.getMessage());
    }


    /**
     * Assist in simple serialization/deserialization (duplication) of a {@link RowGroupInfo}
     */
    private static class RowGroup2SelfVisitor implements RowGroupInfo.Visitor<RowGroupInfo> {
        @Override
        public RowGroupInfo visit(final @NotNull RowGroupInfo.SingleGroup single) {
            return RowGroupInfo.singleGroup();
        }

        @Override
        public RowGroupInfo visit(final @NotNull RowGroupInfo.MaxGroups maxGroups) {
            return RowGroupInfo.maxGroups(maxGroups.getNumRowGroups());
        }

        @Override
        public RowGroupInfo visit(final @NotNull RowGroupInfo.MaxRows maxRows) {
            return RowGroupInfo.maxRows(maxRows.getMaxRows());
        }

        @Override
        public RowGroupInfo visit(final @NotNull RowGroupInfo.ByGroups byGroups) {
            final OptionalLong maxRows = byGroups.getMaxRows();
            return maxRows.isPresent() ? RowGroupInfo.byGroups(maxRows.getAsLong(), byGroups.getGroups()) :
                    RowGroupInfo.byGroups(byGroups.getGroups());
        }
    }

    /**
     * A helper method to verify that ... we get an equivalent {@link RowGroupInfo} instance using the
     * {@link RowGroup2JsonVisitor}
     *
     * @param selfVisitor a pre-constructed {@link RowGroup2SelfVisitor} instance
     * @param orig a pre-constructed {@link RowGroupInfo} instance
     */
    private void testVisitor(final @NotNull RowGroup2SelfVisitor selfVisitor, final @NotNull RowGroupInfo orig) {
        final RowGroupInfo fromWalk = orig.walk(selfVisitor);

        assertEquals("walked copy RowGroupInfo produces same RowGroupInfo", orig, fromWalk);
    }

    /**
     * Verify that ... each defined {@link RowGroupInfo} implementation is able to self-copy itself to a new instance,
     * which is equal to the original instance. Additionally, we ensure that we are not able to copy a "custom"
     * {@link RowGroupInfo} implementation
     */
    @Test
    public void testSelfVisitor() {
        final RowGroup2SelfVisitor selfVisitor = new RowGroup2SelfVisitor();

        testVisitor(selfVisitor, RowGroupInfo.singleGroup());
        testVisitor(selfVisitor, RowGroupInfo.maxGroups(24));
        testVisitor(selfVisitor, RowGroupInfo.maxRows(1500));
        testVisitor(selfVisitor, RowGroupInfo.byGroups("a", "b"));
        testVisitor(selfVisitor, RowGroupInfo.byGroups(200, "b", "c"));

        final UnsupportedOperationException uoe = assertThrowsExactly(UnsupportedOperationException.class,
                () -> testVisitor(selfVisitor, new UnsupportedImpl()));
        assertEquals(String.format("Unknown %s type", RowGroupInfo.class.getCanonicalName()), uoe.getMessage());
    }

    /**
     * a "custom" {@link RowGroupInfo}, which should cause an {@link UnsupportedOperationException} when we try to copy
     */
    private static class UnsupportedImpl implements RowGroupInfo {

        @Override
        public <T> T walk(final @NotNull Visitor<T> visitor) {
            throw new UnsupportedOperationException(
                    String.format("Unknown %s type", RowGroupInfo.class.getCanonicalName()));
        }
    }
}
