//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.engine.table.Table;
import io.deephaven.parquet.table.metadata.RowGroupInfo;
import io.deephaven.util.function.ThrowingConsumer;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.core.JsonProcessingException;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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
                entry(RowGroupInfo.SingleRowGroup.class.getSimpleName(), (node) -> RowGroupInfo.singleRowGroup()),
                entry(RowGroupInfo.SplitByMaxRows.class.getSimpleName(), RowGroup2JsonVisitor::constructMaxRows),
                entry(RowGroupInfo.SplitEvenly.class.getSimpleName(), RowGroup2JsonVisitor::constructSplitEvenly),
                entry(RowGroupInfo.SplitByGroups.class.getSimpleName(), RowGroup2JsonVisitor::constructByGroup));
        private static final String NAME_ATTR = "name";

        private final ObjectMapper mapper;

        private RowGroup2JsonVisitor() {
            this.mapper = new ObjectMapper();
        }

        @Override
        public String visit(final @NotNull RowGroupInfo.SingleRowGroup single) {
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
        public String visit(final @NotNull RowGroupInfo.SplitEvenly splitEvenly) {
            final String jsonStr;
            try {
                // read type-specific properties and add the type-specific name
                final ObjectNode jsonNode = mapper.valueToTree(splitEvenly);
                jsonNode.put(NAME_ATTR, splitEvenly.getClass().getSimpleName());
                jsonStr = mapper.writeValueAsString(jsonNode);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return jsonStr;
        }

        @Override
        public String visit(final @NotNull RowGroupInfo.SplitByMaxRows withMaxRows) {
            final String jsonStr;
            try {
                // read type-specific properties and add the type-specific name
                final ObjectNode jsonNode = mapper.valueToTree(withMaxRows);
                jsonNode.put(NAME_ATTR, withMaxRows.getClass().getSimpleName());
                jsonStr = mapper.writeValueAsString(jsonNode);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            return jsonStr;
        }

        @Override
        public String visit(final @NotNull RowGroupInfo.SplitByGroups byGroups) {
            final String jsonStr;
            try {
                // read type-specific properties and add the type-specific name
                final ObjectNode jsonNode = mapper.valueToTree(byGroups);
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
         * Deserialize a new {@link RowGroupInfo.SplitByMaxRows} instance, based on the serialized {@link ObjectNode}
         *
         * @param node a serialized {@link RowGroupInfo}, represented as an {@link ObjectNode}
         * @return a new deserialized {@link RowGroupInfo} instance
         */
        private static RowGroupInfo constructMaxRows(final @NotNull ObjectNode node) {
            return RowGroupInfo.withMaxRows(node.get("maxRows").asLong());
        }

        /**
         * Deserialize a new {@link RowGroupInfo.SplitEvenly} instance, based on the serialized {@link ObjectNode}
         *
         * @param node a serialized {@link RowGroupInfo}, represented as an {@link ObjectNode}
         * @return a new deserialized {@link RowGroupInfo} instance
         */
        private static RowGroupInfo constructSplitEvenly(final @NotNull ObjectNode node) {
            return RowGroupInfo.splitEvenly(node.get("numRowGroups").asLong());
        }

        /**
         * Deserialize a new {@link RowGroupInfo.SplitByGroups} instance, based on the serialized {@link ObjectNode}
         *
         * @param node a serialized {@link RowGroupInfo}, represented as an {@link ObjectNode}
         * @return a new deserialized {@link RowGroupInfo} instance
         */
        private static RowGroupInfo constructByGroup(final @NotNull ObjectNode node) {
            final long maxRows = node.get("maxRows").asLong();
            final List<String> groups = new ArrayList<>();
            final Iterator<JsonNode> groupsNode = node.get("groups").elements();
            while (groupsNode.hasNext()) {
                groups.add(groupsNode.next().asText());
            }

            if (maxRows == Long.MAX_VALUE) {
                // could use the 2-param constructor, but it's nice to use both in the test ...
                return RowGroupInfo.byGroup(groups.toArray(new String[0]));
            } else {
                return RowGroupInfo.byGroup(maxRows, groups.toArray(new String[0]));
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

        verifyJsonVisitor(jsonVisitor, RowGroupInfo.singleRowGroup());
        verifyJsonVisitor(jsonVisitor, RowGroupInfo.withMaxRows(42));
        verifyJsonVisitor(jsonVisitor, RowGroupInfo.splitEvenly(13));
        verifyJsonVisitor(jsonVisitor, RowGroupInfo.byGroup("a", "b"));
        verifyJsonVisitor(jsonVisitor, RowGroupInfo.byGroup(99, "c", "d"));

        final UnsupportedOperationException uoe = assertThrowsExactly(UnsupportedOperationException.class,
                () -> verifyJsonVisitor(jsonVisitor, new UnsupportedImpl()));
        assertEquals(String.format("Unknown %s type", RowGroupInfo.class.getCanonicalName()), uoe.getMessage());
    }


    /**
     * Assist in simple serialization/deserialization (duplication) of a {@link RowGroupInfo}
     */
    private static class RowGroup2SelfVisitor implements RowGroupInfo.Visitor<RowGroupInfo> {
        @Override
        public RowGroupInfo visit(final @NotNull RowGroupInfo.SingleRowGroup single) {
            return RowGroupInfo.singleRowGroup();
        }

        @Override
        public RowGroupInfo visit(final @NotNull RowGroupInfo.SplitEvenly splitEvenly) {
            return RowGroupInfo.splitEvenly(splitEvenly.getNumRowGroups());
        }

        @Override
        public RowGroupInfo visit(final @NotNull RowGroupInfo.SplitByMaxRows withMaxRows) {
            return RowGroupInfo.withMaxRows(withMaxRows.getMaxRows());
        }

        @Override
        public RowGroupInfo visit(final @NotNull RowGroupInfo.SplitByGroups byGroups) {
            return RowGroupInfo.byGroup(byGroups.getMaxRows(), byGroups.getGroups());
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

        testVisitor(selfVisitor, RowGroupInfo.singleRowGroup());
        testVisitor(selfVisitor, RowGroupInfo.splitEvenly(24));
        testVisitor(selfVisitor, RowGroupInfo.withMaxRows(1500));
        testVisitor(selfVisitor, RowGroupInfo.byGroup("a", "b"));
        testVisitor(selfVisitor, RowGroupInfo.byGroup(200, "b", "c"));

        final UnsupportedOperationException uoe = assertThrowsExactly(UnsupportedOperationException.class,
                () -> testVisitor(selfVisitor, new UnsupportedImpl()));
        assertEquals(String.format("Unknown %s type", RowGroupInfo.class.getCanonicalName()), uoe.getMessage());
    }

    /**
     * a "custom" {@link RowGroupInfo}, which should cause an {@link UnsupportedOperationException} when we try to copy
     */
    private static class UnsupportedImpl extends RowGroupInfo {
        @Override
        public void applyForRowGroups(final @NotNull Table input,
                final @NotNull ThrowingConsumer<Table, IOException> consumer) throws IOException {

        }

        @Override
        public <T> T walk(final @NotNull Visitor<T> visitor) {
            throw new UnsupportedOperationException(
                    String.format("Unknown %s type", RowGroupInfo.class.getCanonicalName()));
        }
    }
}
