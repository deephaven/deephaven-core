//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select.vectorchunkfilter;

import io.deephaven.api.filter.Filter;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.select.WhereFilterFactory;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.vector.ObjectVector;
import io.deephaven.vector.ObjectVectorDirect;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Set;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static org.junit.Assert.*;

public class TestVectorComponentFilterWrapper {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Test
    public void testFilterToBoolean() {
        final Table toFilter = TableTools
                .newTable(TableTools.stringCol("ToMatch", "Alice", "Bob", "Carol", "David", "Edward", "Frederick"),
                        TableTools.intCol("Group", 0, 0, 1, 1, 2, 2))
                .groupBy("Group")
                .update("Array=ToMatch.toArray()");

        final WhereFilter[] vectorFilters =
                WhereFilterFactory.expandQuickFilter(toFilter.getDefinition(), "e", Set.of("ToMatch"));
        assertEquals(1, vectorFilters.length);
        TestCase.assertTrue(vectorFilters[0] instanceof VectorComponentFilterWrapper);

        final WhereFilter[] arrayFilters =
                WhereFilterFactory.expandQuickFilter(toFilter.getDefinition(), "e", Set.of("Array"));
        TestCase.assertTrue(arrayFilters[0] instanceof VectorComponentFilterWrapper);
        assertEquals(1, arrayFilters.length);

        final Table f1 = toFilter.where(Filter.or(vectorFilters));
        assertTableEquals(toFilter.where("Group in 0, 2"), f1);

        final Table f2 = toFilter.where(Filter.or(arrayFilters));
        assertTableEquals(toFilter.where("Group in 0, 2"), f2);

        try (final VectorChunkFilter chunkFilter = ((VectorComponentFilterWrapper) vectorFilters[0]).chunkFilter();
                final WritableObjectChunk<ObjectVector<String>, Values> values =
                        WritableObjectChunk.makeWritableChunk(32);
                final WritableBooleanChunk<Values> matches = WritableBooleanChunk.makeWritableChunk(32)) {
            values.setSize(0);
            values.add(new ObjectVectorDirect<>("Alice", "Bob"));
            values.add(new ObjectVectorDirect<>("Carol", "David"));
            values.add(new ObjectVectorDirect<>("Edward", "Frederick"));
            values.add(new ObjectVectorDirect<>("Gaston"));
            chunkFilter.filter(values, matches);
            assertEquals(4, matches.size());

            assertTrue(matches.get(0));
            assertFalse(matches.get(1));
            assertTrue(matches.get(2));
            assertFalse(matches.get(3));

            values.setSize(0);
            values.add(new CountingObjectVector<>("Hilda", "Ignacio", "Juliet"));
            values.add(new CountingObjectVector<>("Carol", "David"));
            values.add(new CountingObjectVector<>("Karl"));
            values.add(new CountingObjectVector<>("Keenan"));

            chunkFilter.filterAnd(values, matches);
            assertEquals(4, matches.size());

            assertTrue(matches.get(0));
            assertFalse(matches.get(1));
            assertFalse(matches.get(2));
            assertFalse(matches.get(3));

            assertEquals(3, ((CountingObjectVector<String>) values.get(0)).count);
            assertEquals(0, ((CountingObjectVector<String>) values.get(1)).count);
            assertEquals(1, ((CountingObjectVector<String>) values.get(2)).count);
            assertEquals(0, ((CountingObjectVector<String>) values.get(3)).count);
        }

        try (final VectorChunkFilter chunkFilter = ((VectorComponentFilterWrapper) arrayFilters[0]).chunkFilter();
                final WritableObjectChunk<String[], Values> values =
                        WritableObjectChunk.makeWritableChunk(32);
                final WritableBooleanChunk<Values> matches = WritableBooleanChunk.makeWritableChunk(32)) {
            values.setSize(0);
            values.add(new String[] {"Alice", "Bob"});
            values.add(new String[] {"Carol", "David"});
            values.add(new String[] {"Edward", "Frederick"});
            values.add(new String[] {"Gaston"});
            chunkFilter.filter(values, matches);
            assertEquals(4, matches.size());

            assertTrue(matches.get(0));
            assertFalse(matches.get(1));
            assertTrue(matches.get(2));
            assertFalse(matches.get(3));

            values.setSize(0);
            values.add(new String[] {"Hilda", "Ignacio", "Juliet"});
            values.add(new String[] {"Carol", "David"});
            values.add(new String[] {"Karl"});
            values.add(new String[] {"Keenan"});

            chunkFilter.filterAnd(values, matches);
            assertEquals(4, matches.size());

            assertTrue(matches.get(0));
            assertFalse(matches.get(1));
            assertFalse(matches.get(2));
            assertFalse(matches.get(3));
        }
    }

    /**
     * The instant filter is not an ObjectChunkFilter, therefore this tests the chunked path instead of the
     * {@link io.deephaven.engine.table.impl.chunkfilter.ObjectChunkFilter#matches(Object)} path.
     */
    @Test
    public void testFilterInstants() {
        final Table toFilter = TableTools
                .newTable(TableTools.col("ToMatch", DateTimeUtils.parseInstant("2025-08-19T13:00:00 NY"),
                        DateTimeUtils.parseInstant("2025-08-20T13:01:00 NY"),
                        DateTimeUtils.parseInstant("2025-08-20T13:02:00 NY"),
                        DateTimeUtils.parseInstant("2025-08-20T14:04:00 NY"),
                        DateTimeUtils.parseInstant("2025-08-21T14:04:00 NY"),
                        DateTimeUtils.parseInstant("2025-08-21T13:05:00 NY")),
                        TableTools.intCol("Group", 0, 0, 1, 1, 2, 2))
                .groupBy("Group")
                .update("Array=ToMatch.toArray()");

        final WhereFilter[] vectorFilters =
                WhereFilterFactory.expandQuickFilter(toFilter.getDefinition(), "2025-08-20 NY", Set.of("ToMatch"));
        assertEquals(1, vectorFilters.length);
        TestCase.assertTrue(vectorFilters[0] instanceof VectorComponentFilterWrapper);

        final Table f1 = toFilter.where(Filter.or(vectorFilters));
        assertTableEquals(toFilter.where("Group in 0, 1"), f1);


        final WhereFilter[] arrayFilters =
                WhereFilterFactory.expandQuickFilter(toFilter.getDefinition(), "2025-08-20 NY", Set.of("Array"));
        final Table fa1 = toFilter.where(Filter.or(arrayFilters));
        assertTableEquals(toFilter.where("Group in 0, 1"), fa1);

        try (final VectorChunkFilter chunkFilter = ((VectorComponentFilterWrapper) vectorFilters[0]).chunkFilter();
                final WritableObjectChunk<ObjectVector<Instant>, Values> values =
                        WritableObjectChunk.makeWritableChunk(32);
                final WritableBooleanChunk<Values> matches = WritableBooleanChunk.makeWritableChunk(32)) {
            values.setSize(0);
            values.add(makeInstantVector("2025-08-19T13:00:00 NY", "2025-08-20T01:00:00 NY"));
            values.add(makeInstantVector("2025-08-19T13:00:00 NY", "2025-08-19T1:00:00 NY"));
            values.add(makeInstantVector("2025-08-20T13:00:00 NY", "2025-08-19T1:00:00 NY"));
            values.add(makeInstantVector("2025-08-19T15:00:00 NY"));
            chunkFilter.filter(values, matches);
            assertEquals(4, matches.size());

            assertTrue(matches.get(0));
            assertFalse(matches.get(1));
            assertTrue(matches.get(2));
            assertFalse(matches.get(3));

            values.setSize(0);
            values.add(makeInstantVector("2025-08-19T13:00:00 NY", "2025-08-19T01:00:00 NY", "2025-08-20T01:00:00 NY"));
            values.add(makeInstantVector("2025-08-18T13:00:00 NY", "2025-08-18T01:00:00 NY"));
            values.add(makeInstantVector("2025-08-22T13:00:00 NY"));
            values.add(makeInstantVector("2025-08-20T20:00:00 NY"));

            chunkFilter.filterAnd(values, matches);
            assertEquals(4, matches.size());

            assertTrue(matches.get(0));
            assertFalse(matches.get(1));
            assertFalse(matches.get(2));
            assertFalse(matches.get(3));

            assertEquals(3, ((CountingObjectVector<Instant>) values.get(0)).count);
            assertEquals(0, ((CountingObjectVector<Instant>) values.get(1)).count);
            assertEquals(1, ((CountingObjectVector<Instant>) values.get(2)).count);
            assertEquals(0, ((CountingObjectVector<Instant>) values.get(3)).count);
        }

        try (final VectorChunkFilter chunkFilter = ((VectorComponentFilterWrapper) arrayFilters[0]).chunkFilter();
                final WritableObjectChunk<Instant[], Values> values =
                        WritableObjectChunk.makeWritableChunk(32);
                final WritableBooleanChunk<Values> matches = WritableBooleanChunk.makeWritableChunk(32)) {
            values.setSize(0);
            values.add(makeInstantVector("2025-08-19T13:00:00 NY", "2025-08-20T01:00:00 NY").toArray());
            values.add(makeInstantVector("2025-08-19T13:00:00 NY", "2025-08-19T1:00:00 NY").toArray());
            values.add(makeInstantVector("2025-08-20T13:00:00 NY", "2025-08-19T1:00:00 NY").toArray());
            values.add(makeInstantVector("2025-08-19T15:00:00 NY").toArray());
            chunkFilter.filter(values, matches);
            assertEquals(4, matches.size());

            assertTrue(matches.get(0));
            assertFalse(matches.get(1));
            assertTrue(matches.get(2));
            assertFalse(matches.get(3));

            values.setSize(0);
            values.add(makeInstantVector("2025-08-19T13:00:00 NY", "2025-08-19T01:00:00 NY", "2025-08-20T01:00:00 NY")
                    .toArray());
            values.add(makeInstantVector("2025-08-18T13:00:00 NY", "2025-08-18T01:00:00 NY").toArray());
            values.add(makeInstantVector("2025-08-22T20:00:00 NY").toArray());
            values.add(makeInstantVector("2025-08-20T22:00:00 NY").toArray());

            chunkFilter.filterAnd(values, matches);
            assertEquals(4, matches.size());

            assertTrue(matches.get(0));
            assertFalse(matches.get(1));
            assertFalse(matches.get(2));
            assertFalse(matches.get(3));
        }
    }

    @NotNull
    private static ObjectVector<Instant> makeInstantVector(final String... times) {
        return new CountingObjectVector<>(
                Arrays.stream(times).map(DateTimeUtils::parseInstant).toArray(Instant[]::new));
    }

    private static class CountingObjectVector<T> implements ObjectVector<T> {
        private final ObjectVector<T> wrapped;
        int count = 0;

        private CountingObjectVector(final T... data) {
            this.wrapped = new ObjectVectorDirect<>(data);
        }

        @Override
        public T get(final long index) {
            count++;
            return wrapped.get(index);
        }

        @Override
        public T[] toArray() {
            return wrapped.toArray();
        }

        @Override
        public ObjectVector<T> getDirect() {
            return wrapped.getDirect();
        }

        @Override
        public long size() {
            return wrapped.size();
        }

        @Override
        public ObjectVector<T> subVector(final long fromIndexInclusive, final long toIndexExclusive) {
            return wrapped.subVector(fromIndexInclusive, toIndexExclusive);
        }

        @Override
        public ObjectVector<T> subVectorByPositions(final long[] positions) {
            return wrapped.subVectorByPositions(positions);
        }

        @Override
        public T[] copyToArray() {
            return wrapped.copyToArray();
        }

        @Override
        public Class<T> getComponentType() {
            return wrapped.getComponentType();
        }
    }
}
