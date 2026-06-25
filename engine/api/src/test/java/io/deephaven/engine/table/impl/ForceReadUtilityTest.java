//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.util.TableTools;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jmock.Expectations;
import org.jmock.Mockery;
import org.jmock.Sequence;
import org.jmock.lib.concurrent.Synchroniser;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ForceReadUtilityTest {

    @Test
    public void forceReadUtilityOf() {
        final Mockery mockery = new Mockery();
        {
            mockery.setThreadingPolicy(new Synchroniser());
        }

        final ColumnSource<?> cs1 = mockery.mock(ColumnSource.class, "cs1");
        final ColumnSource<?> cs2 = mockery.mock(ColumnSource.class, "cs2");
        final ColumnSource<?> cs3 = mockery.mock(ColumnSource.class, "cs3");
        final ColumnSource<?> cs4 = mockery.mock(ColumnSource.class, "cs4");

        // 100 row table
        final Table table;
        {
            final LinkedHashMap<String, ColumnSource<?>> sources = new LinkedHashMap<>();
            {
                sources.put("CS1", cs1);
                sources.put("CS2", cs2);
                sources.put("CS3", cs3);
                sources.put("CS4", cs4);
            }
            final QueryTable queryTable = new QueryTable(TableDefinition.of(
                    ColumnDefinition.ofLong("CS1"),
                    ColumnDefinition.ofString("CS2"),
                    ColumnDefinition.ofDouble("CS3"),
                    ColumnDefinition.ofDouble("CS4")),
                    RowSetFactory.flat(100).toTracking(),
                    sources,
                    null,
                    null);
            {
                queryTable.setFlat();
            }
            table = queryTable;
        }

        // Exercise all of the read options
        // subset of table row set: [0, 99] - [40, 59]
        // readSize 33:
        // [0, 32]
        // [33, 39] v [60, 85]
        // [86, 99]
        // exclude CS3, read out-of-order: CS4, CS1, CS2
        // column consideration 2, we expect CS4 & CS1 to be fully read before CS2 is read
        final RowSet rowSet = table.getRowSet().minus(RowSetFactory.fromRange(40, 59));
        final ForceReadUtility options = ForceReadUtility.builder()
                .table(table)
                .readSize(33)
                .maxColumns(2)
                .addColumnNames("CS4", "CS1", "CS2")
                .build();

        mockery.checking(new Expectations() {
            {
                WritableRowSet r1 = RowSetFactory.fromRange(0, 32);
                WritableRowSet r2 = RowSetFactory.unionInsert(List.of(
                        RowSetFactory.fromRange(33, 39),
                        RowSetFactory.fromRange(60, 85)));
                WritableRowSet r3 = RowSetFactory.fromRange(86, 99);

                ChunkSource.GetContext c1 = mockery.mock(ChunkSource.GetContext.class, "c1");
                ChunkSource.GetContext c2 = mockery.mock(ChunkSource.GetContext.class, "c2");
                ChunkSource.GetContext c4 = mockery.mock(ChunkSource.GetContext.class, "c4");

                Sequence sequence = mockery.sequence("sequence");

                // CS4, CS1 read; shared context

                oneOf(cs4).makeGetContext(with(options.readSize()), with(any(SharedContext.class)));
                will(returnValue(c4));
                inSequence(sequence);

                oneOf(cs1).makeGetContext(with(options.readSize()), with(any(SharedContext.class)));
                will(returnValue(c1));
                inSequence(sequence);

                expectGetChunk(cs4, r1);
                inSequence(sequence);

                expectGetChunk(cs1, r1);
                inSequence(sequence);

                expectGetChunk(cs4, r2);
                inSequence(sequence);

                expectGetChunk(cs1, r2);
                inSequence(sequence);

                expectGetChunk(cs4, r3);
                inSequence(sequence);

                expectGetChunk(cs1, r3);
                inSequence(sequence);

                oneOf(c4).close();
                inSequence(sequence);

                oneOf(c1).close();
                inSequence(sequence);

                // CS2 read; no shared context

                oneOf(cs2).makeGetContext(with(options.readSize()));
                will(returnValue(c2));
                inSequence(sequence);

                expectGetChunk(cs2, r1);
                inSequence(sequence);

                expectGetChunk(cs2, r2);
                inSequence(sequence);

                expectGetChunk(cs2, r3);
                inSequence(sequence);

                oneOf(c2).close();
                inSequence(sequence);
            }

            private void expectGetChunk(ColumnSource<?> cs, WritableRowSet rs) {
                oneOf(cs).getChunk(with(any(ChunkSource.GetContext.class)), withRS(rs));
            }

            private RowSequence withRS(final RowSet expected) {
                return this.with(new BaseMatcher<RowSequence>() {
                    @Override
                    public boolean matches(Object actual) {
                        try (RowSet rowSet = ((RowSequence) actual).asRowSet()) {
                            return rowSet.equals(expected);
                        }
                    }

                    @Override
                    public void describeTo(Description description) {
                        description.appendText("row set " + expected);
                    }
                });
            }
        });

        ForceReadUtility.of(options, rowSet);
    }

    @Test
    public void missingTable() {
        try {
            ForceReadUtility.builder().build();
            fail("Expected exception");
        } catch (IllegalStateException e) {
            assertEquals("Cannot build ForceReadUtility, some of required attributes are not set [table]",
                    e.getMessage());
        }
    }

    @Test
    public void badColumnName() {
        try {
            ForceReadUtility.builder()
                    .table(TableTools.emptyTable(42))
                    .addColumnNames("DoesNotExist")
                    .build();
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            assertEquals("Unknown column names [DoesNotExist], available column names are []", e.getMessage());
        }
    }

    @Test
    public void badReadSize() {
        try {
            ForceReadUtility.builder()
                    .table(TableTools.emptyTable(42))
                    .readSize(0)
                    .build();
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            assertEquals("readSize must be positive", e.getMessage());
        }
    }

    @Test
    public void badMaxColumns() {
        try {
            ForceReadUtility.builder()
                    .table(TableTools.emptyTable(42))
                    .maxColumns(0)
                    .build();
            fail("Expected exception");
        } catch (IllegalArgumentException e) {
            assertEquals("maxColumns must be positive", e.getMessage());
        }
    }
}
