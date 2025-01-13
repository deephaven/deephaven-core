//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.locations.TableBackedTableLocationProvider;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.experimental.categories.Category;

import java.util.Map;

@Category(OutOfBandTest.class)
public class ManyLocationsTest extends RefreshingTableTestCase {

    private static final boolean DISABLE_PERFORMANCE_TEST = true;

    public void testManyLocationsCoalesce() {
        if (DISABLE_PERFORMANCE_TEST) {
            return;
        }

        final long startConstructionNanos = System.nanoTime();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final TableBackedTableLocationProvider tlp = new TableBackedTableLocationProvider(
                updateGraph,
                true,
                TableUpdateMode.APPEND_ONLY,
                TableUpdateMode.APPEND_ONLY);
        final Table singleLocationTable = TableTools.emptyTable(1000).update("AA=ii");
        final Table past = new PartitionAwareSourceTable(
                TableDefinition.of(
                        ColumnDefinition.ofInt("PI").withPartitioning(),
                        ColumnDefinition.ofLong("AA")),
                "TestTable",
                RegionedTableComponentFactoryImpl.INSTANCE,
                tlp,
                updateGraph);
        final long endConstructionNanos = System.nanoTime();
        System.out.printf("Construction time: %.2fs%n",
                (endConstructionNanos - startConstructionNanos) / 1_000_000_000D);

        final long startCreationNanos = System.nanoTime();
        for (int pi = 0; pi < 100_000; ++pi) {
            tlp.add(singleLocationTable, Map.of("PI", pi));
        }
        final long endCreationNanos = System.nanoTime();
        System.out.printf("Creation time: %.2fs%n", (endCreationNanos - startCreationNanos) / 1_000_000_000D);

        final long startCoalesceNanos = System.nanoTime();
        past.coalesce();
        final long endCoalesceNanos = System.nanoTime();
        System.out.printf("Coalesce time: %.2fs%n", (endCoalesceNanos - startCoalesceNanos) / 1_000_000_000D);

        System.out.printf("Total time: %.2fs%n", (endCoalesceNanos - startConstructionNanos) / 1_000_000_000D);
    }
}
