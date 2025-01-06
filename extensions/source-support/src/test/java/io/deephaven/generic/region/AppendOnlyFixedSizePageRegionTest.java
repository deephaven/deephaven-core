//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.generic.region;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.select.SimulationClock;
import io.deephaven.engine.table.impl.sources.regioned.*;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.locations.DependentRegistrar;
import io.deephaven.engine.testutil.locations.TableBackedTableLocationProvider;
import io.deephaven.engine.updategraph.*;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.*;

/**
 * Unit tests for {@link AppendOnlyFixedSizePageRegionChar} and its replicas.
 */
@Category(OutOfBandTest.class)
public class AppendOnlyFixedSizePageRegionTest {

    @Rule
    public final EngineCleanup cleanup = new EngineCleanup();

    @Test
    public void testCorrectness() {
        final Instant startTime = Instant.now();
        final Instant endTime = DateTimeUtils.plus(startTime, 1_000_000_000L);
        final SimulationClock clock = new SimulationClock(startTime, endTime, 100_000_000L);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final UpdateSourceCombiner updateSources = new UpdateSourceCombiner(updateGraph);
        final TimeTable[] timeTables = new TimeTable[] {
                new TimeTable(updateSources, clock, startTime, 1000, false),
                new TimeTable(updateSources, clock, startTime, 10000, false),
                new TimeTable(updateSources, clock, startTime, 100000, false)
        };
        final Table[] withTypes = addTypes(timeTables);
        final DependentRegistrar dependentRegistrar = new DependentRegistrar(withTypes);
        final Table expected = makeMerged(withTypes);
        final Table actual = makeRegioned(dependentRegistrar, withTypes);
        System.out.println("Initial start time: " + clock.instantNanos());
        TstUtils.assertTableEquals(expected, actual);
        clock.start();
        while (!clock.done()) {
            updateGraph.runWithinUnitTestCycle(() -> {
                clock.advance();
                updateSources.run();
                dependentRegistrar.run();
            });
            System.out.println("Cycle start time: " + clock.instantNanos());
            TstUtils.assertTableEquals(expected, actual);
        }
    }

    private static Table[] addTypes(@NotNull final Table... tables) {
        return Arrays.stream(tables).map(AppendOnlyFixedSizePageRegionTest::addTypes).toArray(Table[]::new);
    }

    private static Table addTypes(@NotNull final Table table) {
        return table.updateView(
                "B    = ii % 1000  == 0  ? NULL_BYTE   : (byte)  ii",
                "C    = ii % 27    == 26 ? NULL_CHAR   : (char)  ('A' + ii % 27)",
                "S    = ii % 30000 == 0  ? NULL_SHORT  : (short) ii",
                "I    = ii % 512   == 0  ? NULL_INT    : (int)   ii",
                "L    = ii % 1024  == 0  ? NULL_LONG   :         ii",
                "F    = ii % 2048  == 0  ? NULL_FLOAT  : (float) (ii * 0.25)",
                "D    = ii % 4096  == 0  ? NULL_DOUBLE :         ii * 1.25",
                "Bl   = ii % 8192  == 0  ? null        :         ii % 2 == 0",
                "Str  = ii % 128   == 0  ? null        :         Long.toString(ii)");
    }

    private static Table makeMerged(@NotNull final Table... constituents) {
        return TableTools.merge(constituents);
    }

    private static Table makeRegioned(
            @NotNull final UpdateSourceRegistrar registrar,
            @NotNull final Table... constituents) {
        assertThat(constituents).isNotNull();
        assertThat(constituents).isNotEmpty();

        return new SimpleSourceTable(
                constituents[0].getDefinition(),
                "Test SimpleSourceTable",
                RegionedTableComponentFactoryImpl.INSTANCE,
                new TableBackedTableLocationProvider(registrar, false, TableUpdateMode.STATIC, TableUpdateMode.STATIC,
                        constituents),
                registrar).coalesce();
    }

}
