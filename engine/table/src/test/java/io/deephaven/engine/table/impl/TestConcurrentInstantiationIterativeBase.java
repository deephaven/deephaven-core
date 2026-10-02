//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.BooleanGenerator;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.util.mutable.MutableInt;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.*;

public abstract class TestConcurrentInstantiationIterativeBase extends TestConcurrentInstantiationBase {
    void testIterative(List<Function<Table, Table>> transformations) {
        testIterative(transformations, 0, new MutableInt(50));
    }

    @SuppressWarnings("ConstantConditions")
    void testIterative(List<Function<Table, Table>> transformations, int seed, MutableInt numSteps) {
        final ColumnInfo<?, ?>[] columnInfos;

        final int size = 100;
        final Random random = new Random(seed);
        final int maxSteps = numSteps.get();

        final QueryTable table = getTable(size, random,
                columnInfos = initColumnInfos(new String[] {"Sym", "intCol", "boolCol", "boolCol2", "doubleCol"},
                        new SetGenerator<>("aa", "bb", "bc", "cc", "dd", "ee", "ff", "gg", "hh", "ii"),
                        new IntGenerator(0, 100),
                        new BooleanGenerator(),
                        new BooleanGenerator(),
                        new DoubleGenerator(0, 100)));

        final Callable<Table> complete = () -> {
            Table t = table;
            for (Function<Table, Table> transformation : transformations) {
                t = transformation.apply(t);
            }
            return t;
        };

        final List<Pair<Callable<Table>, Function<Table, Table>>> splitCallables = new ArrayList<>();
        for (int ii = 1; ii <= transformations.size() - 1; ++ii) {
            final int fii = ii;
            final Callable<Table> firstHalf = () -> {
                Table t = table;
                for (int jj = 0; jj < fii; ++jj) {
                    t = transformations.get(jj).apply(t);
                }
                return t;
            };
            final Function<Table, Table> secondHalf = (firstResult) -> {
                Table t = firstResult;
                for (int jj = fii; jj < transformations.size(); ++jj) {
                    t = transformations.get(jj).apply(t);
                }
                return t;
            };
            splitCallables.add(new Pair<>(firstHalf, secondHalf));
        }

        final Table standard = updateGraph.exclusiveLock().computeLocked(() -> {
            try {
                return complete.call();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
                throw new RuntimeException(e);
            }
        });

        final boolean beforeUpdate = true;
        final boolean beforeNotify = true;

        final boolean beforeAndAfterUpdate = true;
        final boolean beforeStartAndBeforeUpdate = true;
        final boolean beforeStartAndAfterUpdate = true;

        final boolean beforeAndAfterNotify = true;
        final boolean beforeStartAndAfterNotify = true;
        final boolean beforeUpdateAndAfterNotify = true;

        final boolean beforeAndAfterCycle = true;
        final boolean beforeUpdateAndAfterCycle = true;
        final boolean beforeNotifyAndAfterCycle = true;
        final boolean beforeStartAndAfterCycle = true;

        final List<Table> results = new ArrayList<>();
        // noinspection MismatchedQueryAndUpdateOfCollection
        final List<TableUpdateListener> listeners = new ArrayList<>();
        int lastResultSize = 0;

        try {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Input Table:\n");
                showWithRowSet(table);
            }

            for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
                final int i = numSteps.get();
                if (RefreshingTableTestCase.printTableUpdates) {
                    System.out.println("Step = " + i);
                }

                final List<Table> beforeStartFirstHalf = new ArrayList<>(splitCallables.size());
                for (Pair<Callable<Table>, Function<Table, Table>> splitCallable : splitCallables) {
                    beforeStartFirstHalf.add(pool.submit(splitCallable.first).get(TIMEOUT_LENGTH, TIMEOUT_UNIT));
                }

                updateGraph.startCycleForUnitTests(false);

                if (beforeUpdate) {
                    // before we update the underlying data
                    final Table chain1 = pool.submit(complete).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                            .withAttributes(Map.of(
                                    "Step", i,
                                    "Type", "beforeUpdate"));
                    results.add(chain1);
                }

                if (beforeStartAndBeforeUpdate) {
                    final List<Table> beforeStartAndBeforeUpdateSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeStartFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeUpdateSplit"));
                        beforeStartAndBeforeUpdateSplitResults.add(splitResult);
                    }
                    results.addAll(beforeStartAndBeforeUpdateSplitResults);
                }

                final List<Table> beforeUpdateFirstHalf = new ArrayList<>(splitCallables.size());
                for (Pair<Callable<Table>, Function<Table, Table>> splitCallable : splitCallables) {
                    beforeUpdateFirstHalf.add(pool.submit(splitCallable.first).get(TIMEOUT_LENGTH, TIMEOUT_UNIT));
                }

                final RowSet[] updates = GenerateTableUpdates.computeTableUpdates(size, random, table, columnInfos);

                if (beforeNotify) {
                    // after we update the underlying data, but before we notify
                    final Table chain2 = pool.submit(complete).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                            .withAttributes(Map.of(
                                    "Step", i,
                                    "Type", "beforeNotify"));
                    results.add(chain2);
                }

                if (beforeAndAfterUpdate) {
                    final List<Table> beforeAndAfterUpdateSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeUpdateFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeAndAfterUpdateSplit",
                                        "SplitIndex", splitIndex));
                        beforeAndAfterUpdateSplitResults.add(splitResult);
                    }
                    results.addAll(beforeAndAfterUpdateSplitResults);
                }

                if (beforeStartAndAfterUpdate) {
                    final List<Table> beforeStartAndAfterUpdateSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeStartFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeStartAndAfterUpdate",
                                        "SplitIndex", splitIndex));
                        beforeStartAndAfterUpdateSplitResults.add(splitResult);
                    }
                    results.addAll(beforeStartAndAfterUpdateSplitResults);
                }

                final List<Table> beforeNotifyFirstHalf = new ArrayList<>(splitCallables.size());
                for (Pair<Callable<Table>, Function<Table, Table>> splitCallable : splitCallables) {
                    beforeNotifyFirstHalf.add(pool.submit(splitCallable.first).get(TIMEOUT_LENGTH, TIMEOUT_UNIT));
                }

                table.notifyListeners(updates[0], updates[1], updates[2]);
                updateGraph.markSourcesRefreshedForUnitTests();

                if (beforeAndAfterNotify) {
                    final List<Table> beforeAndAfterNotifySplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeNotifyFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeAndAfterNotify"));
                        beforeAndAfterNotifySplitResults.add(splitResult);
                    }
                    results.addAll(beforeAndAfterNotifySplitResults);
                }

                if (beforeStartAndAfterNotify) {
                    final List<Table> beforeStartAndAfterNotifySplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeStartFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeStartAndAfterNotify",
                                        "SplitIndex", splitIndex));
                        beforeStartAndAfterNotifySplitResults.add(splitResult);
                    }
                    results.addAll(beforeStartAndAfterNotifySplitResults);
                }

                if (beforeUpdateAndAfterNotify) {
                    final List<Table> beforeUpdateAndAfterNotifySplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeUpdateFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeUpdateAndAfterNotify",
                                        "SplitIndex", splitIndex));
                        beforeUpdateAndAfterNotifySplitResults.add(splitResult);
                    }
                    results.addAll(beforeUpdateAndAfterNotifySplitResults);
                }

                final List<Table> beforeCycleFirstHalf = new ArrayList<>(splitCallables.size());
                if (beforeAndAfterCycle) {
                    for (Pair<Callable<Table>, Function<Table, Table>> splitCallable : splitCallables) {
                        beforeCycleFirstHalf.add(pool.submit(splitCallable.first).get(TIMEOUT_LENGTH, TIMEOUT_UNIT));
                    }
                }

                if (beforeNotify) {
                    // after notification, on the same cycle
                    final Table chain3 = pool.submit(complete).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                            .withAttributes(Map.of(
                                    "Step", i,
                                    "Type", "beforeNotify"));
                    results.add(chain3);
                }

                for (int newResult = lastResultSize; newResult < results.size(); ++newResult) {
                    final Table dynamicTable = results.get(newResult);
                    final InstrumentedTableUpdateListenerAdapter listener =
                            new InstrumentedTableUpdateListenerAdapter("errorListener", dynamicTable, false) {
                                @Override
                                public void onUpdate(final TableUpdate upstream) {}

                                @Override
                                public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
                                    originalException.printStackTrace(System.err);
                                    fail(originalException.getMessage());
                                }
                            };
                    listeners.add(listener);
                    dynamicTable.addUpdateListener(listener);
                }
                lastResultSize = results.size();
                updateGraph
                        .completeCycleForUnitTests();

                if (beforeStartAndAfterCycle) {
                    final List<Table> beforeStartAndAfterCycleSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeStartFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeStartAndAfterCycle",
                                        "SplitIndex", splitIndex));
                        beforeStartAndAfterCycleSplitResults.add(splitResult);
                    }

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = updateGraph.exclusiveLock()
                                .computeLocked(() -> splitCallables.get(fSplitIndex).second
                                        .apply(beforeStartFirstHalf.get(fSplitIndex)))
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeStartAndAfterCycleLocked",
                                        "SplitIndex", splitIndex));
                        beforeStartAndAfterCycleSplitResults.add(splitResult);
                    }
                    results.addAll(beforeStartAndAfterCycleSplitResults);
                }

                if (beforeUpdateAndAfterCycle) {
                    final List<Table> beforeUpdateAndAfterCycleSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeUpdateFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeUpdateAndAfterCycle",
                                        "SplitIndex", splitIndex));
                        beforeUpdateAndAfterCycleSplitResults.add(splitResult);
                    }

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = updateGraph.exclusiveLock()
                                .computeLocked(() -> splitCallables.get(fSplitIndex).second
                                        .apply(beforeUpdateFirstHalf.get(fSplitIndex)))
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeUpdateAndAfterCycleLocked",
                                        "SplitIndex", splitIndex));
                        beforeUpdateAndAfterCycleSplitResults.add(splitResult);
                    }
                    results.addAll(beforeUpdateAndAfterCycleSplitResults);
                }

                if (beforeNotifyAndAfterCycle) {
                    final List<Table> beforeNotifyAndAfterCycleSplitResults = new ArrayList<>(splitCallables.size());
                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeNotifyFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeNotifyAndAfterCycle",
                                        "SplitIndex", splitIndex));
                        beforeNotifyAndAfterCycleSplitResults.add(splitResult);
                    }

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final Table splitResult =
                                splitCallables.get(splitIndex).second.apply(beforeNotifyFirstHalf.get(splitIndex))
                                        .withAttributes(Map.of(
                                                "Step", i,
                                                "Type", "beforeNotifyAndAfterCycleLocked",
                                                "SplitIndex", splitIndex));
                        beforeNotifyAndAfterCycleSplitResults.add(splitResult);
                    }

                    results.addAll(beforeNotifyAndAfterCycleSplitResults);
                }

                Assert.eqTrue(beforeAndAfterCycle, "beforeAndAfterCycle");
                if (transformations.size() > 1) {
                    Assert.eqFalse(beforeCycleFirstHalf.isEmpty(), "beforeCycleFirstHalf.isEmpty()");
                    final List<Table> beforeAndAfterCycleSplitResults = new ArrayList<>(splitCallables.size());

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = pool.submit(() -> splitCallables.get(fSplitIndex).second
                                .apply(beforeCycleFirstHalf.get(fSplitIndex))).get(TIMEOUT_LENGTH, TIMEOUT_UNIT)
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeAndAfterCycle",
                                        "SplitIndex", splitIndex));
                        beforeAndAfterCycleSplitResults.add(splitResult);
                    }

                    for (int splitIndex = 0; splitIndex < splitCallables.size(); ++splitIndex) {
                        final int fSplitIndex = splitIndex;
                        final Table splitResult = updateGraph.exclusiveLock()
                                .computeLocked(() -> splitCallables.get(fSplitIndex).second
                                        .apply(beforeCycleFirstHalf.get(fSplitIndex)))
                                .withAttributes(Map.of(
                                        "Step", i,
                                        "Type", "beforeAndAfterCycle",
                                        "SplitIndex", splitIndex));
                        beforeAndAfterCycleSplitResults.add(splitResult);
                    }

                    results.addAll(beforeAndAfterCycleSplitResults);
                }

                if (RefreshingTableTestCase.printTableUpdates) {
                    System.out.println("Input Table: (" + Objects.hashCode(table) + ")");
                    showWithRowSet(table);
                    System.out.println("Standard Table: (" + Objects.hashCode(standard) + ")");
                    showWithRowSet(standard);
                    System.out.println("Verifying " + results.size() + " tables (size = " + standard.size() + ")");
                }

                // now verify all the outstanding results
                for (Table checkTable : results) {
                    String diff = diff(checkTable, standard, 10);
                    if (!diff.isEmpty() && RefreshingTableTestCase.printTableUpdates) {
                        System.out.println("Check Table: " + checkTable.getAttribute("Step") + ", " +
                                checkTable.getAttribute("Type") +
                                ", splitIndex=" + checkTable.getAttribute("SplitIndex") +
                                ", hash=" + Objects.hashCode(checkTable));
                        showWithRowSet(checkTable);
                    }
                    assertEquals("", diff);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
