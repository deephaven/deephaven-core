/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl;

import io.deephaven.base.Pair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.util.TableTools;
import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.EnumSet;
import java.util.function.Supplier;

import static io.deephaven.engine.util.TableTools.showWithRowSet;

public abstract class EvalNugget implements EvalNuggetInterface {
    public static EvalNugget from(Supplier<Table> makeTable) {
        return new EvalNugget() {
            @Override
            protected Table e() {
                return makeTable.get();
            }
        };
    }

    public EvalNugget() {
        this(null);
    }

    public EvalNugget(String description) {
        this.description = description;
        if (RefreshingTableTestCase.printTableUpdates) {
            showResult("Original Table:", originalValue);
            System.out.println();
        }
    }

    private final String description;
    public final Table originalValue = e();
    private Table recomputedTable = null;
    private Throwable exception = null;

    // We should listen for failures on the table, and if we get any, the test case is no good.
    class FailureListener extends InstrumentedTableUpdateListener {
        FailureListener() {
            super("Failure ShiftObliviousListener");
        }

        @Override
        public void onUpdate(final TableUpdate upstream) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Incremental Table Update:");
                System.out.println(upstream);
            }
        }

        @Override
        public void onFailureInternal(Throwable originalException, Entry sourceEntry) {
            exception = originalException;
            final StringWriter errors = new StringWriter();
            if (description != null) {
                errors.write("Failure updating " + description + "\n");
            }
            originalException.printStackTrace(new PrintWriter(errors));

            showResult("Incremental Table at Failure State:", originalValue);
            TestCase.fail(errors.toString());
        }
    }

    private final TableUpdateListener failureListener = new FailureListener();
    {
        // subscribe before the validator in case we are printing table updates
        if (originalValue instanceof QueryTable) {
            ((QueryTable) originalValue).listenForUpdates(failureListener);
        }
    }

    private final TableUpdateValidator validator;
    {
        if (originalValue instanceof QueryTable && ((QueryTable) originalValue).isRefreshing()) {
            validator = TableUpdateValidator.make((QueryTable) originalValue);
            validator.getResultTable().listenForUpdates(failureListener);
        } else {
            validator = null;
        }
    }

    protected abstract Table e();

    public EvalNugget hasUnstableColumns(final String... columnNames) {
        if (validator != null) {
            validator.dontValidateColumns(columnNames);
        }
        return this;
    }

    public void validate(final String msg) {
        if (validator != null) {
            validator.validate();
        }

        Assert.assertNull(exception);
        if (recomputedTable == null) {
            recomputedTable = e();
        }
        checkDifferences(msg, recomputedTable);
        recomputedTable = null;
    }

    void showResult(String label, Table e) {
        System.out.println(label);
        showWithRowSet(e, 100);
    }

    void checkDifferences(String msg, Table recomputed) {
        TstUtils.assertTableEquals(msg, forComparison(recomputed), forComparison(originalValue), diffItems());
    }

    @NotNull
    EnumSet<TableDiff.DiffItems> diffItems() {
        return EnumSet.of(TableDiff.DiffItems.DoublesExact);
    }

    Table forComparison(Table t) {
        return t;
    }

    public void show() {
        recomputedTable = e();
        final Table recomputedForComparison = forComparison(recomputedTable);
        final Table originalForComparison = forComparison(originalValue);

        final int maxLines = 100;
        final Pair<String, Long> diffPair =
                TableTools.diffPair(originalForComparison, recomputedForComparison, maxLines, diffItems());

        if (diffPair.getFirst().equals("")) {
            showResult("Recomputed Table:", recomputedTable);
        } else if (!diffPair.getFirst().equals("")) {
            final long numTableRows =
                    Math.min(maxLines, Math.max(originalForComparison.size(), recomputedForComparison.size()));
            final long firstRow = Math.max(0, diffPair.getSecond() - 5);
            final long lastRow =
                    Math.min(firstRow + numTableRows, Math.min(firstRow + maxLines, diffPair.getSecond() + 5));

            System.out.println("Recomputed Table Differs:\n" + diffPair.getFirst() + "\nRecomputed Table Rows ["
                    + firstRow + ", " + lastRow + "]:");
            showWithRowSet(recomputedForComparison, firstRow, lastRow + 1);
            System.out.println("Incremental Table Rows [" + firstRow + ", " + lastRow + "]:");
            showWithRowSet(originalForComparison, firstRow, lastRow + 1);

            if (recomputedForComparison != recomputedTable) {
                showResult("Recomputed Table (unmodified):", recomputedTable);
                e();
            }
            if (originalForComparison != originalValue) {
                showResult("Incremental Table (unmodified):", originalValue);
            }
        }
    }

    public abstract static class Sorted extends EvalNugget {
        private final String[] sortColumns;

        public Sorted(String... sortColumns) {
            this.sortColumns = sortColumns;
        }

        public Sorted(String description, String... sortColumns) {
            super(description);
            this.sortColumns = sortColumns;
        }

        @Override
        Table forComparison(Table t) {
            return t.sort(sortColumns);
        }

        public static EvalNugget from(Supplier<Table> makeTable, String... sortColumns) {
            return new EvalNugget.Sorted(sortColumns) {
                @Override
                protected Table e() {
                    return makeTable.get();
                }
            };
        }
    }
}
