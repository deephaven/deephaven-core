package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.vector.*;
import io.deephaven.chunk.util.hashing.ChunkEquals;
import io.deephaven.engine.table.impl.sources.SparseArrayColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableList;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;

public class TableUpdateValidator implements QueryTable.Operation {
    private static final boolean useSharedContext = Configuration.getInstance()
            .getBooleanForClassWithDefault(TableUpdateValidator.class, "useSharedContext", true);
    private static final boolean aggressiveUpdateValidation = Configuration.getInstance()
            .getBooleanForClassWithDefault(TableUpdateValidator.class, "aggressiveUpdateValidation", false);
    private static final int CHUNK_SIZE = 4096;

    public static TableUpdateValidator make(final QueryTable tableToValidate) {
        return make(null, tableToValidate);
    }

    public static TableUpdateValidator make(final String description, final QueryTable tableToValidate) {
        if (!tableToValidate.isRefreshing()) {
            throw new IllegalArgumentException("Validator has nothing to validate if input table is not refreshing.");
        }
        final TableUpdateValidator validator = new TableUpdateValidator(description, tableToValidate);
        tableToValidate.getResult(validator);
        return validator;
    }

    private final QueryTable tableToValidate;
    private final ModifiedColumnSet validationMCS;
    private ColumnInfo[] columnInfos;

    private WritableRowSet rowSet;
    private QueryTable resultTable;
    private SharedContext sharedContext;
    private final String description;

    private TableUpdateValidator(final String description, final QueryTable tableToValidate) {
        this.description = description == null ? tableToValidate.getDescription() : description;
        this.tableToValidate = tableToValidate;
        this.validationMCS = tableToValidate.newModifiedColumnSet(
                tableToValidate.getColumnSourceMap().keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
        Assert.neq(validationMCS, "validationMCS", ModifiedColumnSet.ALL, "ModifiedColumnSet.ALL");
        Assert.neq(validationMCS, "validationMCS", ModifiedColumnSet.EMPTY, "ModifiedColumnSet.EMPTY");

        columnInfos = tableToValidate.getColumnSourceMap().keySet().stream()
                .map((name) -> new ColumnInfo(tableToValidate, name))
                .toArray(ColumnInfo[]::new);
    }

    private SafeCloseable maybeOpenSharedContext() {
        return sharedContext = useSharedContext ? new NullOnCloseSharedContext() : null;
    }

    private class NullOnCloseSharedContext extends SharedContext {

        @Override
        public void close() {
            sharedContext = null;
            super.close();
        }
    }

    private void maybeResetSharedContext() {
        if (sharedContext != null) {
            sharedContext.reset();
        }
    }

    @Override
    public String getDescription() {
        return "UpdateValidator(" + description + ")";
    }

    @Override
    public String getLogPrefix() {
        return "UpdateValidator";
    }

    @Override
    public Result initialize(boolean usePrev, long beforeClock) {
        rowSet = usePrev ? tableToValidate.getRowSet().copyPrev() : tableToValidate.getRowSet().copy();

        resultTable = new QueryTable(RowSetFactory.empty().toTracking(),
                Collections.emptyMap());
        resultTable.setFlat();

        final TableUpdateListener listener;
        try (final SafeCloseable ignored1 = maybeOpenSharedContext();
                final SafeCloseable ignored2 = new SafeCloseableList(columnInfos)) {
            updateValues(ModifiedColumnSet.ALL, rowSet, usePrev);

            listener = new BaseTable.ListenerImpl(getDescription(), tableToValidate, resultTable) {
                @Override
                public void onUpdate(final TableUpdate upstream) {
                    TableUpdateValidator.this.onUpdate(upstream);
                }
            };
        }

        return new Result(resultTable, listener);
    }

    public QueryTable getResultTable() {
        return resultTable;
    }

    public void validate() {
        Assert.equals(rowSet, "rowSet", tableToValidate.getRowSet(), "tableToValidate.build()");
    }

    public void deepValidation() {
        try (final SafeCloseable ignored1 = maybeOpenSharedContext();
                final SafeCloseable ignored2 = new SafeCloseableList(columnInfos)) {

            validate();
            validateValues("EndOfTickValidation", ModifiedColumnSet.ALL, rowSet, false, false);
            if (!issues.isEmpty()) {
                final StringBuilder result =
                        new StringBuilder("Table to validate " + getDescription() + " has inconsistent state:");
                for (final String issue : issues) {
                    result.append("\n - ").append(issue);
                }
                result.append("\n");
                throw new RuntimeException(result.toString());
            }
        }
    }

    private void onUpdate(final TableUpdate upstream) {
        if (resultTable.size() >= MAX_ISSUES) {
            return;
        }

        try (final SafeCloseable ignored1 = maybeOpenSharedContext();
                final SafeCloseable ignored2 = new SafeCloseableList(columnInfos)) {
            if (!upstream.modifiedColumnSet().isCompatibleWith(validationMCS)) {
                noteIssue(
                        () -> "upstream.modifiedColumnSet is not compatible with table.newModifiedColumnSet(...): upstream="
                                + upstream.modifiedColumnSet() + " initialized=" + validationMCS);
            }

            // remove
            if (aggressiveUpdateValidation) {
                validateValues("pre-update", ModifiedColumnSet.ALL, rowSet, true, false);
            } else {
                validateValues("pre-update removed", ModifiedColumnSet.ALL, upstream.removed(), true, false);
                validateValues("pre-update modified", upstream.modifiedColumnSet(), upstream.getModifiedPreShift(),
                        true,
                        false);
            }

            validateIndexesEqual("pre-update rowSet", rowSet, tableToValidate.getRowSet().copyPrev());
            rowSet.remove(upstream.removed());
            Arrays.stream(columnInfos).forEach((ci) -> ci.remove(upstream.removed()));

            // shift columns first because they use rowSet
            Arrays.stream(columnInfos).forEach((ci) -> upstream.shifted().apply(ci));
            upstream.shifted().apply(rowSet);

            if (aggressiveUpdateValidation) {
                final RowSet unmodified = rowSet.minus(upstream.modified());
                validateValues("post-shift unmodified", ModifiedColumnSet.ALL, unmodified, false, false);
                validateValues("post-shift unmodified columns", upstream.modifiedColumnSet(), upstream.modified(),
                        false,
                        true);
            }

            // added
            if (rowSet.overlaps(upstream.added())) {
                noteIssue(() -> "post-shift rowSet contains rows that are added: "
                        + rowSet.intersect(upstream.added()));
            }
            rowSet.insert(upstream.added());
            validateIndexesEqual("post-update rowSet", rowSet, tableToValidate.getRowSet());
            updateValues(ModifiedColumnSet.ALL, upstream.added(), false);

            // modified
            updateValues(upstream.modifiedColumnSet(), upstream.modified(), false);

            if (!issues.isEmpty()) {
                StringBuilder result =
                        new StringBuilder("Table to validate " + getDescription() + " generated an erroneous update:");
                for (String issue : issues) {
                    result.append("\n - ").append(issue);
                }
                result.append("\n");
                resultTable.notifyListenersOnError(new RuntimeException(result.toString()), null);
            }
        }
    }

    private void validateIndexesEqual(final String what, final RowSet expected, final RowSet actual) {
        if (expected.equals(actual)) {
            return;
        }

        final RowSet missing = expected.minus(actual);
        final RowSet excess = actual.minus(expected);
        if (missing.isNonempty()) {
            noteIssue(() -> what + " expected.minus(actual)=" + missing);
        }
        if (excess.isNonempty()) {
            noteIssue(() -> what + " actual.minus(expected)=" + excess);
        }
    }

    // TODO: Should this string array actually just be the table output? with columns like 'expected', 'actual', 'row',
    // 'cycle', etc?
    private final int MAX_ISSUES = 10;
    private final ArrayList<String> issues = new ArrayList<>();

    private void noteIssue(Supplier<String> issue) {
        if (issues.size() < MAX_ISSUES) {
            issues.add(issue.get());
        }
    }

    private void validateValues(final String what, final ModifiedColumnSet columnsToCheck, final RowSet toValidate,
            final boolean usePrev, final boolean invertMCS) {
        try (final RowSequence.Iterator it = toValidate.getRowSequenceIterator()) {
            while (it.hasMore()) {
                final RowSequence subKeys = it.getNextRowSequenceWithLength(CHUNK_SIZE);
                for (final ColumnInfo ci : columnInfos) {
                    if (columnsToCheck.containsAny(ci.modifiedColumnSet) == !invertMCS) {
                        ci.validateValues(what, subKeys, usePrev);
                    }
                }
                maybeResetSharedContext();
            }
        }
    }

    private void updateValues(final ModifiedColumnSet columnsToUpdate, final RowSet toUpdate, final boolean usePrev) {
        try (final RowSequence.Iterator it = toUpdate.getRowSequenceIterator()) {
            while (it.hasMore()) {
                final RowSequence subKeys = it.getNextRowSequenceWithLength(CHUNK_SIZE);
                for (final ColumnInfo ci : columnInfos) {
                    if (columnsToUpdate.containsAny(ci.modifiedColumnSet)) {
                        ci.updateValues(subKeys, usePrev);
                    }
                }
                maybeResetSharedContext();
            }
        }
    }

    /**
     * Some things won't last forever, like a Vector that is really a column wrapper. We need to turn those into
     * something that will persist properly until the next clock cycle.
     *
     * @param fromSource
     * @return a version of fromSource that does not reference ephemeral stuff.
     */
    private Object maybeWrap(Object fromSource) {
        if (fromSource instanceof ObjectVector) {
            return ((ObjectVector) fromSource).getDirect();
        } else if (fromSource instanceof IntVector) {
            return ((IntVector) fromSource).getDirect();
        } else if (fromSource instanceof LongVector) {
            return ((LongVector) fromSource).getDirect();
        } else if (fromSource instanceof ShortVector) {
            return ((ShortVector) fromSource).getDirect();
        } else if (fromSource instanceof DoubleVector) {
            return ((DoubleVector) fromSource).getDirect();
        } else if (fromSource instanceof FloatVector) {
            return ((FloatVector) fromSource).getDirect();
        } else if (fromSource instanceof CharVector) {
            return ((CharVector) fromSource).getDirect();
        } else if (fromSource instanceof ByteVector) {
            return ((ByteVector) fromSource).getDirect();
        }
        return fromSource;
    }

    public void dontValidateColumns(String[] columnNames) {
        ArrayList<ColumnInfo> ciBuilder = new ArrayList<>();
        for (final ColumnInfo ci : columnInfos) {
            if (Arrays.stream(columnNames).noneMatch((cn) -> cn.equals(ci.name))) {
                ciBuilder.add(ci);
            }
        }

        columnInfos = ciBuilder.toArray(new ColumnInfo[0]);
    }

    private class ColumnInfo implements RowSetShiftData.Callback, SafeCloseable {
        final String name;
        final boolean isPrimitive;
        final ModifiedColumnSet modifiedColumnSet;

        final ColumnSource<?> source;
        final SparseArrayColumnSource<?> expectedSource;

        final ChunkEquals chunkEquals;

        // transients
        ColumnSource.GetContext sourceGetContext;
        ColumnSource.FillContext sourceFillContext;
        WritableObjectChunk<Object, Values> sourceFillChunk;
        ColumnSource.GetContext expectedGetContext;
        ChunkSink.FillFromContext expectedFillFromContext;
        WritableBooleanChunk equalValuesDest;

        private ColumnInfo(QueryTable tableToValidate, String columnName) {
            this.name = columnName;
            this.modifiedColumnSet = tableToValidate.newModifiedColumnSet(columnName);

            this.source = tableToValidate.getColumnSource(columnName);
            this.isPrimitive = source.getType().isPrimitive();
            this.expectedSource =
                    SparseArrayColumnSource.getSparseMemoryColumnSource(source.getType(), source.getComponentType());

            this.chunkEquals = ChunkEquals.makeEqual(source.getChunkType());
        }

        private ColumnSource.GetContext sourceGetContext() {
            if (sourceGetContext == null) {
                sourceGetContext = this.source.makeGetContext(CHUNK_SIZE, sharedContext);
            }
            return sourceGetContext;
        }

        private ColumnSource.FillContext sourceFillContext() {
            if (sourceFillContext == null) {
                sourceFillContext = isPrimitive ? null : this.source.makeFillContext(CHUNK_SIZE, sharedContext);
            }
            return sourceFillContext;
        }

        private WritableObjectChunk<Object, Values> sourceFillChunk() {
            if (sourceFillChunk == null) {
                sourceFillChunk = isPrimitive ? null : WritableObjectChunk.makeWritableChunk(CHUNK_SIZE);
            }
            return sourceFillChunk;
        }

        private ColumnSource.GetContext expectedGetContext() {
            if (expectedGetContext == null) {
                expectedGetContext = expectedSource.makeGetContext(CHUNK_SIZE, sharedContext);
            }
            return expectedGetContext;
        }

        private ChunkSink.FillFromContext expectedFillFromContext() {
            if (expectedFillFromContext == null) {
                expectedFillFromContext = expectedSource.makeFillFromContext(CHUNK_SIZE);
            }
            return expectedFillFromContext;
        }

        private WritableBooleanChunk equalValuesDest() {
            if (equalValuesDest == null) {
                equalValuesDest = WritableBooleanChunk.makeWritableChunk(CHUNK_SIZE);
            }
            return equalValuesDest;
        }

        @Override
        public void shift(final long beginRange, final long endRange, final long shiftDelta) {
            expectedSource.shift(rowSet.subSetByKeyRange(beginRange, endRange), shiftDelta);
        }

        public void remove(final RowSet toRemove) {
            expectedSource.remove(toRemove);
        }

        private void updateValues(final RowSequence toUpdate, final boolean usePrev) {
            if (isPrimitive) {
                expectedSource.fillFromChunk(expectedFillFromContext(), getSourceChunk(toUpdate, usePrev), toUpdate);
                return;
            }

            sourceFillChunk().setSize(toUpdate.intSize());
            if (usePrev) {
                source.fillPrevChunk(sourceFillContext(), sourceFillChunk(), toUpdate);
            } else {
                source.fillChunk(sourceFillContext(), sourceFillChunk(), toUpdate);
            }

            for (int i = 0; i < sourceFillChunk().size(); ++i) {
                final Object fromSource = sourceFillChunk().get(i);
                final Object wrapped = maybeWrap(fromSource);
                if (fromSource != wrapped) {
                    sourceFillChunk().set(i, wrapped);
                }
            }

            expectedSource.fillFromChunk(expectedFillFromContext(), sourceFillChunk(), toUpdate);
        }

        public void validateValues(final String what, final RowSequence toValidate, final boolean usePrev) {
            Assert.leq(toValidate.size(), "toValidate.size()", CHUNK_SIZE, "CHUNK_SIZE");
            final Chunk<? extends Values> expected =
                    expectedSource.getChunk(expectedGetContext(), toValidate);
            final Chunk<? extends Values> actual = getSourceChunk(toValidate, usePrev);
            chunkEquals.equal(expected, actual, equalValuesDest());
            MutableInt off = new MutableInt();
            toValidate.forAllRowKeys((i) -> {
                off.increment();
                if (equalValuesDest().get(off.intValue() - 1)) {
                    return;
                }

                noteIssue(() -> {
                    Object eValue = expectedSource.get(i);
                    Object aValue = usePrev ? source.getPrev(i) : source.get(i);
                    String chunkEValue = ChunkUtils.extractKeyStringFromChunk(expectedSource.getChunkType(), expected,
                            off.intValue() - 1);
                    String chunkAValue =
                            ChunkUtils.extractKeyStringFromChunk(source.getChunkType(), actual, off.intValue() - 1);
                    return what + (usePrev ? " (previous)" : "") +
                            " columnName=" + name + " k=" + i +
                            " (from source) expected=" + eValue + " actual=" + aValue +
                            " (from chunk) expected=" + chunkEValue + " actual=" + chunkAValue;
                });
            });
        }

        private Chunk<? extends Values> getSourceChunk(RowSequence rowSequence, boolean usePrev) {
            return usePrev ? source.getPrevChunk(sourceGetContext(), rowSequence)
                    : source.getChunk(sourceGetContext(), rowSequence);
        }

        @Override
        public void close() {
            if (sourceGetContext != null) {
                sourceGetContext.close();
                sourceGetContext = null;
            }
            if (sourceFillContext != null) {
                sourceFillContext.close();
                sourceFillContext = null;
            }
            if (sourceFillChunk != null) {
                sourceFillChunk.close();
                sourceFillChunk = null;
            }
            if (expectedGetContext != null) {
                expectedGetContext.close();
                expectedGetContext = null;
            }
            if (expectedFillFromContext != null) {
                expectedFillFromContext.close();
                expectedFillFromContext = null;
            }
            if (equalValuesDest != null) {
                equalValuesDest.close();
                equalValuesDest = null;
            }
        }
    }
}
