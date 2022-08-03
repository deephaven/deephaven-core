/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.by.alternatingcolumnsource.AlternatingColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableByteArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableObjectArraySource;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import io.deephaven.engine.table.impl.util.TypedHasherUtil;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.ProbeContext;
import io.deephaven.util.QueryConstants;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;

import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getKeyChunks;
import static io.deephaven.engine.table.impl.util.TypedHasherUtil.getPrevKeyChunks;

public abstract class RightIncrementalAsOfJoinStateManagerTypedBase extends RightIncrementalHashedAsOfJoinStateManager {
    public static final int CHUNK_SIZE = 4096;
    private static final long MAX_TABLE_SIZE = 1 << 30; // maximum array size

    public static final byte ENTRY_EMPTY_STATE = QueryConstants.NULL_BYTE;

    // the number of slots in our table
    protected int tableSize;

    // the number of slots in our alternate table, to start with "1" is a lie, but rehashPointer is zero; so our
    // location value is positive and can be compared against rehashPointer safely
    protected int alternateTableSize = 1;

    // how much of the alternate sources are necessary to rehash?
    protected int rehashPointer = 0;

    protected long numEntries = 0;

    // the table will be rehashed to a load factor of targetLoadFactor if our loadFactor exceeds maximumLoadFactor
    // or if it falls below minimum load factor we will instead contract the table
    private final double maximumLoadFactor;

    // the keys for our hash entries
    protected final ChunkType[] chunkTypes;
    protected WritableColumnSource[] mainKeySources;
    protected WritableColumnSource[] alternateKeySources;

    /**
     * We use our side source to originally build the RowSet using builders. When a state is activated (meaning we have
     * a corresponding entry for it on the other side); we'll turn it into an SSA. If we have updates for an inactive
     * state, then we turn it into a WritableRowSet. The entry state tells us what we have on each side, using a nibble
     * for the left and a nibble for the right.
     */
    protected ImmutableObjectArraySource<Object> leftRowSetSource;
    protected ImmutableObjectArraySource<Object> alternateLeftRowSetSource;
    protected ImmutableObjectArraySource<Object> rightRowSetSource;
    protected ImmutableObjectArraySource<Object> alternateRightRowSetSource;

    protected ImmutableByteArraySource stateSource = new ImmutableByteArraySource();
    protected ImmutableByteArraySource alternateStateSource = new ImmutableByteArraySource();

    // the mask for insertion into the main table (this is used so that we can identify whether a slot belongs to the
    // main or alternate table)
    protected int mainInsertMask = 0;
    protected int alternateInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;

    /**
     * Each slot in the hash table has a 'cookie', which we reset by incrementing the cookie generation. The cookie
     * allows us to index into an array source that is passed in for each operation; serving as an intrusive set of
     * modified states (we'll add relevant indices in the probe/build to a RowSet builder).
     */
    protected ImmutableLongArraySource mainCookieSource = new ImmutableLongArraySource();
    protected ImmutableLongArraySource alternateCookieSource;
    protected long cookieGeneration;
    protected int nextCookie;

    protected void resetCookie() {
        cookieGeneration += (10 + nextCookie);
        nextCookie = 0;
    }

    protected long getCookieMain(long slot) {
        return getCookie(mainCookieSource, slot);
    }

    protected long getCookieAlternate(long slot) {
        return getCookie(alternateCookieSource, slot);
    }

    protected long getCookie(ImmutableLongArraySource cookieSource, long slot) {
        long cookie = cookieSource.getUnsafe(slot);
        if (cookie == QueryConstants.NULL_LONG || cookie < cookieGeneration) {
            cookieSource.set(slot, cookie = cookieGeneration + nextCookie);
            nextCookie++;
        }
        return cookie - cookieGeneration;
    }

    protected long makeCookieMain(long slot) {
        return makeCookie(mainCookieSource, slot);
    }

    protected long makeCookie(ImmutableLongArraySource cookieSource, long slot) {
        long cookie = cookieGeneration + nextCookie;
        cookieSource.set(slot, cookie);
        nextCookie++;
        return cookie - cookieGeneration;
    }

    protected void migrateCookie(long cookie, long destinationLocation, LongArraySource hashSlots) {
        if (cookie >= cookieGeneration && cookie - cookieGeneration < nextCookie) {
            hashSlots.set(cookie, destinationLocation | mainInsertMask);
            mainCookieSource.set(destinationLocation, cookie);
        }
    }

    protected RightIncrementalAsOfJoinStateManagerTypedBase(ColumnSource<?>[] tableKeySources,
            ColumnSource<?>[] keySourcesForErrorMessages, int tableSize, double maximumLoadFactor) {
        // region super
        super(keySourcesForErrorMessages);
        // endregion super

        this.tableSize = tableSize;
        Require.leq(tableSize, "tableSize", MAX_TABLE_SIZE);
        Require.gtZero(tableSize, "tableSize");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);
        Require.inRange(maximumLoadFactor, 0.0, 0.95, "maximumLoadFactor");

        // region constructor
        mainKeySources = new WritableColumnSource[tableKeySources.length];
        alternateKeySources = new WritableColumnSource[tableKeySources.length];
        chunkTypes = new ChunkType[tableKeySources.length];
        leftRowSetSource = new ImmutableObjectArraySource<>(Object.class, null);
        alternateLeftRowSetSource = new ImmutableObjectArraySource<>(Object.class, null);
        rightRowSetSource = new ImmutableObjectArraySource<>(Object.class, null);
        alternateRightRowSetSource = new ImmutableObjectArraySource<>(Object.class, null);
        // endregion constructor

        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            chunkTypes[ii] = tableKeySources[ii].getChunkType();
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    tableKeySources[ii].getType(), tableKeySources[ii].getComponentType());
        }

        this.maximumLoadFactor = maximumLoadFactor;

        ensureCapacity(tableSize);
    }

    private void ensureCapacity(int tableSize) {
        for (WritableColumnSource<?> mainKeySource : mainKeySources) {
            mainKeySource.ensureCapacity(tableSize);
        }
        // region ensureCapacity
        leftRowSetSource.ensureCapacity(tableSize);
        rightRowSetSource.ensureCapacity(tableSize);
        stateSource.ensureCapacity(tableSize);
        mainCookieSource.ensureCapacity(tableSize);
        // endregion ensureCapacity
    }

    public static class BuildContext extends TypedHasherUtil.BuildOrProbeContext {
        private BuildContext(ColumnSource<?>[] buildSources, int chunkSize) {
            super(buildSources, chunkSize);
        }

        final MutableInt rehashCredits = new MutableInt(0);
    }

    BuildContext makeBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new BuildContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    ProbeContext makeProbeContext(ColumnSource<?>[] buildSources, long maxSize) {
        return new ProbeContext(buildSources, (int) Math.min(CHUNK_SIZE, maxSize));
    }

    protected static void createBuilder(ImmutableObjectArraySource<RowSetBuilderSequential> source, long location,
            long keyToAdd) {
        final RowSetBuilderSequential builder;
        source.set(location, builder = RowSetFactory.builderSequential());
        builder.appendKey(keyToAdd);
    }

    protected static void addToBuilder(ImmutableObjectArraySource<RowSetBuilderSequential> source, long location,
            long keyToAdd) {
        source.getUnsafe(location).appendKey(keyToAdd);
    }

    protected void addLeftIndex(long tableLocation, long keyToAdd, byte currentState) {
        final boolean isEmpty = (currentState & ENTRY_LEFT_MASK) == ENTRY_LEFT_IS_EMPTY;
        if (isEmpty) {
            final byte newState = (byte) ((currentState & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_BUILDER);
            stateSource.set(tableLocation, newState);
            // noinspection unchecked
            createBuilder((ImmutableObjectArraySource) leftRowSetSource, tableLocation, keyToAdd);
        } else {
            // noinspection unchecked
            addToBuilder((ImmutableObjectArraySource) leftRowSetSource, tableLocation, keyToAdd);
        }
    }

    protected void addRightIndex(long tableLocation, long keyToAdd, byte currentState) {
        final boolean isEmpty = (currentState & ENTRY_RIGHT_MASK) == ENTRY_RIGHT_IS_EMPTY;
        if (isEmpty) {
            final byte newState = (byte) ((currentState & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_BUILDER);
            stateSource.set(tableLocation, newState);
            // noinspection unchecked
            createBuilder((ImmutableObjectArraySource) rightRowSetSource, tableLocation, keyToAdd);
        } else {
            // noinspection unchecked
            addToBuilder((ImmutableObjectArraySource) rightRowSetSource, tableLocation, keyToAdd);
        }
    }

    protected void addAlternateLeftIndex(long tableLocation, long keyToAdd, byte currentState) {
        final boolean isEmpty = (currentState & ENTRY_LEFT_MASK) == ENTRY_LEFT_IS_EMPTY;
        if (isEmpty) {
            final byte newState = (byte) ((currentState & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_BUILDER);
            alternateStateSource.set(tableLocation, newState);
            // noinspection unchecked
            createBuilder((ImmutableObjectArraySource) alternateLeftRowSetSource, tableLocation, keyToAdd);
        } else {
            // noinspection unchecked
            addToBuilder((ImmutableObjectArraySource) alternateLeftRowSetSource, tableLocation, keyToAdd);
        }
    }

    protected void addAlternateRightIndex(long tableLocation, long keyToAdd, byte currentState) {
        final boolean isEmpty = (currentState & ENTRY_RIGHT_MASK) == ENTRY_RIGHT_IS_EMPTY;
        if (isEmpty) {
            final byte newState = (byte) ((currentState & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_BUILDER);
            alternateStateSource.set(tableLocation, newState);
            // noinspection unchecked
            createBuilder((ImmutableObjectArraySource) alternateRightRowSetSource, tableLocation, keyToAdd);
        } else {
            // noinspection unchecked
            addToBuilder((ImmutableObjectArraySource) alternateRightRowSetSource, tableLocation, keyToAdd);
        }
    }

    protected void buildTable(
            final boolean initialBuild,
            final BuildContext bc,
            final RowSequence buildRows,
            final ColumnSource<?>[] buildSources,
            final LongArraySource hashSlots,
            final TypedHasherUtil.BuildHandler buildHandler) {
        try (final RowSequence.Iterator rsIt = buildRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[buildSources.length];

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(bc.chunkSize);

                while (doRehash(initialBuild, bc.rehashCredits, chunkOk.intSize(), hashSlots)) {
                    migrateFront(hashSlots);
                }

                getKeyChunks(buildSources, bc.getContexts, sourceKeyChunks, chunkOk);

                buildHandler.doBuild(chunkOk, sourceKeyChunks);

                bc.resetSharedContexts();
            }
        }
    }

    private class LeftBuildHandler implements TypedHasherUtil.BuildHandler {
        final LongArraySource hashSlots;
        final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders;

        private LeftBuildHandler(final LongArraySource hashSlots) {
            this.hashSlots = hashSlots;
            this.sequentialBuilders = null;
        }

        private LeftBuildHandler(final LongArraySource hashSlots,
                final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
            this.hashSlots = hashSlots;
            this.sequentialBuilders = sequentialBuilders;
        }

        @Override
        public void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            hashSlots.ensureCapacity(nextCookie + chunkOk.intSize());
            buildFromLeftSide(chunkOk, sourceKeyChunks, hashSlots, sequentialBuilders);
        }
    }

    private class RightBuildHandler implements TypedHasherUtil.BuildHandler {
        final LongArraySource hashSlots;
        final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders;

        private RightBuildHandler(final LongArraySource hashSlots) {
            this.hashSlots = hashSlots;
            this.sequentialBuilders = null;
        }

        private RightBuildHandler(final LongArraySource hashSlots,
                final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
            this.hashSlots = hashSlots;
            this.sequentialBuilders = sequentialBuilders;
        }

        @Override
        public void doBuild(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            hashSlots.ensureCapacity(nextCookie + chunkOk.intSize());
            buildFromRightSide(chunkOk, sourceKeyChunks, hashSlots, sequentialBuilders);
        }
    }

    @Override
    public int buildFromLeftSide(RowSequence leftRowSet, ColumnSource<?>[] leftSources,
            @NotNull final LongArraySource addedSlots) {
        if (leftRowSet.isEmpty()) {
            return 0;
        }
        try (final BuildContext bc = makeBuildContext(leftSources, leftRowSet.size())) {
            int startCookie = nextCookie;
            buildTable(true, bc, leftRowSet, leftSources, addedSlots, new LeftBuildHandler(addedSlots));
            return nextCookie - startCookie;
        }
    }

    @Override
    public int buildFromRightSide(RowSequence rightRowSet, ColumnSource<?>[] rightSources,
            @NotNull final LongArraySource addedSlots, int usedSlots) {
        if (rightRowSet.isEmpty()) {
            return usedSlots;
        }
        try (final BuildContext bc = makeBuildContext(rightSources, rightRowSet.size())) {
            int startCookie = nextCookie;
            buildTable(true, bc, rightRowSet, rightSources, addedSlots, new RightBuildHandler(addedSlots));
            return usedSlots + (nextCookie - startCookie);
        }
    }

    @Override
    public int markForRemoval(RowSet restampRemovals, ColumnSource<?>[] sources, LongArraySource slots,
            ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return accumulateIndices(restampRemovals, sources, slots, sequentialBuilders, true);
    }

    @Override
    public int probeAdditions(RowSet restampAdditions, ColumnSource<?>[] sources, LongArraySource slots,
            ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return accumulateIndices(restampAdditions, sources, slots, sequentialBuilders, false);
    }

    @Override
    public int gatherShiftIndex(RowSet restampAdditions, ColumnSource<?>[] sources, LongArraySource slots,
            ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return accumulateIndices(restampAdditions, sources, slots, sequentialBuilders, true);
    }

    public int gatherModifications(RowSet restampAdditions, ColumnSource<?>[] sources, LongArraySource slots,
            ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
        return accumulateIndices(restampAdditions, sources, slots, sequentialBuilders, false);
    }

    protected void probeTable(
            final ProbeContext pc,
            final RowSequence probeRows,
            final boolean usePrev,
            final ColumnSource<?>[] probeSources,
            final TypedHasherUtil.ProbeHandler handler) {
        try (final RowSequence.Iterator rsIt = probeRows.getRowSequenceIterator()) {
            // noinspection unchecked
            final Chunk<Values>[] sourceKeyChunks = new Chunk[probeSources.length];

            while (rsIt.hasMore()) {
                final RowSequence chunkOk = rsIt.getNextRowSequenceWithLength(pc.chunkSize);

                if (usePrev) {
                    getPrevKeyChunks(probeSources, pc.getContexts, sourceKeyChunks, chunkOk);
                } else {
                    getKeyChunks(probeSources, pc.getContexts, sourceKeyChunks, chunkOk);
                }

                handler.doProbe(chunkOk, sourceKeyChunks);

                pc.resetSharedContexts();
            }
        }
    }

    private class RightProbeHandler implements TypedHasherUtil.ProbeHandler {
        final LongArraySource hashSlots;
        final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders;

        private RightProbeHandler() {
            this.hashSlots = null;
            this.sequentialBuilders = null;
        }

        private RightProbeHandler(final LongArraySource hashSlots,
                final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {
            this.hashSlots = hashSlots;
            this.sequentialBuilders = sequentialBuilders;
        }

        @Override
        public void doProbe(RowSequence chunkOk, Chunk<Values>[] sourceKeyChunks) {
            probeRightSide(chunkOk, sourceKeyChunks, hashSlots, sequentialBuilders);
        }
    }

    @Override
    public void probeRightInitial(RowSequence rightIndex, ColumnSource<?>[] rightSources) {
        if (rightIndex.isEmpty()) {
            return;
        }
        try (final ProbeContext pc = makeProbeContext(rightSources, rightIndex.size())) {
            probeTable(pc, rightIndex, false, rightSources, new RightProbeHandler());
        }
    }

    private int accumulateIndices(RowSet rowSet, ColumnSource<?>[] sources, LongArraySource slots,
            ObjectArraySource<RowSetBuilderSequential> sequentialBuilders, boolean usePrev) {
        resetCookie();

        if (rowSet.isEmpty()) {
            return 0;
        }

        try (final ProbeContext pc = makeProbeContext(sources, rowSet.size())) {
            probeTable(pc, rowSet, usePrev, sources, new RightProbeHandler(slots, sequentialBuilders));
        }

        return nextCookie;
    }

    @Override
    public int buildAdditions(boolean isLeftSide, RowSet additions, ColumnSource<?>[] sources, LongArraySource slots,
            ObjectArraySource<RowSetBuilderSequential> sequentialBuilders) {

        resetCookie();

        if (additions.isEmpty()) {
            return 0;
        }

        try (final BuildContext bc = makeBuildContext(sources, additions.size())) {
            if (isLeftSide) {
                buildTable(false, bc, additions, sources, slots, new LeftBuildHandler(slots, sequentialBuilders));
            } else {
                buildTable(false, bc, additions, sources, slots, new RightBuildHandler(slots, sequentialBuilders));
            }
            return nextCookie;
        }
    }

    @Override
    public int getTableSize() {
        return tableSize;
    }

    @Override
    public int getOverflowSize() {
        return 0;
    }

    @Override
    public WritableRowSet getAndClearLeftIndex(long slot) {
        final RowSetBuilderSequential builder = (RowSetBuilderSequential) leftRowSetSource.getUnsafe(slot);
        leftRowSetSource.set(slot, null);
        if (builder == null) {
            return null;
        }
        return builder.build();
    }

    @Override
    public byte getState(long slot) {
        final ImmutableByteArraySource source;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            source = stateSource;
        } else {
            source = alternateStateSource;
        }
        // clear the mask bits
        slot = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;

        return source.getUnsafe(slot);
    }

    @Override
    public SegmentedSortedArray getRightSsa(long slot) {
        final ImmutableByteArraySource source;
        final ImmutableObjectArraySource<Object> rowSetSource;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            source = stateSource;
            rowSetSource = rightRowSetSource;
        } else {
            source = alternateStateSource;
            rowSetSource = alternateRightRowSetSource;
        }
        // clear the mask bits
        slot = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;

        final byte entryType = source.getUnsafe(slot);
        if ((entryType & ENTRY_RIGHT_MASK) == ENTRY_RIGHT_IS_SSA) {
            return (SegmentedSortedArray) rowSetSource.getUnsafe(slot);
        }
        throw new IllegalStateException();
    }

    @Override
    public SegmentedSortedArray getRightSsa(long slot, Function<RowSet, SegmentedSortedArray> ssaFactory) {
        final ImmutableByteArraySource source;
        final ImmutableObjectArraySource<Object> rowSetSource;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            source = stateSource;
            rowSetSource = rightRowSetSource;
        } else {
            source = alternateStateSource;
            rowSetSource = alternateRightRowSetSource;
        }
        // clear the mask bits
        slot = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;

        final byte entryType = source.getUnsafe(slot);
        switch (entryType & ENTRY_RIGHT_MASK) {
            case ENTRY_RIGHT_IS_EMPTY:
                return makeSsaFromEmpty(slot, ssaFactory, rowSetSource, source,
                        (byte) ((entryType & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_SSA));
            case ENTRY_RIGHT_IS_INDEX:
                return makeSsaFromIndex(slot, ssaFactory, rowSetSource, source,
                        (byte) ((entryType & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_SSA));
            case ENTRY_RIGHT_IS_BUILDER:
                return makeSsaFromBuilder(slot, ssaFactory, rowSetSource, source,
                        (byte) ((entryType & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_SSA));
            case ENTRY_RIGHT_IS_SSA:
                return (SegmentedSortedArray) rowSetSource.getUnsafe(slot);
        }
        throw new IllegalStateException();
    }

    @Override
    public WritableRowSet getRightIndex(long slot) {
        final ImmutableByteArraySource source;
        final ImmutableObjectArraySource<Object> rowSetSource;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            source = stateSource;
            rowSetSource = rightRowSetSource;
        } else {
            source = alternateStateSource;
            rowSetSource = alternateRightRowSetSource;
        }
        // clear the mask bits
        slot = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;

        final byte entryType = source.getUnsafe(slot);
        if ((entryType & ENTRY_RIGHT_MASK) == ENTRY_RIGHT_IS_INDEX) {
            return (WritableRowSet) rowSetSource.getUnsafe(slot);
        } else if ((entryType & ENTRY_RIGHT_MASK) == ENTRY_RIGHT_IS_BUILDER) {
            final WritableRowSet rowSet = ((RowSetBuilderSequential) rowSetSource.getUnsafe(slot)).build();
            rowSetSource.set(slot, rowSet);
            source.set(slot, (byte) ((entryType & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_INDEX));
            return rowSet;
        }
        throw new IllegalStateException();
    }

    @Override
    public WritableRowSet getLeftIndex(long slot) {
        final ImmutableByteArraySource source;
        final ImmutableObjectArraySource<Object> rowSetSource;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            source = stateSource;
            rowSetSource = leftRowSetSource;
        } else {
            source = alternateStateSource;
            rowSetSource = alternateLeftRowSetSource;
        }
        // clear the mask bits
        slot = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;

        final byte entryType = source.getUnsafe(slot);
        if ((entryType & ENTRY_LEFT_MASK) == ENTRY_LEFT_IS_INDEX) {
            return (WritableRowSet) rowSetSource.getUnsafe(slot);
        } else if ((entryType & ENTRY_LEFT_MASK) == ENTRY_LEFT_IS_BUILDER) {
            final WritableRowSet rowSet = ((RowSetBuilderSequential) rowSetSource.getUnsafe(slot)).build();
            rowSetSource.set(slot, rowSet);
            source.set(slot, (byte) ((entryType & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_INDEX));
            return rowSet;
        }
        throw new IllegalStateException();
    }

    @Override
    public void setLeftIndex(long slot, RowSet rowSet) {
        final ImmutableByteArraySource source;
        final ImmutableObjectArraySource<Object> rowSetSource;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            source = stateSource;
            rowSetSource = leftRowSetSource;
        } else {
            source = alternateStateSource;
            rowSetSource = alternateLeftRowSetSource;
        }
        // clear the mask bits
        slot = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;

        final byte entryType = source.getUnsafe(slot);
        if ((entryType & ENTRY_LEFT_MASK) == ENTRY_LEFT_IS_EMPTY) {
            source.set(slot, (byte) ((entryType & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_INDEX));
            rowSetSource.set(slot, rowSet);
            return;
        }
        throw new IllegalStateException();
    }

    @Override
    public void setRightIndex(long slot, RowSet rowSet) {
        final ImmutableByteArraySource source;
        final ImmutableObjectArraySource<Object> rowSetSource;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            source = stateSource;
            rowSetSource = rightRowSetSource;
        } else {
            source = alternateStateSource;
            rowSetSource = alternateRightRowSetSource;
        }
        // clear the mask bits
        slot = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;

        final byte entryType = source.getUnsafe(slot);
        if ((entryType & ENTRY_RIGHT_MASK) == ENTRY_RIGHT_IS_EMPTY) {
            source.set(slot, (byte) ((entryType & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_INDEX));
            rowSetSource.set(slot, rowSet);
            return;
        }
        throw new IllegalStateException();
    }

    @Override
    public SegmentedSortedArray getLeftSsa(long slot) {
        final ImmutableByteArraySource source;
        final ImmutableObjectArraySource<Object> rowSetSource;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            source = stateSource;
            rowSetSource = leftRowSetSource;
        } else {
            source = alternateStateSource;
            rowSetSource = alternateLeftRowSetSource;
        }
        // clear the mask bits
        slot = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;

        final byte entryType = source.getUnsafe(slot);
        if ((entryType & ENTRY_LEFT_MASK) == ENTRY_LEFT_IS_SSA) {
            return (SegmentedSortedArray) rowSetSource.getUnsafe(slot);
        }
        throw new IllegalStateException();
    }

    @Override
    public SegmentedSortedArray getLeftSsa(long slot, Function<RowSet, SegmentedSortedArray> ssaFactory) {
        final ImmutableByteArraySource source;
        final ImmutableObjectArraySource<Object> rowSetSource;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            source = stateSource;
            rowSetSource = leftRowSetSource;
        } else {
            source = alternateStateSource;
            rowSetSource = alternateLeftRowSetSource;
        }
        // clear the mask bits
        slot = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;

        final byte entryType = source.getUnsafe(slot);
        switch (entryType & ENTRY_LEFT_MASK) {
            case ENTRY_LEFT_IS_EMPTY:
                return makeSsaFromEmpty(slot, ssaFactory, rowSetSource, source,
                        (byte) ((entryType & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_SSA));
            case ENTRY_LEFT_IS_BUILDER:
                return makeSsaFromBuilder(slot, ssaFactory, rowSetSource, source,
                        (byte) ((entryType & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_SSA));
            case ENTRY_LEFT_IS_INDEX:
                return makeSsaFromIndex(slot, ssaFactory, rowSetSource, source,
                        (byte) ((entryType & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_SSA));
            case ENTRY_LEFT_IS_SSA:
                return (SegmentedSortedArray) rowSetSource.getUnsafe(slot);
        }
        throw new IllegalStateException();
    }

    @Override
    public SegmentedSortedArray getLeftSsaOrIndex(long slot, MutableObject<WritableRowSet> indexOutput) {
        final ImmutableByteArraySource source;
        final ImmutableObjectArraySource<Object> rowSetSource;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            source = stateSource;
            rowSetSource = leftRowSetSource;
        } else {
            source = alternateStateSource;
            rowSetSource = alternateLeftRowSetSource;
        }
        // clear the mask bits
        slot = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;

        final byte entryType = source.getUnsafe(slot);
        final byte stateValueForIndex = (byte) ((entryType & ENTRY_RIGHT_MASK) | ENTRY_LEFT_IS_INDEX);
        return getSsaOrIndex(indexOutput, slot, leftEntryAsRightType(entryType), rowSetSource, source,
                stateValueForIndex);
    }

    @Override
    public SegmentedSortedArray getRightSsaOrIndex(long slot, MutableObject<WritableRowSet> indexOutput) {
        final ImmutableByteArraySource source;
        final ImmutableObjectArraySource<Object> rowSetSource;
        if ((slot & AlternatingColumnSource.ALTERNATE_SWITCH_MASK) == mainInsertMask) {
            source = stateSource;
            rowSetSource = rightRowSetSource;
        } else {
            source = alternateStateSource;
            rowSetSource = alternateRightRowSetSource;
        }
        // clear the mask bits
        slot = slot & AlternatingColumnSource.ALTERNATE_INNER_MASK;

        final byte entryType = source.getUnsafe(slot);
        final byte stateValueForIndex = (byte) ((entryType & ENTRY_LEFT_MASK) | ENTRY_RIGHT_IS_INDEX);
        return getSsaOrIndex(indexOutput, slot, getRightEntryType(entryType), rowSetSource, source,
                stateValueForIndex);
    }

    @Nullable
    private static SegmentedSortedArray getSsaOrIndex(MutableObject<WritableRowSet> indexOutput, long location,
            byte entryType, ImmutableObjectArraySource<Object> sideSource, ImmutableByteArraySource stateSource,
            byte stateValueForIndex) {
        switch (entryType) {
            case ENTRY_RIGHT_IS_SSA:
                return (SegmentedSortedArray) sideSource.getUnsafe(location);
            case ENTRY_RIGHT_IS_INDEX:
                indexOutput.setValue((WritableRowSet) sideSource.getUnsafe(location));
                return null;
            case ENTRY_RIGHT_IS_EMPTY: {
                final WritableRowSet emptyRowSet = RowSetFactory.empty();
                sideSource.set(location, emptyRowSet);
                stateSource.set(location, stateValueForIndex);
                indexOutput.setValue(emptyRowSet);
                return null;
            }
            case ENTRY_RIGHT_IS_BUILDER: {
                final WritableRowSet rowSet = ((RowSetBuilderSequential) sideSource.getUnsafe(location)).build();
                sideSource.set(location, rowSet);
                stateSource.set(location, stateValueForIndex);
                indexOutput.setValue(rowSet);
                return null;
            }
            default:
                throw new IllegalStateException();
        }
    }

    @Nullable
    private SegmentedSortedArray makeSsaFromBuilder(long slot, Function<RowSet, SegmentedSortedArray> ssaFactory,
            ImmutableObjectArraySource<Object> ssaSource, ImmutableByteArraySource stateSource, byte newState) {
        final RowSetBuilderSequential builder = (RowSetBuilderSequential) ssaSource.getUnsafe(slot);
        final RowSet rowSet;
        if (builder == null) {
            rowSet = RowSetFactory.empty();
        } else {
            rowSet = builder.build();
        }
        return makeSsaFromIndex(slot, ssaFactory, ssaSource, stateSource, newState, rowSet);
    }

    @Nullable
    private SegmentedSortedArray makeSsaFromEmpty(long slot, Function<RowSet, SegmentedSortedArray> ssaFactory,
            ImmutableObjectArraySource<Object> ssaSource, ImmutableByteArraySource stateSource, byte newState) {
        return makeSsaFromIndex(slot, ssaFactory, ssaSource, stateSource, newState, RowSetFactory.empty());
    }

    @Nullable
    private SegmentedSortedArray makeSsaFromIndex(long slot, Function<RowSet, SegmentedSortedArray> ssaFactory,
            ImmutableObjectArraySource<Object> ssaSource, ImmutableByteArraySource stateSource, byte newState) {
        return makeSsaFromIndex(slot, ssaFactory, ssaSource, stateSource, newState, (RowSet) ssaSource.getUnsafe(slot));
    }

    private SegmentedSortedArray makeSsaFromIndex(long slot, Function<RowSet, SegmentedSortedArray> ssaFactory,
            ImmutableObjectArraySource<Object> ssaSource, ImmutableByteArraySource stateSource, byte newState,
            RowSet rowSet) {
        stateSource.set(slot, newState);
        final SegmentedSortedArray ssa = ssaFactory.apply(rowSet);
        rowSet.close();
        ssaSource.set(slot, ssa);
        return ssa;
    }

    protected void newAlternate() {
        alternateRightRowSetSource = rightRowSetSource;
        rightRowSetSource = new ImmutableObjectArraySource<>(Object.class, null);
        rightRowSetSource.ensureCapacity(tableSize);

        alternateLeftRowSetSource = leftRowSetSource;
        leftRowSetSource = new ImmutableObjectArraySource<>(Object.class, null);
        leftRowSetSource.ensureCapacity(tableSize);

        alternateStateSource = stateSource;
        stateSource = new ImmutableByteArraySource();
        stateSource.ensureCapacity(tableSize);

        alternateCookieSource = mainCookieSource;
        mainCookieSource = new ImmutableLongArraySource();
        mainCookieSource.ensureCapacity(tableSize);

        if (mainInsertMask == 0) {
            mainInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;
            alternateInsertMask = 0;
        } else {
            mainInsertMask = 0;
            alternateInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;
        }
    }

    protected void clearAlternate() {
        for (int ii = 0; ii < mainKeySources.length; ++ii) {
            alternateKeySources[ii] = null;
        }

    }

    /**
     * @param fullRehash should we rehash the entire table (if false, we rehash incrementally)
     * @param rehashCredits the number of entries this operation has rehashed (input/output)
     * @param nextChunkSize the size of the chunk we are processing
     * @return true if a front migration is required
     */
    public boolean doRehash(boolean fullRehash, MutableInt rehashCredits, int nextChunkSize,
            LongArraySource hashSlots) {
        if (rehashPointer > 0) {
            final int requiredRehash = nextChunkSize - rehashCredits.intValue();
            if (requiredRehash <= 0) {
                return false;
            }

            // before building, we need to do at least as much rehash work as we would do build work
            rehashCredits.add(rehashInternalPartial(requiredRehash, hashSlots));
            if (rehashPointer == 0) {
                clearAlternate();
            }
        }

        int oldTableSize = tableSize;
        while (rehashRequired(nextChunkSize)) {
            tableSize *= 2;

            if (tableSize < 0 || tableSize > MAX_TABLE_SIZE) {
                throw new UnsupportedOperationException("Hash table exceeds maximum size!");
            }
        }

        if (oldTableSize == tableSize) {
            return false;
        }

        // we can't give the caller credit for rehashes with the old table, we need to begin migrating things again
        if (rehashCredits.intValue() > 0) {
            rehashCredits.setValue(0);
        }

        if (fullRehash) {
            // if we are doing a full rehash, we need to ditch the alternate
            if (rehashPointer > 0) {
                rehashInternalPartial((int) numEntries, hashSlots);
                clearAlternate();
            }

            rehashInternalFull(oldTableSize);

            return false;
        }

        Assert.eqZero(rehashPointer, "rehashPointer");

        for (int ii = 0; ii < mainKeySources.length; ++ii) {
            alternateKeySources[ii] = mainKeySources[ii];
            mainKeySources[ii] = InMemoryColumnSource.getImmutableMemoryColumnSource(tableSize,
                    alternateKeySources[ii].getType(), alternateKeySources[ii].getComponentType());
            mainKeySources[ii].ensureCapacity(tableSize);
        }
        alternateTableSize = oldTableSize;
        if (numEntries > 0) {
            rehashPointer = alternateTableSize;
        }

        newAlternate();

        return true;
    }

    public boolean rehashRequired(int nextChunkSize) {
        return (numEntries + nextChunkSize) > (tableSize * maximumLoadFactor);
    }

    protected int hashToTableLocation(int hash) {
        return hash & (tableSize - 1);
    }

    protected int hashToTableLocationAlternate(int hash) {
        return hash & (alternateTableSize - 1);
    }

    abstract protected void buildFromLeftSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            final LongArraySource hashSlots, final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    abstract protected void buildFromRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            final LongArraySource hashSlots, final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    abstract protected void probeRightSide(RowSequence rowSequence, Chunk[] sourceKeyChunks,
            final LongArraySource hashSlots, final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders);

    abstract protected int rehashInternalPartial(int entriesToRehash, LongArraySource hashSlots);

    abstract protected void migrateFront(LongArraySource hashSlots);

    abstract protected void rehashInternalFull(final int oldSize);
}
