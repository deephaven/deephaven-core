package io.deephaven.engine.table.impl.updateby.hashing;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.by.alternatingcolumnsource.AlternatingColumnSource;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableByteArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableIntArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableLongArraySource;
import io.deephaven.engine.table.impl.sources.immutable.ImmutableObjectArraySource;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;

public abstract class AddOnlyUpdateByStateManagerTypedBase extends UpdateByStateManager {
    public static final int CHUNK_SIZE = 4096;
    private static final long MAX_TABLE_SIZE = 1 << 30; // maximum array size

    public static final int EMPTY_RIGHT_VALUE = QueryConstants.NULL_INT;

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

    protected ImmutableIntArraySource stateSource = new ImmutableIntArraySource();
    protected ImmutableIntArraySource alternateStateSource = new ImmutableIntArraySource();

    // the mask for insertion into the main table (this is used so that we can identify whether a slot belongs to the
    // main or alternate table)
    protected int mainInsertMask = 0;
    protected int alternateInsertMask = (int) AlternatingColumnSource.ALTERNATE_SWITCH_MASK;

    protected AddOnlyUpdateByStateManagerTypedBase(ColumnSource<?>[] tableKeySources,
                    ColumnSource<?>[] keySourcesForErrorMessages, int tableSize, double maximumLoadFactor) {

        super(keySourcesForErrorMessages);

        this.tableSize = tableSize;
        Require.leq(tableSize, "tableSize", MAX_TABLE_SIZE);
        Require.gtZero(tableSize, "tableSize");
        Require.eq(Integer.bitCount(tableSize), "Integer.bitCount(tableSize)", 1);
        Require.inRange(maximumLoadFactor, 0.0, 0.95, "maximumLoadFactor");

        mainKeySources = new WritableColumnSource[tableKeySources.length];
        alternateKeySources = new WritableColumnSource[tableKeySources.length];
        chunkTypes = new ChunkType[tableKeySources.length];

        for (int ii = 0; ii < tableKeySources.length; ++ii) {
            chunkTypes[ii] = keySourcesForErrorMessages[ii].getChunkType();
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
        stateSource.ensureCapacity(tableSize);
    }

    @Override
    public void add(SafeCloseable bc, RowSequence orderedKeys, ColumnSource<?>[] sources, MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions) {
    }

    @Override
    public SafeCloseable makeUpdateByBuildContext(ColumnSource<?>[] keySources, long updateSize) {
        return null;
    }

    protected void newAlternate() {
        alternateStateSource = stateSource;
        stateSource = new ImmutableIntArraySource();
        stateSource.ensureCapacity(tableSize);

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
             LongArraySource hashSlots, MutableInt hashSlotOffset);

    abstract protected int rehashInternalPartial(int entriesToRehash, LongArraySource hashSlots);

    abstract protected void migrateFront(LongArraySource hashSlots);

    abstract protected void rehashInternalFull(final int oldSize);
}
