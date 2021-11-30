package io.deephaven.engine.table.impl.select.formula;

import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public class FormulaKernelAdapter extends io.deephaven.engine.table.impl.select.Formula {
    private final FormulaSourceDescriptor sourceDescriptor;
    private final Map<String, ? extends ColumnSource> columnSources;
    private final FormulaKernel kernel;
    private final ChunkType chunkType;
    private final GetHandler getHandler;

    public FormulaKernelAdapter(final TrackingRowSet rowSet, final FormulaSourceDescriptor sourceDescriptor,
            final Map<String, ? extends ColumnSource> columnSources,
            final FormulaKernel kernel) {
        super(rowSet);
        this.sourceDescriptor = sourceDescriptor;
        this.columnSources = columnSources;
        this.kernel = kernel;
        Class rt = sourceDescriptor.returnType;
        if (rt != Boolean.class && io.deephaven.util.type.TypeUtils.isBoxedType(rt)) {
            rt = io.deephaven.util.type.TypeUtils.getUnboxedType(rt);
        }
        this.chunkType = ChunkType.fromElementType(rt);
        if (rt == byte.class) {
            getHandler = (key, prev) -> io.deephaven.util.type.TypeUtils.box(handleGetByte(key, prev));
        } else if (rt == Boolean.class) {
            getHandler = this::handleGetBoolean;
        } else if (rt == char.class) {
            getHandler = (key, prev) -> io.deephaven.util.type.TypeUtils.box(handleGetChar(key, prev));
        } else if (rt == double.class) {
            getHandler = (key, prev) -> io.deephaven.util.type.TypeUtils.box(handleGetDouble(key, prev));
        } else if (rt == float.class) {
            getHandler = (key, prev) -> io.deephaven.util.type.TypeUtils.box(handleGetFloat(key, prev));
        } else if (rt == int.class) {
            getHandler = (key, prev) -> io.deephaven.util.type.TypeUtils.box(handleGetInt(key, prev));
        } else if (rt == long.class) {
            getHandler = (key, prev) -> io.deephaven.util.type.TypeUtils.box(handleGetLong(key, prev));
        } else if (rt == short.class) {
            getHandler = (key, prev) -> TypeUtils.box(handleGetShort(key, prev));
        } else {
            getHandler = this::handleGetObject;
        }
    }

    @Override
    public Object get(final long k) {
        return getHandler.apply(k, false);
    }

    @Override
    public Object getPrev(final long k) {
        return getHandler.apply(k, true);
    }

    @Override
    public Boolean getBoolean(final long k) {
        return handleGetBoolean(k, false);
    }

    @Override
    public byte getByte(final long k) {
        return handleGetByte(k, false);
    }

    @Override
    public char getChar(final long k) {
        return handleGetChar(k, false);
    }

    @Override
    public double getDouble(final long k) {
        return handleGetDouble(k, false);
    }

    @Override
    public float getFloat(final long k) {
        return handleGetFloat(k, false);
    }

    @Override
    public int getInt(final long k) {
        return handleGetInt(k, false);
    }

    public long getLong(final long k) {
        return handleGetLong(k, false);
    }

    @Override
    public short getShort(final long k) {
        return handleGetShort(k, false);
    }

    @Override
    public Boolean getPrevBoolean(final long k) {
        return handleGetBoolean(k, true);
    }

    @Override
    public byte getPrevByte(final long k) {
        return handleGetByte(k, true);
    }

    @Override
    public char getPrevChar(final long k) {
        return handleGetChar(k, true);
    }

    @Override
    public double getPrevDouble(final long k) {
        return handleGetDouble(k, true);
    }

    @Override
    public float getPrevFloat(final long k) {
        return handleGetFloat(k, true);
    }

    @Override
    public int getPrevInt(final long k) {
        return handleGetInt(k, true);
    }

    @Override
    public long getPrevLong(final long k) {
        return handleGetLong(k, true);
    }

    @Override
    public short getPrevShort(final long k) {
        return handleGetShort(k, true);
    }

    private Object handleGetObject(final long k, boolean usePrev) {
        try (final WritableObjectChunk<Object, Values> __dest = WritableObjectChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private Boolean handleGetBoolean(final long k, boolean usePrev) {
        try (final WritableObjectChunk<Boolean, Values> __dest = WritableObjectChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private byte handleGetByte(final long k, boolean usePrev) {
        try (final WritableByteChunk<Values> __dest = WritableByteChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private char handleGetChar(final long k, boolean usePrev) {
        try (final WritableCharChunk<Values> __dest = WritableCharChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private double handleGetDouble(final long k, boolean usePrev) {
        try (final WritableDoubleChunk<Values> __dest = WritableDoubleChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private float handleGetFloat(final long k, boolean usePrev) {
        try (final WritableFloatChunk<Values> __dest = WritableFloatChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private int handleGetInt(final long k, boolean usePrev) {
        try (final WritableIntChunk<Values> __dest = WritableIntChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private long handleGetLong(final long k, boolean usePrev) {
        try (final WritableLongChunk<Values> __dest = WritableLongChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private short handleGetShort(final long k, boolean usePrev) {
        try (final WritableShortChunk<Values> __dest = WritableShortChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private void commonGetLogic(WritableChunk<Values> __dest, final long k, boolean usePrev) {
        try (final RowSet rs = RowSetFactory.fromKeys(k)) {
            try (final AdapterContext context = makeFillContext(1)) {
                fillChunkHelper(context, __dest, rs, usePrev, true);
            }
        }
    }

    @Override
    protected ChunkType getChunkType() {
        return chunkType;
    }

    @Override
    public void fillChunk(@NotNull final FillContext __context,
            @NotNull final WritableChunk<? super Values> __destination,
            @NotNull final RowSequence __rowSequence) {
        fillChunkHelper(__context, __destination, __rowSequence, false, true);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext __context,
            @NotNull final WritableChunk<? super Values> __destination,
            @NotNull final RowSequence __rowSequence) {
        fillChunkHelper(__context, __destination, __rowSequence, true, true);
    }

    private void fillChunkHelper(@NotNull final FillContext __context,
            @NotNull final WritableChunk<? super Values> __destination,
            @NotNull final RowSequence __rowSequence, final boolean usePrev, final boolean lookupI) {
        final int RowSequenceSize = __rowSequence.intSize();
        __destination.setSize(RowSequenceSize);
        // Shortcut if __rowSequence is empty
        if (RowSequenceSize == 0) {
            return;
        }
        final AdapterContext __typedContext = (AdapterContext) __context;
        final Chunk<? extends Values>[] sourceChunks = new Chunk[sourceDescriptor.sources.length];
        try (final RowSequence flat = RowSetFactory.flat(__rowSequence.size())) {
            for (int ii = 0; ii < sourceDescriptor.sources.length; ++ii) {
                final String name = sourceDescriptor.sources[ii];
                switch (name) {
                    case "i": {
                        if (lookupI) {
                            // Potentially repeated work w.r.t. "ii".
                            __typedContext.iChunk.setSize(0);
                            __rowSet.invert(__rowSequence.asRowSet()).forAllRowKeys(longVal -> {
                                final int i = LongSizedDataStructure.intSize("FormulaNubbin i usage", longVal);
                                __typedContext.iChunk.add(i);
                            });
                        } else {
                            // sequential i
                            for (int pos = 0; pos < RowSequenceSize; ++pos) {
                                __typedContext.iChunk.set(pos, pos);
                            }
                            __typedContext.iChunk.setSize(RowSequenceSize);
                        }
                        sourceChunks[ii] = __typedContext.iChunk;
                        break;
                    }
                    case "ii": {
                        if (lookupI) {
                            __rowSet.invert(__rowSequence.asRowSet()).fillRowKeyChunk(__typedContext.iiChunk);
                        } else {
                            // sequential ii
                            for (int pos = 0; pos < RowSequenceSize; ++pos) {
                                __typedContext.iiChunk.set(pos, pos);
                            }
                            __typedContext.iiChunk.setSize(RowSequenceSize);
                        }
                        sourceChunks[ii] = __typedContext.iiChunk;
                        break;
                    }
                    case "k": {
                        __rowSequence.fillRowKeyChunk(__typedContext.kChunk);
                        sourceChunks[ii] = __typedContext.kChunk;
                        break;
                    }
                    default: {
                        final ColumnSource cs = columnSources.get(name);
                        final ColumnSource.GetContext ctx = __typedContext.sourceContexts[ii];
                        sourceChunks[ii] =
                                usePrev ? cs.getPrevChunk(ctx, __rowSequence) : cs.getChunk(ctx, __rowSequence);
                    }
                }
            }
        }
        kernel.applyFormulaChunk(__typedContext.kernelContext, __destination, sourceChunks);
    }

    @Override
    public AdapterContext makeFillContext(final int chunkCapacity) {
        WritableIntChunk<OrderedRowKeys> iChunk = null;
        WritableLongChunk<OrderedRowKeys> iiChunk = null;
        WritableLongChunk<OrderedRowKeys> kChunk = null;

        final String[] sources = sourceDescriptor.sources;
        // Create whichever of the three special chunks we need
        for (final String source : sources) {
            switch (source) {
                case "i":
                    iChunk = WritableIntChunk.makeWritableChunk(chunkCapacity);
                    break;
                case "ii":
                    iiChunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
                    break;
                case "k":
                    kChunk = WritableLongChunk.makeWritableChunk(chunkCapacity);
                    break;
            }
        }

        // Make contexts -- we leave nulls in the slots where i, ii, or k would be.
        final ColumnSource.GetContext[] sourceContexts = new ColumnSource.GetContext[sources.length];
        for (int ii = 0; ii < sources.length; ++ii) {
            final String name = sources[ii];
            if (name.equals("i") || name.equals("ii") || name.equals("k")) {
                continue;
            }
            final ColumnSource cs = columnSources.get(name);
            sourceContexts[ii] = cs.makeGetContext(chunkCapacity);
        }
        final FillContext kernelContext = kernel.makeFillContext(chunkCapacity);
        return new AdapterContext(iChunk, iiChunk, kChunk, sourceContexts, kernelContext);
    }

    private static class AdapterContext implements FillContext {
        final WritableIntChunk<OrderedRowKeys> iChunk;
        final WritableLongChunk<OrderedRowKeys> iiChunk;
        final WritableLongChunk<OrderedRowKeys> kChunk;
        final ColumnSource.GetContext[] sourceContexts;
        final FillContext kernelContext;

        AdapterContext(WritableIntChunk<OrderedRowKeys> iChunk,
                WritableLongChunk<OrderedRowKeys> iiChunk,
                WritableLongChunk<OrderedRowKeys> kChunk,
                ColumnSource.GetContext[] sourceContexts,
                FillContext kernelContext) {
            this.iChunk = iChunk;
            this.iiChunk = iiChunk;
            this.kChunk = kChunk;
            this.sourceContexts = sourceContexts;
            this.kernelContext = kernelContext;
        }

        @Override
        public void close() {
            kernelContext.close();
            for (final ColumnSource.GetContext sc : sourceContexts) {
                if (sc != null) {
                    sc.close();
                }
            }
            if (kChunk != null) {
                kChunk.close();
            }
            if (iiChunk != null) {
                iiChunk.close();
            }
            if (iChunk != null) {
                iChunk.close();
            }
        }
    }

    private interface GetHandler {
        Object apply(long key, boolean usePrev);
    }
}
