package io.deephaven.db.v2.select.formula;

import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

public class FormulaKernelAdapter extends io.deephaven.db.v2.select.Formula {
    private final FormulaSourceDescriptor sourceDescriptor;
    private final Map<String, ? extends ColumnSource> columnSources;
    private final FormulaKernel kernel;
    private final ChunkType chunkType;
    private final GetHandler getHandler;

    public FormulaKernelAdapter(final Index index, final FormulaSourceDescriptor sourceDescriptor,
        final Map<String, ? extends ColumnSource> columnSources,
        final FormulaKernel kernel) {
        super(index);
        this.sourceDescriptor = sourceDescriptor;
        this.columnSources = columnSources;
        this.kernel = kernel;
        Class rt = sourceDescriptor.returnType;
        if (rt != Boolean.class && io.deephaven.util.type.TypeUtils.isBoxedType(rt)) {
            rt = io.deephaven.util.type.TypeUtils.getUnboxedType(rt);
        }
        this.chunkType = ChunkType.fromElementType(rt);
        if (rt == byte.class) {
            getHandler =
                (key, prev) -> io.deephaven.util.type.TypeUtils.box(handleGetByte(key, prev));
        } else if (rt == Boolean.class) {
            getHandler = this::handleGetBoolean;
        } else if (rt == char.class) {
            getHandler =
                (key, prev) -> io.deephaven.util.type.TypeUtils.box(handleGetChar(key, prev));
        } else if (rt == double.class) {
            getHandler =
                (key, prev) -> io.deephaven.util.type.TypeUtils.box(handleGetDouble(key, prev));
        } else if (rt == float.class) {
            getHandler =
                (key, prev) -> io.deephaven.util.type.TypeUtils.box(handleGetFloat(key, prev));
        } else if (rt == int.class) {
            getHandler =
                (key, prev) -> io.deephaven.util.type.TypeUtils.box(handleGetInt(key, prev));
        } else if (rt == long.class) {
            getHandler =
                (key, prev) -> io.deephaven.util.type.TypeUtils.box(handleGetLong(key, prev));
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
        try (final WritableObjectChunk<Object, Attributes.Values> __dest =
            WritableObjectChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private Boolean handleGetBoolean(final long k, boolean usePrev) {
        try (final WritableObjectChunk<Boolean, Attributes.Values> __dest =
            WritableObjectChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private byte handleGetByte(final long k, boolean usePrev) {
        try (final WritableByteChunk<Attributes.Values> __dest =
            WritableByteChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private char handleGetChar(final long k, boolean usePrev) {
        try (final WritableCharChunk<Attributes.Values> __dest =
            WritableCharChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private double handleGetDouble(final long k, boolean usePrev) {
        try (final WritableDoubleChunk<Attributes.Values> __dest =
            WritableDoubleChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private float handleGetFloat(final long k, boolean usePrev) {
        try (final WritableFloatChunk<Attributes.Values> __dest =
            WritableFloatChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private int handleGetInt(final long k, boolean usePrev) {
        try (final WritableIntChunk<Attributes.Values> __dest =
            WritableIntChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private long handleGetLong(final long k, boolean usePrev) {
        try (final WritableLongChunk<Attributes.Values> __dest =
            WritableLongChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private short handleGetShort(final long k, boolean usePrev) {
        try (final WritableShortChunk<Attributes.Values> __dest =
            WritableShortChunk.makeWritableChunk(1)) {
            commonGetLogic(__dest, k, usePrev);
            return __dest.get(0);
        }
    }

    private void commonGetLogic(WritableChunk<Values> __dest, final long k, boolean usePrev) {
        try (final Index ok = Index.FACTORY.getIndexByValues(k)) {
            try (final AdapterContext context = makeFillContext(1)) {
                fillChunkHelper(context, __dest, ok, usePrev, true);
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
        @NotNull final OrderedKeys __orderedKeys) {
        fillChunkHelper(__context, __destination, __orderedKeys, false, true);
    }

    @Override
    public void fillPrevChunk(@NotNull final FillContext __context,
        @NotNull final WritableChunk<? super Values> __destination,
        @NotNull final OrderedKeys __orderedKeys) {
        fillChunkHelper(__context, __destination, __orderedKeys, true, true);
    }

    private void fillChunkHelper(@NotNull final FillContext __context,
        @NotNull final WritableChunk<? super Values> __destination,
        @NotNull final OrderedKeys __orderedKeys, final boolean usePrev, final boolean lookupI) {
        final int orderedKeysSize = __orderedKeys.intSize();
        __destination.setSize(orderedKeysSize);
        // Shortcut if __orderedKeys is empty
        if (orderedKeysSize == 0) {
            return;
        }
        final AdapterContext __typedContext = (AdapterContext) __context;
        final Chunk<? extends Attributes.Values>[] sourceChunks =
            new Chunk[sourceDescriptor.sources.length];
        try (final OrderedKeys flat = Index.FACTORY.getFlatIndex(__orderedKeys.size())) {
            for (int ii = 0; ii < sourceDescriptor.sources.length; ++ii) {
                final String name = sourceDescriptor.sources[ii];
                switch (name) {
                    case "i": {
                        if (lookupI) {
                            // Potentially repeated work w.r.t. "ii".
                            __typedContext.iChunk.setSize(0);
                            __index.invert(__orderedKeys.asIndex()).forAllLongs(longVal -> {
                                final int i = LongSizedDataStructure
                                    .intSize("FormulaNubbin i usage", longVal);
                                __typedContext.iChunk.add(i);
                            });
                        } else {
                            // sequential i
                            for (int pos = 0; pos < orderedKeysSize; ++pos) {
                                __typedContext.iChunk.set(pos, pos);
                            }
                            __typedContext.iChunk.setSize(orderedKeysSize);
                        }
                        sourceChunks[ii] = __typedContext.iChunk;
                        break;
                    }
                    case "ii": {
                        if (lookupI) {
                            __index.invert(__orderedKeys.asIndex())
                                .fillKeyIndicesChunk(__typedContext.iiChunk);
                        } else {
                            // sequential ii
                            for (int pos = 0; pos < orderedKeysSize; ++pos) {
                                __typedContext.iiChunk.set(pos, pos);
                            }
                            __typedContext.iiChunk.setSize(orderedKeysSize);
                        }
                        sourceChunks[ii] = __typedContext.iiChunk;
                        break;
                    }
                    case "k": {
                        __orderedKeys.fillKeyIndicesChunk(__typedContext.kChunk);
                        sourceChunks[ii] = __typedContext.kChunk;
                        break;
                    }
                    default: {
                        final ColumnSource cs = columnSources.get(name);
                        final ColumnSource.GetContext ctx = __typedContext.sourceContexts[ii];
                        sourceChunks[ii] = usePrev ? cs.getPrevChunk(ctx, __orderedKeys)
                            : cs.getChunk(ctx, __orderedKeys);
                    }
                }
            }
        }
        kernel.applyFormulaChunk(__typedContext.kernelContext, __destination, sourceChunks);
    }

    @Override
    public AdapterContext makeFillContext(final int chunkCapacity) {
        WritableIntChunk<OrderedKeyIndices> iChunk = null;
        WritableLongChunk<OrderedKeyIndices> iiChunk = null;
        WritableLongChunk<OrderedKeyIndices> kChunk = null;

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
        final ColumnSource.GetContext[] sourceContexts =
            new ColumnSource.GetContext[sources.length];
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
        final WritableIntChunk<OrderedKeyIndices> iChunk;
        final WritableLongChunk<OrderedKeyIndices> iiChunk;
        final WritableLongChunk<OrderedKeyIndices> kChunk;
        final ColumnSource.GetContext[] sourceContexts;
        final FillContext kernelContext;

        AdapterContext(WritableIntChunk<OrderedKeyIndices> iChunk,
            WritableLongChunk<OrderedKeyIndices> iiChunk,
            WritableLongChunk<OrderedKeyIndices> kChunk,
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
