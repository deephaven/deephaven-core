package io.deephaven.engine.table.impl;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.Context;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

public class ContextWithChunk<ATTR extends Any, CONTEXT extends Context> implements Context {

    private final CONTEXT context;
    private WritableChunk<ATTR> writableChunk;
    private final ResettableWritableChunk<ATTR> resettableWritableChunk;

    ContextWithChunk(CONTEXT context, ChunkType chunkType, int chunkCapacity) {
        this.context = context;
        writableChunk = chunkType.makeWritableChunk(chunkCapacity);
        writableChunk.setSize(chunkCapacity);
        resettableWritableChunk = chunkType.makeResettableWritableChunk();
    }

    @Override
    public void close() {
        context.close();
        writableChunk.close();
        resettableWritableChunk.close();
    }

    /**
     * @return a {@link WritableChunk} which you can use for results
     *
     * @apiNote the chunk is valid until the next call to this function, {@link #getResettableChunk()},
     *          {@link #getWritableChunk(Context)}, {@link #getResettableChunk(Context)}, or
     *          {@link #resetChunkFromArray(Context, Object, int, int)} for this context.
     */
    public WritableChunk<ATTR> getWritableChunk() {
        return resettableWritableChunk.resetFromChunk(writableChunk, 0, writableChunk.size());
    }

    /**
     * @return a {@link ResettableChunk} chunk which you can use for results by calling one of its various reset
     *         methods.
     *
     * @apiNote the chunk is valid until the next call to this function, {@link #getWritableChunk()},
     *          {@link #getWritableChunk(Context)}, {@link #getResettableChunk(Context)}, or
     *          {@link #resetChunkFromArray(Context, Object, int, int)} for this context.
     */
    public ResettableChunk<ATTR> getResettableChunk() {
        return resettableWritableChunk;
    }

    /**
     * @return The context held in this Context
     */
    public CONTEXT getContext() {
        return context;
    }

    /**
     * @return The context held in this Context
     */
    public static <CONTEXT extends Context> CONTEXT getContext(@NotNull Context context) {
        // noinspection unchecked
        return (CONTEXT) ((ContextWithChunk) context).context;
    }

    /**
     * Makes sure that the internal array (and hence the writableChunk) is at least specified size.
     */
    public void ensureLength(final int length) {
        if (writableChunk.size() < length) {
            if (writableChunk.capacity() < length) {
                final SafeCloseable oldWritableChunk = writableChunk;
                writableChunk = writableChunk.getChunkType().makeWritableChunk(length);
                oldWritableChunk.close();
            }
            writableChunk.setSize(length);
        }
    }

    /**
     * @param context The context that owns the reusable chunk
     *
     * @return a {@link WritableChunk} which you can use for results. The size will be set to 0.
     *
     * @apiNote the chunk is valid until the next call to this function, {@link #getWritableChunk()},
     *          {@link #getResettableChunk()}, {@link #getResettableChunk(Context)}, or
     *          {@link #resetChunkFromArray(Context, Object, int, int)} for this context.
     */
    public static <ATTR extends Any, WRITABLE_CHUNK extends WritableChunk<ATTR>> WRITABLE_CHUNK getWritableChunk(
            @NotNull Context context) {
        // noinspection unchecked
        WRITABLE_CHUNK writableChunk = (WRITABLE_CHUNK) ((ContextWithChunk<ATTR, ?>) context).getWritableChunk();
        writableChunk.setSize(0);
        return writableChunk;
    }

    /**
     * @param context The context that owns the reusable chunk
     *
     * @return a {@link ResettableWritableChunk}, which you can use for results by using one of its various reset
     *         methods.
     *
     * @apiNote the chunk is valid until the next call to this function, {@link #getWritableChunk()},
     *          {@link #getResettableChunk()}, {@link #getWritableChunk(Context)},
     *          {@link #resetChunkFromArray(Context, Object, int, int)} for this context.
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public static <ATTR extends Any, RESETTABLE_WRITABLE_CHUNK extends ResettableWritableChunk<ATTR>> RESETTABLE_WRITABLE_CHUNK getResettableChunk(
            @NotNull Context context) {
        // noinspection unchecked
        return (RESETTABLE_WRITABLE_CHUNK) ((ContextWithChunk<ATTR, ?>) context).getResettableChunk();
    }

    /**
     * @param context The context that owns the reusable chunk
     * @param array The array to alias. If this is null, returns a null-value filled chunk.
     * @param offset The offset in the array for the beginning of the chunk
     * @param length The length of the chunk
     *
     * @return A chunk which aliases the region of the array which can be used for results.
     *
     * @apiNote the chunk is valid until the next call to this function, {@link #getWritableChunk()},
     *          {@link #getResettableChunk()}, {@link #getWritableChunk(Context)}, or
     *          {@link #getResettableChunk(Context)} for this context.
     */
    public static <ATTR extends Any, CHUNK extends Chunk<ATTR>> CHUNK resetChunkFromArray(
            @NotNull Context context, Object array, int offset, int length) {
        // noinspection unchecked
        ContextWithChunk<ATTR, ?> getContext = (ContextWithChunk<ATTR, ?>) context;

        if (array == null) {
            WritableChunk<ATTR> writableChunk = getContext.getWritableChunk();
            writableChunk.setSize(length);
            writableChunk.fillWithNullValue(0, length);

            // noinspection unchecked
            return (CHUNK) writableChunk;
        } else {
            // noinspection unchecked
            return (CHUNK) getContext.getResettableChunk().resetFromArray(array, offset, length);
        }
    }


    /**
     * Checks if this chunk is the result of a call to {@link #getWritableChunk()} or {@link #getWritableChunk(Context)}
     * with this context. This is primarily intended for testing and verification code.
     */
    public static <ATTR extends Any> boolean isMyWritableChunk(@NotNull Context context, Chunk<ATTR> chunk) {
        // noinspection unchecked
        return chunk.isAlias(((ContextWithChunk<ATTR, ?>) context).writableChunk);
    }

    /**
     * Checks if this chunk is the result of a call to {@link #getResettableChunk()} or
     * {@link #getResettableChunk(Context)} with this context, followed by a some reset call, including the result of a
     * call to {@link #resetChunkFromArray(Context, Object, int, int)}. This is primarily intended for testing and
     * verification code.
     */
    public static <ATTR extends Any> boolean isMyResettableChunk(@NotNull Context context,
            Chunk<ATTR> chunk) {
        // noinspection unchecked
        ContextWithChunk<ATTR, ?> getContext = (ContextWithChunk<ATTR, ?>) context;
        return !chunk.isAlias(getContext.writableChunk) && chunk.isAlias(getContext.resettableWritableChunk);
    }
}
