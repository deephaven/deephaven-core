package io.deephaven.chunk.util.pools;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Static callback holder for pooled chunk allocation.
 */
public class ChunkPoolInstrumentation {

    private static Function<Supplier<?>, ?> allocationRecorder = Supplier::get;

    /**
     * Set an allocation recorder for this process' {@link ChunkPool chunk pools}.
     *
     * @param allocationRecorder The new allocation recorder, or {@code null} to install a default no-op recorder
     */
    public static void setAllocationRecorder(@Nullable final Function<Supplier<?>, ?> allocationRecorder) {
        ChunkPoolInstrumentation.allocationRecorder = allocationRecorder == null ? Supplier::get : allocationRecorder;
    }

    /**
     * Return the result of {@code allocationProcedure}, run by the currently installed {@code allocationRecorder}.
     *
     * @param allocationProcedure The allocation procedure to {@link Supplier#get()}
     * @return The result of {@code allocationProcedure}
     */
    public static <RETURN_TYPE> RETURN_TYPE getAndRecord(@NotNull final Supplier<RETURN_TYPE> allocationProcedure) {
        // noinspection unchecked
        return (RETURN_TYPE) allocationRecorder.apply(allocationProcedure);
    }
}
