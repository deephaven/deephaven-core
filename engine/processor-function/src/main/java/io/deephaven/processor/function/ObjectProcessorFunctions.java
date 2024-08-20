//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor.function;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.function.TypedFunction;
import io.deephaven.processor.ObjectProcessor;

import java.util.List;

public final class ObjectProcessorFunctions {

    /**
     * Creates a function-based processor whose {@link ObjectProcessor#outputTypes()} is the
     * {@link TypedFunction#returnType()} from each function in {@code functions}.
     *
     * <p>
     * The implementation of {@link ObjectProcessor#processAll(ObjectChunk, List)} is column-oriented with a virtual
     * call and cast per-column.
     *
     * @param functions the functions
     * @return the function-based processor
     * @param <T> the object type
     */
    public static <T> ObjectProcessor<T> of(List<TypedFunction<? super T>> functions) {
        return ObjectProcessorFunctionsImpl.create(functions);
    }
}
