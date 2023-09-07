/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import com.google.protobuf.Message;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.functions.TypedFunction;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

/**
 * Encapsulates the logic to extract a result from a {@link Message}.
 */
@Immutable
@SimpleStyle
public abstract class ProtobufFunction {

    /**
     * Creates the unnamed protobuf function. Equivalent to {@code of(FieldPath.empty(), f)}.
     *
     * @param f the function
     * @return the unnamed protobuf function
     */
    public static ProtobufFunction unnamed(TypedFunction<Message> f) {
        return of(FieldPath.empty(), f);
    }

    /**
     * Creates the protobuf function.
     *
     * @param path the field path
     * @param f the function
     * @return the protobuf function
     */
    public static ProtobufFunction of(FieldPath path, TypedFunction<Message> f) {
        return ImmutableProtobufFunction.of(path, f);
    }

    /**
     * The path that {@link #function()} uses to produce its result.
     *
     * @return the path
     */
    @Parameter
    public abstract FieldPath path();

    /**
     * The function to extract a result from a {@link Message}.
     *
     * @return the function
     */
    @Parameter
    public abstract TypedFunction<Message> function();
}
