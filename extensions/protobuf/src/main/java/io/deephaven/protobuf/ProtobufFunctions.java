//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.protobuf;

import com.google.protobuf.Message;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.function.TypedFunction;
import org.immutables.value.Value.Immutable;

import java.util.List;

/**
 * A list of {@link #functions()}.
 */
@Immutable
@BuildableStyle
public abstract class ProtobufFunctions {

    private static final ProtobufFunctions EMPTY = builder().build();

    public static Builder builder() {
        return ImmutableProtobufFunctions.builder();
    }

    /**
     * Creates an empty protobuf functions. Equivalent to {@code builder().build()}.
     *
     * @return the empty protobuf functions
     */
    public static ProtobufFunctions empty() {
        return EMPTY;
    }

    /**
     * Creates a protobuf functions with a single, unnamed {@code function}. Equivalent to
     * {@code builder().addFunctions(ProtobufFunction.unnammed(function)).build()}.
     *
     * @param function the function
     * @return the protobuf functions
     */
    public static ProtobufFunctions unnamed(TypedFunction<Message> function) {
        return builder().addFunctions(ProtobufFunction.unnamed(function)).build();
    }

    /**
     * Creates a protobuf fuctions with {@code functions}. Equivalent to
     * {@code builder().addFunctions(functions).build()}.
     *
     * @param functions the functions
     * @return the protobuf functions
     */
    public static ProtobufFunctions of(ProtobufFunction... functions) {
        return builder().addFunctions(functions).build();
    }

    /**
     * The protobuf functions.
     *
     * @return the functions
     */
    public abstract List<ProtobufFunction> functions();

    public interface Builder {
        Builder addFunctions(ProtobufFunction element);

        Builder addFunctions(ProtobufFunction... elements);

        Builder addAllFunctions(Iterable<? extends ProtobufFunction> elements);

        ProtobufFunctions build();
    }
}
