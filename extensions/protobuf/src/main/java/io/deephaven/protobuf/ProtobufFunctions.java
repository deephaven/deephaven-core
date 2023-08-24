/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import com.google.protobuf.Message;
import io.deephaven.annotations.BuildableStyle;
import io.deephaven.functions.TypedFunction;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.Optional;

/**
 * A list of {@link #functions()}.
 */
@Immutable
@BuildableStyle
public abstract class ProtobufFunctions {

    public static Builder builder() {
        return ImmutableProtobufFunctions.builder();
    }

    public static ProtobufFunctions empty() {
        return builder().build();
    }

    public static ProtobufFunctions unnamed(TypedFunction<Message> tf) {
        return of(ProtobufFunction.of(tf));
    }

    public static ProtobufFunctions of(ProtobufFunction... functions) {
        return builder().addFunctions(functions).build();
    }

    public abstract List<ProtobufFunction> functions();

    public final Optional<ProtobufFunction> find(List<String> namePath) {
        for (ProtobufFunction function : functions()) {
            if (namePath.equals(function.path().namePath())) {
                return Optional.of(function);
            }
        }
        return Optional.empty();
    }

    public final Optional<ProtobufFunction> find(FieldNumberPath numberPath) {
        for (ProtobufFunction function : functions()) {
            if (numberPath.equals(function.path().numberPath())) {
                return Optional.of(function);
            }
        }
        return Optional.empty();
    }

    public interface Builder {
        Builder addFunctions(ProtobufFunction element);

        Builder addFunctions(ProtobufFunction... elements);

        Builder addAllFunctions(Iterable<? extends ProtobufFunction> elements);

        ProtobufFunctions build();
    }
}
