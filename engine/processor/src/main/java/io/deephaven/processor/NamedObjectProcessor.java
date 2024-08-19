//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.qst.type.Type;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Immutable
@BuildableStyle
public abstract class NamedObjectProcessor<T> {

    public static <T> Builder<T> builder() {
        return ImmutableNamedObjectProcessor.builder();
    }

    public static <T> NamedObjectProcessor<T> of(ObjectProcessor<? super T> processor, String... names) {
        return NamedObjectProcessor.<T>builder().processor(processor).addNames(names).build();
    }

    public static <T> NamedObjectProcessor<T> of(ObjectProcessor<? super T> processor, Iterable<String> names) {
        return NamedObjectProcessor.<T>builder().processor(processor).addAllNames(names).build();
    }

    /**
     * The name for each output of {@link #processor()}.
     */
    public abstract List<String> names();

    /**
     * The object processor.
     */
    public abstract ObjectProcessor<? super T> processor();

    public interface Builder<T> {
        Builder<T> processor(ObjectProcessor<? super T> processor);

        Builder<T> addNames(String element);

        Builder<T> addNames(String... elements);

        Builder<T> addAllNames(Iterable<String> elements);

        NamedObjectProcessor<T> build();
    }

    public interface Provider extends ObjectProcessor.Provider {

        /**
         * The name for each output of the processors. Equivalent to the named processors'
         * {@link NamedObjectProcessor#names()}.
         *
         * @return the names
         */
        List<String> names();

        /**
         * Creates a named object processor that can process the {@code inputType}. This will successfully create a
         * named processor when {@code inputType} is one of, or extends from one of, {@link #inputTypes()}. Otherwise,
         * an {@link IllegalArgumentException} will be thrown. Equivalent to
         * {@code NamedObjectProcessor.of(processor(inputType), names())}.
         *
         * @param inputType the input type
         * @return the object processor
         * @param <T> the input type
         */
        default <T> NamedObjectProcessor<? super T> named(Type<T> inputType) {
            return NamedObjectProcessor.of(processor(inputType), names());
        }
    }

    @Check
    final void checkSizes() {
        if (names().size() != processor().outputSize()) {
            throw new IllegalArgumentException(
                    String.format("Unmatched sizes; names().size()=%d, processor().outputSize()=%d",
                            names().size(), processor().outputSize()));
        }
    }
}
