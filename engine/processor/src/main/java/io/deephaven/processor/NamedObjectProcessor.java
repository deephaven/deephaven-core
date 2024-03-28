//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.processor;

import io.deephaven.annotations.BuildableStyle;
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

    public static <T> NamedObjectProcessor<T> of(ObjectProcessor<T> processor, String... names) {
        return NamedObjectProcessor.<T>builder().processor(processor).addColumnNames(names).build();
    }

    public static <T> NamedObjectProcessor<T> of(ObjectProcessor<T> processor, Iterable<String> names) {
        return NamedObjectProcessor.<T>builder().processor(processor).addAllColumnNames(names).build();
    }

    public static <T> NamedObjectProcessor<T> prefix(ObjectProcessor<T> processor, String prefix) {
        final int size = processor.size();
        if (size == 1) {
            return of(processor, prefix);
        }
        return of(processor, IntStream.range(0, size).mapToObj(ix -> prefix + "_" + ix).collect(Collectors.toList()));
    }

    public abstract ObjectProcessor<T> processor();

    public abstract List<String> columnNames();

    public interface Builder<T> {
        Builder<T> processor(ObjectProcessor<T> processor);

        Builder<T> addColumnNames(String element);

        Builder<T> addColumnNames(String... elements);

        Builder<T> addAllColumnNames(Iterable<String> elements);

        NamedObjectProcessor<T> build();
    }


    public interface Provider {

        /**
         * Creates a named object processor that can process the input type {@code inputType}.
         *
         * @param inputType the input type
         * @return the named object processor
         * @param <T> the input type
         */
        <T> NamedObjectProcessor<? super T> named(Class<T> inputType);
    }

    @Check
    final void checkSizes() {
        if (columnNames().size() != processor().size()) {
            throw new IllegalArgumentException(
                    String.format("Unmatched sizes; columnNames().size()=%d, processor().size()=%d",
                            columnNames().size(), processor().size()));
        }
    }
}
