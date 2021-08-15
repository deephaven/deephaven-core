package io.deephaven.qst;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
@BuildableStyle
public abstract class LabeledValues<T> implements Iterable<LabeledValue<T>> {

    public static <T> Builder<T> builder() {
        return ImmutableLabeledValues.builder();
    }

    public static <T> LabeledValues<T> of(Iterable<String> labels, Iterable<T> values) {
        Builder<T> builder = builder();
        Iterator<String> it1 = labels.iterator();
        Iterator<T> it2 = values.iterator();
        while (it1.hasNext() && it2.hasNext()) {
            builder.add(it1.next(), it2.next());
        }
        if (it1.hasNext() || it2.hasNext()) {
            throw new IllegalStateException();
        }
        return builder.build();
    }

    public static <T> LabeledValues<T> merge(LabeledValues<T>... labeledValues) {
        return merge(Arrays.asList(labeledValues));
    }

    public static <T> LabeledValues<T> merge(Iterable<LabeledValues<T>> labeledValues) {
        Builder<T> builder = builder();
        for (LabeledValues<T> value : labeledValues) {
            for (LabeledValue<T> v : value) {
                builder.add(v);
            }
        }
        return builder.build();
    }

    abstract Map<String, LabeledValue<T>> map();

    public final T get(String name) {
        return map().get(name).value();
    }

    public final Collection<String> labels() {
        return map().keySet();
    }

    public final Iterable<T> values() {
        return () -> map().values().stream().map(LabeledValue::value).iterator();
    }

    public final Stream<T> valuesStream() {
        return map().values().stream().map(LabeledValue::value);
    }

    public final Set<T> valueSet() {
        return map().values().stream().map(LabeledValue::value).collect(Collectors.toSet());
    }

    @Override
    public final Iterator<LabeledValue<T>> iterator() {
        return map().values().iterator();
    }

    @Override
    public final void forEach(Consumer<? super LabeledValue<T>> action) {
        map().values().forEach(action);
    }

    @Override
    public Spliterator<LabeledValue<T>> spliterator() {
        return map().values().spliterator();
    }

    @Check
    final void checkKeys() {
        for (Entry<String, LabeledValue<T>> e : map().entrySet()) {
            if (!e.getKey().equals(e.getValue().name())) {
                throw new IllegalArgumentException("Keys must be equal to the value names");
            }
        }
    }

    public interface Builder<T> {

        default Builder<T> add(LabeledValue<T> value) {
            return putMap(value.name(), value);
        }

        default Builder<T> add(String name, T value) {
            return putMap(name, LabeledValue.of(name, value));
        }

        default Builder<T> addPrefixed(String prefix, LabeledValues<T> values) {
            for (LabeledValue<T> value : values) {
                add(prefix + value.name(), value.value());
            }
            return this;
        }

        Builder<T> putMap(String key, LabeledValue<T> value);

        LabeledValues<T> build();
    }
}
