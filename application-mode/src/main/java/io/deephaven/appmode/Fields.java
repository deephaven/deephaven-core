package io.deephaven.appmode;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Spliterator;
import java.util.function.Consumer;

@Immutable
@BuildableStyle
public abstract class Fields implements Iterable<Field<?>> {

    public static Builder builder() {
        return ImmutableFields.builder();
    }

    public static Fields of(Field<?>... fields) {
        return builder().addFields(fields).build();
    }

    public static Fields of(Collection<Field<?>> fields) {
        return builder().addAllFields(fields).build();
    }

    abstract Map<String, Field<?>> fields();

    public final int size() {
        return fields().size();
    }

    @Override
    public final Iterator<Field<?>> iterator() {
        return fields().values().iterator();
    }

    @Override
    public final void forEach(Consumer<? super Field<?>> action) {
        fields().values().forEach(action);
    }

    @Override
    public final Spliterator<Field<?>> spliterator() {
        return fields().values().spliterator();
    }

    public interface Builder {

        Builder putFields(String key, Field<?> value);

        default Builder addFields(Field<?> field) {
            return putFields(field.name(), field);
        }

        default Builder addFields(Field<?>... fields) {
            for (Field<?> field : fields) {
                addFields(field);
            }
            return this;
        }

        default Builder addAllFields(Collection<Field<?>> fields) {
            for (Field<?> field : fields) {
                addFields(field);
            }
            return this;
        }

        Fields build();
    }

    @Check
    final void checkKeys() {
        for (Entry<String, Field<?>> e : fields().entrySet()) {
            if (!e.getKey().equals(e.getValue().name())) {
                throw new IllegalArgumentException("field name must be used as the key");
            }
        }
    }
}
