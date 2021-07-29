package io.deephaven.qst.table;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Labeled tables is a list of {@link LabeledTable labeled tables}. Each label will be distinct.
 */
@Immutable
@BuildableStyle
public abstract class LabeledTables implements Iterable<LabeledTable> {

    public interface Builder {

        Builder putMap(String key, TableSpec value);

        default Builder addTables(LabeledTable element) {
            return putMap(element.label(), element.table());
        }

        default Builder addTables(LabeledTable... elements) {
            for (LabeledTable element : elements) {
                addTables(element);
            }
            return this;
        }

        default Builder addAllTables(Iterable<LabeledTable> elements) {
            for (LabeledTable element : elements) {
                addTables(element);
            }
            return this;
        }

        LabeledTables build();
    }

    public static Builder builder() {
        return ImmutableLabeledTables.builder();
    }

    public static LabeledTables of(LabeledTable... tables) {
        return builder().addTables(tables).build();
    }

    abstract Map<String, TableSpec> map();

    public final Collection<TableSpec> tables() {
        return map().values();
    }

    public final TableSpec getTable(String name) {
        return map().get(name);
    }

    public final Stream<LabeledTable> stream() {
        return StreamSupport.stream(spliterator(), false);
    }

    @Override
    public final Iterator<LabeledTable> iterator() {
        return new LabeledTableIterator(map().entrySet().iterator());
    }

    @Override
    public final void forEach(Consumer<? super LabeledTable> action) {
        for (Entry<String, TableSpec> e : map().entrySet()) {
            action.accept(adapt(e));
        }
    }

    @Override
    public final Spliterator<LabeledTable> spliterator() {
        return map().entrySet().stream().map(LabeledTables::adapt).spliterator();
    }

    private static LabeledTable adapt(Entry<String, TableSpec> e) {
        return LabeledTable.of(e.getKey(), e.getValue());
    }

    private static class LabeledTableIterator implements Iterator<LabeledTable> {
        private final Iterator<Entry<String, TableSpec>> it;

        LabeledTableIterator(Iterator<Entry<String, TableSpec>> it) {
            this.it = Objects.requireNonNull(it);
        }

        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public LabeledTable next() {
            return adapt(it.next());
        }
    }
}
