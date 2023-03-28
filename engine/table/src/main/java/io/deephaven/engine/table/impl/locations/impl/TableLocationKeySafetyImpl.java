package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

final class TableLocationKeySafetyImpl<TLK extends TableLocationKey> implements TableLocationKeyFinder<TLK> {
    private final TableLocationKeyFinder<TLK> finder;
    private final Set<TLK> tlks;
    private Set<String> partitionsKeys;

    public TableLocationKeySafetyImpl(TableLocationKeyFinder<TLK> finder) {
        this.finder = Objects.requireNonNull(finder);
        this.tlks = new HashSet<>();
    }

    @Override
    public synchronized void findKeys(@NotNull Consumer<TLK> locationKeyObserver) {
        final Checked checked = new Checked(locationKeyObserver);
        finder.findKeys(checked);
        checked.finished();
    }

    private class Checked implements Consumer<TLK> {
        private final Consumer<TLK> consumer;
        private final Set<TLK> seen;

        public Checked(Consumer<TLK> consumer) {
            this.consumer = Objects.requireNonNull(consumer);
            this.seen = new HashSet<>();
        }

        @Override
        public void accept(TLK tlk) {
            if (partitionsKeys == null) {
                partitionsKeys = new LinkedHashSet<>(tlk.getPartitionKeys());
            } else if (!partitionsKeys.equals(tlk.getPartitionKeys())) {
                throw new TableDataException(String
                        .format("TableLocationKeyFinder %s has produced an inconsistent TableLocationKey with unexpected partition keys. expected=%s actual=%s.",
                                finder, partitionsKeys, tlk.getPartitionKeys()));
            }
            if (!seen.add(tlk)) {
                throw new TableDataException(String.format(
                        "TableLocationKeyFinder %s has produced a duplicate TableLocationKey %s.", finder, tlk));
            }
            consumer.accept(tlk);
        }

        public void finished() {
            for (TLK tlk : tlks) {
                if (!seen.contains(tlk)) {
                    throw new TableDataException(
                            String.format(
                                    "TableLocationKeyFinder %s has removed a previously seen TableLocationKey %s.",
                                    finder, tlk));
                }
            }
            tlks.addAll(seen);
        }
    }
}
