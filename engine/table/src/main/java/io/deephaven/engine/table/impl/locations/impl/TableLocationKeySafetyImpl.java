package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationKey;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

final class TableLocationKeySafetyImpl<TLK extends TableLocationKey> implements TableLocationKeyFinder<TLK> {
    private final TableLocationKeyFinder<TLK> finder;
    private final boolean verifyNoneRemoved;

    private final Set<TLK> tlks;
    private List<String> partitionsKeys;

    public TableLocationKeySafetyImpl(TableLocationKeyFinder<TLK> finder, boolean verifyNoneRemoved) {
        this.finder = Objects.requireNonNull(finder);
        this.verifyNoneRemoved = verifyNoneRemoved;
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
                partitionsKeys = new ArrayList<>(tlk.getPartitionKeys());
            } else if (!TableLocationKeySafetyImpl.equals(partitionsKeys, tlk.getPartitionKeys())) {
                throw new TableDataException(String.format(
                        "TableLocationKeyFinder %s has produced an inconsistent TableLocationKey with unexpected partition keys. expected=%s actual=%s.",
                        finder, partitionsKeys, tlk.getPartitionKeys()));
            }
            if (!seen.add(tlk)) {
                throw new TableDataException(String.format(
                        "TableLocationKeyFinder %s has produced a duplicate TableLocationKey %s.", finder, tlk));
            }
            consumer.accept(tlk);
        }

        public void finished() {
            if (verifyNoneRemoved) {
                for (TLK tlk : tlks) {
                    if (!seen.contains(tlk)) {
                        throw new TableDataException(String.format(
                                "TableLocationKeyFinder %s has removed a previously seen TableLocationKey %s.", finder,
                                tlk));
                    }
                }
            }
            tlks.addAll(seen);
        }
    }

    private static <T> boolean equals(Collection<T> c1, Collection<T> c2) {
        final Iterator<T> i2 = c2.iterator();
        for (T t1 : c1) {
            if (!i2.hasNext()) {
                return false;
            }
            if (!Objects.equals(t1, i2.next())) {
                return false;
            }
        }
        return !i2.hasNext();
    }
}
