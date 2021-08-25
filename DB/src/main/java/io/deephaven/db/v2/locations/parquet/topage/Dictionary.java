package io.deephaven.db.v2.locations.parquet.topage;

import io.deephaven.db.v2.sources.StringSetImpl;
import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.ObjectChunk;
import gnu.trove.map.TObjectIntMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.util.function.IntFunction;

public class Dictionary<T, ATTR extends Attributes.Any>
    implements StringSetImpl.ReversibleLookup<T> {

    private final ObjectChunk<T, ATTR> objects;
    private volatile TObjectIntMap<T> reverseMap = null;

    Dictionary(IntFunction<T> sourceFunction, int length) {
        final T[] objects = ObjectChunk.makeArray(length);

        for (int i = 0; i < length; ++i) {
            objects[i] = sourceFunction.apply(i);
        }

        this.objects = ObjectChunk.chunkWrap(objects);
    }

    ObjectChunk<T, ATTR> getChunk() {
        return objects;
    }

    public final T get(long index) {
        return index < 0 || index >= objects.size() ? null : objects.get((int) index);
    }

    public final int rget(int highestIndex, T value) {

        TObjectIntMap<T> localReverseMap = reverseMap;

        if (localReverseMap == null) {
            synchronized (this) {
                localReverseMap = reverseMap;
                if (localReverseMap == null) {
                    localReverseMap = new TObjectIntHashMap<>(objects.size());

                    for (int i = 0; i < objects.size(); ++i) {
                        localReverseMap.put(objects.get(i), i);
                    }

                    reverseMap = localReverseMap;
                }
            }
        }

        return localReverseMap.get(value);
    }

    public final int length() {
        return objects.size();
    }
}
