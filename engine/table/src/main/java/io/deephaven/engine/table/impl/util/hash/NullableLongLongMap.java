//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.util.hash;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import it.unimi.dsi.fastutil.longs.LongLongBiConsumer;

/**
 * The interface we use for our Long2LongMaps that are the basis for a hashed redirection index.
 */
public interface NullableLongLongMap {
    /**
     * Empty the map and release its backing array. The next put allocates a new array at the initial capacity.
     */
    void resetToNull();

    /**
     * Empty the map and release its backing array, as {@link #resetToNull()} does, but remember the capacity the map
     * had reached so that the next allocation is made at that size instead of regrowing from the initial capacity
     * through successive rehashes. Like {@link #resetToNull()} and unlike {@link #clear()}, this never writes into the
     * array a concurrent reader might be probing, so it is safe to call while unsynchronized readers are active.
     */
    void resetToNullRetainingCapacity();

    int capacity();

    /**
     * @return the size of this map
     */
    int size();

    /**
     * @return true if this map is empty (i.e. size is zero)
     */
    boolean isEmpty();

    /**
     * @return the value returned from {@link #get(LongChunk, WritableLongChunk)} when no value is present in the map.
     */
    long defaultReturnValue();

    /**
     * For each element ii of {@code keys}: adds a mapping from {@code keys.get(ii)} to {@code values.get(ii)}, writing
     * the previous value of that key (or {@link #defaultReturnValue()} if there was no mapping) to element ii of
     * {@code oldValues}. Elements are processed in index order. {@code values} must have at least {@code keys.size()}
     * elements. On return, the size of {@code oldValues} is set to {@code keys.size()}; its capacity must be at least
     * that large.
     *
     * @param keys the keys to add
     * @param values the values to add
     * @param oldValues output: the previous value of each key (or {@link #defaultReturnValue()})
     */
    void put(LongChunk<? extends Any> keys, LongChunk<? extends Any> values,
            WritableLongChunk<? extends Any> oldValues);

    /**
     * For each element ii of {@code keys}: adds a mapping from {@code keys.get(ii)} to {@code values.get(ii)} if no
     * mapping for that key already exists, writing the previous value (or {@link #defaultReturnValue()} if there was
     * none) to element ii of {@code oldValues}. Elements are processed in index order; a duplicate key within
     * {@code keys} therefore sees the value established by its own earlier element. Size contracts are as for
     * {@link #put(LongChunk, LongChunk, WritableLongChunk)}.
     *
     * @param keys the keys to add
     * @param values the values to add
     * @param oldValues output: the previous value of each key (or {@link #defaultReturnValue()})
     */
    void putIfAbsent(LongChunk<? extends Any> keys, LongChunk<? extends Any> values,
            WritableLongChunk<? extends Any> oldValues);

    /**
     * For each element ii of {@code keys}: adds a mapping from {@code keys.get(ii)} to {@code values.get(ii)}, as
     * {@link #put(LongChunk, LongChunk, WritableLongChunk)} does, without reporting the previous values: the form for a
     * caller that has no use for them, which then stages no chunk to receive them. Elements are processed in index
     * order. {@code values} must have at least {@code keys.size()} elements.
     *
     * @param keys the keys to add
     * @param values the values to add
     */
    void put(LongChunk<? extends Any> keys, LongChunk<? extends Any> values);

    /**
     * Adds a mapping from every element of {@code keys} to the one {@code value}, without reporting the previous
     * values. Elements are processed in index order.
     *
     * @param keys the keys to add
     * @param value the value every key maps to
     */
    void put(LongChunk<? extends Any> keys, long value);

    /**
     * Gets the value associated with each element of {@code keys}, writing it to the corresponding element of
     * {@code result}. Keys with no mapping yield {@link #defaultReturnValue()}. On return, the size of {@code result}
     * is set to {@code keys.size()}; its capacity must be at least that large.
     *
     * @param keys the keys to get
     * @param result output: the value of each key (or {@link #defaultReturnValue()})
     * @return the number of keys whose result is not {@link #defaultReturnValue()}, so that a caller which must do
     *         something else about the misses can tell at once whether there were none, or nothing but. (A key mapped
     *         to the no-entry value reads as absent, here as in every read.)
     */
    int get(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> result);

    /**
     * For each element ii of {@code keys}: removes the mapping for {@code keys.get(ii)}, writing the removed value (or
     * {@link #defaultReturnValue()} if there was no mapping) to element ii of {@code oldValues}. Elements are processed
     * in index order; a duplicate key within {@code keys} therefore finds nothing left to remove. On return, the size
     * of {@code oldValues} is set to {@code keys.size()}; its capacity must be at least that large.
     *
     * @param keys the keys to remove
     * @param oldValues output: the removed value of each key (or {@link #defaultReturnValue()})
     */
    void remove(LongChunk<? extends Any> keys, WritableLongChunk<? extends Any> oldValues);

    /**
     * Empty the map in place, retaining its backing array and capacity. Not safe in the presence of concurrent readers.
     */
    void clear();

    void forEach(LongLongBiConsumer consumer);

    /**
     * A reusable cursor for scalar access to a {@link NullableLongLongMap}, for callers whose shape is genuinely
     * per-element. {@link #reset} binds the cursor to a map. Bound to the engine's own map, each call goes straight to
     * that map's scalar entry point: one volatile read, one dispatch on the array's shape tag, one kernel probe, the
     * same price a per-element API paid before the maps spoke chunks. Bound to any other implementation, each call
     * travels as a one-element chunk. Either way the calls are cheap; callers with a loop should still allocate and
     * reset once, outside the loop.
     *
     * <p>
     * Contract: an instance may be used by only one thread at a time. It is valid from the time of {@link #reset}, with
     * the same semantics as any other read of these maps: a concurrent writer will not make it crash, but readers under
     * a clock discipline must discard their work if the clock tells them to. One footnote for writers reading their own
     * map: a mutation performed through this cursor keeps the cursor's own binding fresh, but a mutation through any
     * other path (a chunked call on the map, another cursor, {@link NullableLongLongMap#clear},
     * {@link NullableLongLongMap#resetToNull}) invalidates that thread's bindings to the mutated map — reset again
     * before the next use.
     */
    class ScalarAccess {
        private NullableLongLongMap map;
        // Bound at reset when the map is the engine's own implementation: the operations below then go straight to
        // its scalar entry points, one kernel call per key, with no chunk in between. Any other implementation is
        // served through the one-element chunks — in practice only the tests' reference map and the benchmark's
        // fastutil adapter, since every engine map comes from the factory and is the direct kind.
        private HashMapLockFreeKnVn direct;
        private final WritableLongChunk<Any> keyChunk = WritableLongChunk.writableChunkWrap(new long[1]);
        private final WritableLongChunk<Any> valueChunk = WritableLongChunk.writableChunkWrap(new long[1]);
        private final WritableLongChunk<Any> resultChunk = WritableLongChunk.writableChunkWrap(new long[1]);

        public ScalarAccess(final NullableLongLongMap map) {
            reset(map);
        }

        /**
         * Binds the cursor to {@code map}, replacing any earlier binding. Whatever per-batch setup the map's chunked
         * operations need happens here, once, rather than in every {@link #get}. The binding stays fresh across this
         * cursor's own calls, but a mutation of the map through any other path (a chunked call, another cursor, a
         * clear) invalidates it: reset again before the next use.
         *
         * @param map the map to bind the cursor to
         */
        public void reset(final NullableLongLongMap map) {
            this.map = map;
            this.direct = map instanceof HashMapLockFreeKnVn ? (HashMapLockFreeKnVn) map : null;
        }

        /**
         * Gets the value associated with key, exactly as {@link NullableLongLongMap#get} would. Returns the bound map's
         * {@link NullableLongLongMap#defaultReturnValue()} if no mapping exists.
         */
        public long get(final long key) {
            if (direct != null) {
                return direct.getScalar(key);
            }
            keyChunk.set(0, key);
            map.get(keyChunk, resultChunk);
            return resultChunk.get(0);
        }

        /**
         * Adds a mapping from key to value, exactly as {@link NullableLongLongMap#put} would, returning the previous
         * value (or the bound map's {@link NullableLongLongMap#defaultReturnValue()}). Mutating through the cursor
         * keeps its own binding fresh; only mutation through any other path invalidates it.
         */
        public long put(final long key, final long value) {
            if (direct != null) {
                return direct.putScalar(key, value, false);
            }
            keyChunk.set(0, key);
            valueChunk.set(0, value);
            map.put(keyChunk, valueChunk, resultChunk);
            return resultChunk.get(0);
        }

        /**
         * Adds a mapping from key to value if none exists, exactly as {@link NullableLongLongMap#putIfAbsent} would,
         * returning the previous value (or the bound map's {@link NullableLongLongMap#defaultReturnValue()}). Mutating
         * through the cursor keeps its own binding fresh; only mutation through any other path invalidates it.
         */
        public long putIfAbsent(final long key, final long value) {
            if (direct != null) {
                return direct.putScalar(key, value, true);
            }
            keyChunk.set(0, key);
            valueChunk.set(0, value);
            map.putIfAbsent(keyChunk, valueChunk, resultChunk);
            return resultChunk.get(0);
        }

        /**
         * Removes the mapping for key, exactly as {@link NullableLongLongMap#remove} would, returning the removed value
         * (or the bound map's {@link NullableLongLongMap#defaultReturnValue()} if there was no mapping). Mutating
         * through the cursor keeps its own binding fresh; only mutation through any other path invalidates it.
         */
        public long remove(final long key) {
            if (direct != null) {
                return direct.removeScalar(key);
            }
            keyChunk.set(0, key);
            map.remove(keyChunk, resultChunk);
            return resultChunk.get(0);
        }
    }

    /**
     * The {@link ScalarAccess} for a cursor that outlives its uses, a thread-local one in particular, where the plain
     * cursor's lifetime no longer bounds how long the bound map stays reachable. It is {@link AutoCloseable} so that
     * try-with-resources releases the binding at the end of each use, and {@link #close} is {@code reset(null)}:
     * whatever {@link #reset} binds, {@code close} drops. A cursor that lives in a local variable has no need of this;
     * it dies with its frame. Typical usage:
     *
     * <pre>{@code
     * try (final NullableLongLongMap.AutoCloseableScalarAccess sa = REVERSE_LOOKUP_SCALAR_ACCESS.get()) {
     *     sa.reset(map);
     *     return sa.get(key);
     * }
     * }</pre>
     */
    final class AutoCloseableScalarAccess extends ScalarAccess implements AutoCloseable {
        public AutoCloseableScalarAccess() {
            super(null);
        }

        /**
         * Drops the binding made by {@link #reset}, keeping the cursor's scratch for the next one, so that the cursor
         * never keeps a map, and the array behind it, reachable after the map's owner has let it go.
         */
        @Override
        public void close() {
            reset(null);
        }
    }
}
