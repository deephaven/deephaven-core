package io.deephaven.util.datastructures.cache;

import io.deephaven.hash.IntrusiveChainedHashAdapter;
import io.deephaven.hash.KeyedObjectIntrusiveChainedHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.verify.Require;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedStructureBase;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 * An LRU mapping cache that relies on an queue of intrusively doubly-linked nodes for keeping track of eviction policy
 * details.
 */
public abstract class BoundedIntrusiveMappingCache<KEY_TYPE, MAPPING_TYPE extends BoundedIntrusiveMappingCache.Mapping<KEY_TYPE, MAPPING_TYPE>> {

    final int maximumCachedMappings;

    // TODO: Swap this Map out for a data structure with doubly-linked bucket lists, to enable eviction with no extra
    // lookup.
    final KeyedObjectIntrusiveChainedHashMap<KEY_TYPE, MAPPING_TYPE> mappingCache;
    final IntrusiveDoublyLinkedQueue<MAPPING_TYPE> evictionQueue;

    private BoundedIntrusiveMappingCache(final int maximumCachedMappings) {
        this.maximumCachedMappings = Require.gtZero(maximumCachedMappings, "maximumCachedMappings");
        mappingCache = new KeyedObjectIntrusiveChainedHashMap<KEY_TYPE, MAPPING_TYPE>(MappingCacheAdapter.getInstance(),
                MappingKey.getInstance());
        evictionQueue = new IntrusiveDoublyLinkedQueue<MAPPING_TYPE>(EvictionQueueAdapter.getInstance());
    }

    // ==================================================================================================================
    // Mappings
    // ==================================================================================================================

    /**
     * Re-usable struct to store our key->value records.
     */
    @SuppressWarnings("unchecked")
    static class Mapping<KEY_TYPE, MAPPING_TYPE extends Mapping<KEY_TYPE, MAPPING_TYPE>> {

        KEY_TYPE key;

        MAPPING_TYPE nextInMappingCacheBucket;

        MAPPING_TYPE nextInEvictionQueue = (MAPPING_TYPE) this;
        MAPPING_TYPE prevInEvictionQueue = (MAPPING_TYPE) this;
    }

    private static class ByteMapping<KEY_TYPE> extends Mapping<KEY_TYPE, ByteMapping<KEY_TYPE>> {

        private byte value;

        private void initialize(final KEY_TYPE key, final byte value) {
            this.key = key;
            this.value = value;
        }
    }

    private static class ShortMapping<KEY_TYPE> extends Mapping<KEY_TYPE, ShortMapping<KEY_TYPE>> {

        private short value;

        private void initialize(final KEY_TYPE key, final short value) {
            this.key = key;
            this.value = value;
        }
    }

    private static class IntegerMapping<KEY_TYPE> extends Mapping<KEY_TYPE, IntegerMapping<KEY_TYPE>> {

        private int value;

        private void initialize(final KEY_TYPE key, final int value) {
            this.key = key;
            this.value = value;
        }
    }

    private static class LongMapping<KEY_TYPE> extends Mapping<KEY_TYPE, LongMapping<KEY_TYPE>> {

        private long value;

        private void initialize(final KEY_TYPE key, final long value) {
            this.key = key;
            this.value = value;
        }
    }

    private static class FloatMapping<KEY_TYPE> extends Mapping<KEY_TYPE, FloatMapping<KEY_TYPE>> {

        private float value;

        private void initialize(final KEY_TYPE key, final float value) {
            this.key = key;
            this.value = value;
        }
    }

    private static class DoubleMapping<KEY_TYPE> extends Mapping<KEY_TYPE, DoubleMapping<KEY_TYPE>> {

        private double value;

        private void initialize(final KEY_TYPE key, final double value) {
            this.key = key;
            this.value = value;
        }
    }

    private static class CharacterMapping<KEY_TYPE> extends Mapping<KEY_TYPE, CharacterMapping<KEY_TYPE>> {

        private char value;

        private void initialize(final KEY_TYPE key, final char value) {
            this.key = key;
            this.value = value;
        }
    }

    private static class ObjectMapping<KEY_TYPE, VALUE_TYPE>
            extends Mapping<KEY_TYPE, ObjectMapping<KEY_TYPE, VALUE_TYPE>> {

        private VALUE_TYPE value;

        private void initialize(final KEY_TYPE key, final VALUE_TYPE value) {
            this.key = key;
            this.value = value;
        }
    }

    // ==================================================================================================================
    // Keys and Adapters
    // ==================================================================================================================

    /**
     * Key definition for Mappings.
     */
    private static class MappingKey<KEY_TYPE, MAPPING_TYPE extends BoundedIntrusiveMappingCache.Mapping<KEY_TYPE, MAPPING_TYPE>>
            extends KeyedObjectKey.Basic<KEY_TYPE, MAPPING_TYPE> {

        private static final MappingKey<?, ?> INSTANCE = new MappingKey<>();

        private static <KEY_TYPE, MAPPING_TYPE extends BoundedIntrusiveMappingCache.Mapping<KEY_TYPE, MAPPING_TYPE>> MappingKey<KEY_TYPE, MAPPING_TYPE> getInstance() {
            // noinspection unchecked
            return (MappingKey<KEY_TYPE, MAPPING_TYPE>) INSTANCE;
        }

        @Override
        public KEY_TYPE getKey(final MAPPING_TYPE mapping) {
            return mapping.key;
        }
    }

    /**
     * Adapter for the mapping cache.
     */
    private static class MappingCacheAdapter<KEY_TYPE, MAPPING_TYPE extends BoundedIntrusiveMappingCache.Mapping<KEY_TYPE, MAPPING_TYPE>>
            implements IntrusiveChainedHashAdapter<MAPPING_TYPE> {

        private static final MappingCacheAdapter<?, ?> INSTANCE = new MappingCacheAdapter<>();

        private static <KEY_TYPE, MAPPING_TYPE extends BoundedIntrusiveMappingCache.Mapping<KEY_TYPE, MAPPING_TYPE>> MappingCacheAdapter<KEY_TYPE, MAPPING_TYPE> getInstance() {
            // noinspection unchecked
            return (MappingCacheAdapter<KEY_TYPE, MAPPING_TYPE>) INSTANCE;
        }

        @Override
        public MAPPING_TYPE getNext(@NotNull final MAPPING_TYPE self) {
            return self.nextInMappingCacheBucket;
        }

        @Override
        public void setNext(@NotNull final MAPPING_TYPE self, final MAPPING_TYPE next) {
            self.nextInMappingCacheBucket = next;
        }
    }

    /**
     * Adapter for the eviction queue.
     */
    private static class EvictionQueueAdapter<KEY_TYPE, MAPPING_TYPE extends BoundedIntrusiveMappingCache.Mapping<KEY_TYPE, MAPPING_TYPE>>
            implements IntrusiveDoublyLinkedStructureBase.Adapter<MAPPING_TYPE> {

        private static final EvictionQueueAdapter<?, ?> INSTANCE = new EvictionQueueAdapter<>();

        private static <KEY_TYPE, MAPPING_TYPE extends BoundedIntrusiveMappingCache.Mapping<KEY_TYPE, MAPPING_TYPE>> EvictionQueueAdapter<KEY_TYPE, MAPPING_TYPE> getInstance() {
            // noinspection unchecked
            return (EvictionQueueAdapter<KEY_TYPE, MAPPING_TYPE>) INSTANCE;
        }

        @Override
        public @NotNull MAPPING_TYPE getNext(@NotNull final MAPPING_TYPE node) {
            return node.nextInEvictionQueue;
        }

        @Override
        public void setNext(@NotNull final MAPPING_TYPE node, @NotNull final MAPPING_TYPE other) {
            node.nextInEvictionQueue = other;
        }

        @Override
        public @NotNull MAPPING_TYPE getPrev(@NotNull final MAPPING_TYPE node) {
            return node.prevInEvictionQueue;
        }

        @Override
        public void setPrev(@NotNull final MAPPING_TYPE node, @NotNull final MAPPING_TYPE other) {
            node.prevInEvictionQueue = other;
        }
    }

    // ==================================================================================================================
    // Extra functional interfaces for key->value mapping, since Java defines a limited set
    // ==================================================================================================================

    @FunctionalInterface
    public interface ToByteFunction<T> {
        byte applyAsByte(T value);
    }


    @FunctionalInterface
    public interface ToShortFunction<T> {
        short applyAsShort(T value);
    }


    @FunctionalInterface
    public interface ToFloatFunction<T> {
        float applyAsFloat(T value);
    }


    @FunctionalInterface
    public interface ToCharacterFunction<T> {
        char applyAsCharacter(T value);
    }

    // ==================================================================================================================
    // Per-Type Implementations
    // ==================================================================================================================

    @SuppressWarnings("unused")
    public static class ByteImpl<KEY_TYPE> extends BoundedIntrusiveMappingCache<KEY_TYPE, ByteMapping<KEY_TYPE>> {

        @SuppressWarnings("WeakerAccess")
        public ByteImpl(final int maximumCachedMappings) {
            super(maximumCachedMappings);
        }

        public byte computeIfAbsent(final KEY_TYPE key, @NotNull final ToByteFunction<KEY_TYPE> mapper) {
            ByteMapping<KEY_TYPE> mapping = mappingCache.get(key);
            if (mapping == null) {
                final byte value = mapper.applyAsByte(key);
                if (mappingCache.size() >= maximumCachedMappings) {
                    mapping = evictionQueue.remove();
                    mappingCache.remove(mapping.key);
                } else {
                    mapping = new ByteMapping<>();
                }
                mapping.initialize(key, value);
                mappingCache.add(mapping);
            } else {
                evictionQueue.remove(mapping);
            }
            evictionQueue.offer(mapping);
            return mapping.value;
        }
    }

    @SuppressWarnings("unused")
    public static class ShortImpl<KEY_TYPE> extends BoundedIntrusiveMappingCache<KEY_TYPE, ShortMapping<KEY_TYPE>> {

        @SuppressWarnings("WeakerAccess")
        public ShortImpl(final int maximumCachedMappings) {
            super(maximumCachedMappings);
        }

        public short computeIfAbsent(final KEY_TYPE key, @NotNull final ToShortFunction<KEY_TYPE> mapper) {
            ShortMapping<KEY_TYPE> mapping = mappingCache.get(key);
            if (mapping == null) {
                final short value = mapper.applyAsShort(key);
                if (mappingCache.size() >= maximumCachedMappings) {
                    mapping = evictionQueue.remove();
                    mappingCache.remove(mapping.key);
                } else {
                    mapping = new ShortMapping<>();
                }
                mapping.initialize(key, value);
                mappingCache.add(mapping);
            } else {
                evictionQueue.remove(mapping);
            }
            evictionQueue.offer(mapping);
            return mapping.value;
        }
    }

    /**
     * Mapping cache for {@code Object->int} mappings, with LRU eviction.
     * 
     * @param <KEY_TYPE>
     */
    @SuppressWarnings("unused")
    public static class IntegerImpl<KEY_TYPE> extends BoundedIntrusiveMappingCache<KEY_TYPE, IntegerMapping<KEY_TYPE>> {

        @SuppressWarnings("WeakerAccess")
        public IntegerImpl(final int maximumCachedMappings) {
            super(maximumCachedMappings);
        }

        public int computeIfAbsent(final KEY_TYPE key, @NotNull final ToIntFunction<KEY_TYPE> mapper) {
            IntegerMapping<KEY_TYPE> mapping = mappingCache.get(key);
            if (mapping == null) {
                final int value = mapper.applyAsInt(key);
                if (mappingCache.size() >= maximumCachedMappings) {
                    mapping = evictionQueue.remove();
                    mappingCache.remove(mapping.key);
                } else {
                    mapping = new IntegerMapping<>();
                }
                mapping.initialize(key, value);
                mappingCache.add(mapping);
            } else {
                evictionQueue.remove(mapping);
            }
            evictionQueue.offer(mapping);
            return mapping.value;
        }
    }

    /**
     * Mapping cache for {@code Object->int} mappings, with FIFO eviction.
     * 
     * @param <KEY_TYPE>
     */
    public static class FifoIntegerImpl<KEY_TYPE>
            extends BoundedIntrusiveMappingCache<KEY_TYPE, IntegerMapping<KEY_TYPE>> {

        @SuppressWarnings("WeakerAccess")
        public FifoIntegerImpl(final int maximumCachedMappings) {
            super(maximumCachedMappings);
        }

        public int computeIfAbsent(final KEY_TYPE key, @NotNull final ToIntFunction<KEY_TYPE> mapper) {
            IntegerMapping<KEY_TYPE> mapping = mappingCache.get(key);
            if (mapping == null) {
                final int value = mapper.applyAsInt(key);
                if (mappingCache.size() >= maximumCachedMappings) {
                    mapping = evictionQueue.remove();
                    mappingCache.remove(mapping.key);
                } else {
                    mapping = new IntegerMapping<>();
                }
                mapping.initialize(key, value);
                mappingCache.add(mapping);
                evictionQueue.offer(mapping);
            }
            return mapping.value;
        }

        public void clear() {
            mappingCache.clear();
            evictionQueue.clearFast();
        }
    }

    @SuppressWarnings("unused")
    public static class LongImpl<KEY_TYPE> extends BoundedIntrusiveMappingCache<KEY_TYPE, LongMapping<KEY_TYPE>> {

        @SuppressWarnings("WeakerAccess")
        public LongImpl(final int maximumCachedMappings) {
            super(maximumCachedMappings);
        }

        public long computeIfAbsent(final KEY_TYPE key, @NotNull final ToLongFunction<KEY_TYPE> mapper) {
            LongMapping<KEY_TYPE> mapping = mappingCache.get(key);
            if (mapping == null) {
                final long value = mapper.applyAsLong(key);
                if (mappingCache.size() >= maximumCachedMappings) {
                    mapping = evictionQueue.remove();
                    mappingCache.remove(mapping.key);
                } else {
                    mapping = new LongMapping<>();
                }
                mapping.initialize(key, value);
                mappingCache.add(mapping);
            } else {
                evictionQueue.remove(mapping);
            }
            evictionQueue.offer(mapping);
            return mapping.value;
        }
    }

    @SuppressWarnings("unused")
    public static class FloatImpl<KEY_TYPE> extends BoundedIntrusiveMappingCache<KEY_TYPE, FloatMapping<KEY_TYPE>> {

        @SuppressWarnings("WeakerAccess")
        public FloatImpl(final int maximumCachedMappings) {
            super(maximumCachedMappings);
        }

        public float computeIfAbsent(final KEY_TYPE key, @NotNull final ToFloatFunction<KEY_TYPE> mapper) {
            FloatMapping<KEY_TYPE> mapping = mappingCache.get(key);
            if (mapping == null) {
                final float value = mapper.applyAsFloat(key);
                if (mappingCache.size() >= maximumCachedMappings) {
                    mapping = evictionQueue.remove();
                    mappingCache.remove(mapping.key);
                } else {
                    mapping = new FloatMapping<>();
                }
                mapping.initialize(key, value);
                mappingCache.add(mapping);
            } else {
                evictionQueue.remove(mapping);
            }
            evictionQueue.offer(mapping);
            return mapping.value;
        }
    }

    @SuppressWarnings("unused")
    public static class DoubleImpl<KEY_TYPE> extends BoundedIntrusiveMappingCache<KEY_TYPE, DoubleMapping<KEY_TYPE>> {

        @SuppressWarnings("WeakerAccess")
        public DoubleImpl(final int maximumCachedMappings) {
            super(maximumCachedMappings);
        }

        public double computeIfAbsent(final KEY_TYPE key, @NotNull final ToDoubleFunction<KEY_TYPE> mapper) {
            DoubleMapping<KEY_TYPE> mapping = mappingCache.get(key);
            if (mapping == null) {
                final double value = mapper.applyAsDouble(key);
                if (mappingCache.size() >= maximumCachedMappings) {
                    mapping = evictionQueue.remove();
                    mappingCache.remove(mapping.key);
                } else {
                    mapping = new DoubleMapping<>();
                }
                mapping.initialize(key, value);
                mappingCache.add(mapping);
            } else {
                evictionQueue.remove(mapping);
            }
            evictionQueue.offer(mapping);
            return mapping.value;
        }
    }

    @SuppressWarnings("unused")
    public static class CharacterImpl<KEY_TYPE>
            extends BoundedIntrusiveMappingCache<KEY_TYPE, CharacterMapping<KEY_TYPE>> {

        @SuppressWarnings("WeakerAccess")
        public CharacterImpl(final int maximumCachedMappings) {
            super(maximumCachedMappings);
        }

        public char computeIfAbsent(final KEY_TYPE key, @NotNull final ToCharacterFunction<KEY_TYPE> mapper) {
            CharacterMapping<KEY_TYPE> mapping = mappingCache.get(key);
            if (mapping == null) {
                final char value = mapper.applyAsCharacter(key);
                if (mappingCache.size() >= maximumCachedMappings) {
                    mapping = evictionQueue.remove();
                    mappingCache.remove(mapping.key);
                } else {
                    mapping = new CharacterMapping<>();
                }
                mapping.initialize(key, value);
                mappingCache.add(mapping);
            } else {
                evictionQueue.remove(mapping);
            }
            evictionQueue.offer(mapping);
            return mapping.value;
        }
    }

    @SuppressWarnings("unused")
    public static class ObjectImpl<KEY_TYPE, VALUE_TYPE>
            extends BoundedIntrusiveMappingCache<KEY_TYPE, ObjectMapping<KEY_TYPE, VALUE_TYPE>> {

        @SuppressWarnings("WeakerAccess")
        public ObjectImpl(final int maximumCachedMappings) {
            super(maximumCachedMappings);
        }

        public VALUE_TYPE computeIfAbsent(final KEY_TYPE key, @NotNull final Function<KEY_TYPE, VALUE_TYPE> mapper) {
            ObjectMapping<KEY_TYPE, VALUE_TYPE> mapping = mappingCache.get(key);
            if (mapping == null) {
                final VALUE_TYPE value = mapper.apply(key);
                if (mappingCache.size() >= maximumCachedMappings) {
                    mapping = evictionQueue.remove();
                    mappingCache.remove(mapping.key);
                } else {
                    mapping = new ObjectMapping<>();
                }
                mapping.initialize(key, value);
                mappingCache.add(mapping);
            } else {
                evictionQueue.remove(mapping);
            }
            evictionQueue.offer(mapping);
            return mapping.value;
        }
    }

    // ==================================================================================================================
    // Code Generation Helpers
    // ==================================================================================================================

    public static String getCacheDeclarationType(final Class<?> keyType, final Class<?> valueType) {
        final String keyClassName = TypeUtils.getBoxedType(keyType).getCanonicalName();
        if (valueType == byte.class || valueType == Byte.class) {
            return "BoundedIntrusiveMappingCache.ByteImpl<" + keyClassName + '>';
        }
        if (valueType == short.class || valueType == Short.class) {
            return "BoundedIntrusiveMappingCache.ShortImpl<" + keyClassName + '>';
        }
        if (valueType == int.class || valueType == Integer.class) {
            return "BoundedIntrusiveMappingCache.IntegerImpl<" + keyClassName + '>';
        }
        if (valueType == long.class || valueType == Long.class) {
            return "BoundedIntrusiveMappingCache.LongImpl<" + keyClassName + '>';
        }
        if (valueType == float.class || valueType == Float.class) {
            return "BoundedIntrusiveMappingCache.FloatImpl<" + keyClassName + '>';
        }
        if (valueType == double.class || valueType == Double.class) {
            return "BoundedIntrusiveMappingCache.DoubleImpl<" + keyClassName + '>';
        }
        if (valueType == char.class || valueType == Character.class) {
            return "BoundedIntrusiveMappingCache.CharacterImpl<" + keyClassName + '>';
        }
        final String valueClassName = TypeUtils.getBoxedType(valueType).getCanonicalName();
        return "BoundedIntrusiveMappingCache.ObjectImpl<" + keyClassName + ',' + valueClassName + '>';

    }

    @SuppressWarnings("SameParameterValue")
    public static String getCacheInitializer(final Class<?> valueType, final String maximumCachedMappingsString) {
        if (valueType == byte.class || valueType == Byte.class) {
            return "new BoundedIntrusiveMappingCache.ByteImpl<>(" + maximumCachedMappingsString + ")";
        }
        if (valueType == short.class || valueType == Short.class) {
            return "new BoundedIntrusiveMappingCache.ShortImpl<>(" + maximumCachedMappingsString + ")";
        }
        if (valueType == int.class || valueType == Integer.class) {
            return "new BoundedIntrusiveMappingCache.IntegerImpl<>(" + maximumCachedMappingsString + ")";
        }
        if (valueType == long.class || valueType == Long.class) {
            return "new BoundedIntrusiveMappingCache.LongImpl<>(" + maximumCachedMappingsString + ")";
        }
        if (valueType == float.class || valueType == Float.class) {
            return "new BoundedIntrusiveMappingCache.FloatImpl<>(" + maximumCachedMappingsString + ")";
        }
        if (valueType == double.class || valueType == Double.class) {
            return "new BoundedIntrusiveMappingCache.DoubleImpl<>(" + maximumCachedMappingsString + ")";
        }
        if (valueType == char.class || valueType == Character.class) {
            return "new BoundedIntrusiveMappingCache.CharacterImpl<>(" + maximumCachedMappingsString + ")";
        }
        return "new BoundedIntrusiveMappingCache.ObjectImpl<>(" + maximumCachedMappingsString + ")";
    }
}
