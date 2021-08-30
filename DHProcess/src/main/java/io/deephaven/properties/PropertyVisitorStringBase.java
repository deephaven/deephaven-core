package io.deephaven.properties;

import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * An {@link PropertyVisitor} base which converts the non-String valued calls into
 * {@link #visit(String, String)}.
 */
public abstract class PropertyVisitorStringBase implements PropertyVisitor {

    /**
     * Equivalent to {@code visit(key, Integer.toString(value))}.
     *
     * @param key the key
     * @param value the value
     */
    @Override
    public final void visit(String key, int value) {
        visit(key, Integer.toString(value));
    }

    /**
     * Equivalent to {@code visit(key, Long.toString(value))}.
     *
     * @param key the key
     * @param value the value
     */
    @Override
    public final void visit(String key, long value) {
        visit(key, Long.toString(value));
    }

    /**
     * Equivalent to {@code visit(key, Boolean.toString(value))}.
     *
     * @param key the key
     * @param value the value
     */
    @Override
    public final void visit(String key, boolean value) {
        visit(key, Boolean.toString(value));
    }

    /**
     * Adapts a String {@link BiConsumer} into a {@link PropertyVisitor} via
     * {@link PropertyVisitorStringBase}.
     */
    public static class BiConsumerStringImpl extends PropertyVisitorStringBase {
        private final BiConsumer<String, String> consumer;

        public BiConsumerStringImpl(BiConsumer<String, String> consumer) {
            this.consumer = Objects.requireNonNull(consumer, "consumer");
        }

        @Override
        public void visit(String key, String value) {
            consumer.accept(key, value);
        }
    }
}
