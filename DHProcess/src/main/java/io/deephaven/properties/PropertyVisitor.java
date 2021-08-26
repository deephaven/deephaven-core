package io.deephaven.properties;

import io.deephaven.properties.PropertyVisitorStringBase.BiConsumerStringImpl;
import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.BiConsumer;

// todo: should we implement this interface w/ KeyPath or KeyPath-like keys instead?

/**
 * A property visitor is the generic interface for reading property keys and values from a {@link PropertySet}.
 *
 * @see PropertySet
 */
public interface PropertyVisitor {
    char SEPARATOR = '.';

    static PropertyVisitor stdout() {
        return of(System.out);
    }

    static PropertyVisitor stderr() {
        return of(System.err);
    }

    static PropertyVisitor of(PrintStream out) {
        return new PropertyVisitorPrintStream(out);
    }

    static PropertyVisitor of(BiConsumer<String, String> consumer) {
        return new BiConsumerStringImpl(consumer);
    }

    static Map<String, String> toStringMap(PropertySet properties) {
        final Map<String, String> map = new LinkedHashMap<>();
        of(map::put).visitProperties(properties);
        return map;
    }

    /**
     * Performs this operation on the given key and String value.
     *
     * @param key the key
     * @param value the value
     */
    void visit(String key, String value);

    /**
     * Performs this operation on the given key and int value.
     *
     * @param key the key
     * @param value the value
     */
    void visit(String key, int value);

    /**
     * Performs this operation on the given key and long value.
     *
     * @param key the key
     * @param value the value
     */
    void visit(String key, long value);

    /**
     * Performs this operation on the given key and boolean value.
     *
     * @param key the key
     * @param value the value
     */
    void visit(String key, boolean value);

    /**
     * By default, is equivalent to {@code properties.traverse(this)}. Implementations may choose to override this
     * method, provided the property set is traversed, and this visitor receives all of the updates.
     *
     * @param properties the property set
     */
    default void visitProperties(PropertySet properties) {
        properties.traverse(this);
    }

    /**
     * A helper method that recursively builds up the keys based on the provided key, and the keys of the property set.
     * The majority of implementations should <b>not</b> override this.
     *
     * @param key the key
     * @param properties the property set
     */
    default void visitProperties(String key, PropertySet properties) {
        new PropertyVisitorPrefixed(key + SEPARATOR, this)
                .visitProperties(properties);
    }

    // note: the following helper methods exhibit poor coding from a traditional sense - but it
    // makes traverse implementations much cleaner.

    /**
     * A helper method that makes {@link PropertySet#traverse(PropertyVisitor)} implementations cleaner. Equivalent to
     * {@code value.ifPresent(x -> visit(key, x))}. Must not be overridden.
     *
     * @param key the key
     * @param value the optional value
     */
    default void maybeVisit(String key,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<String> value) {
        value.ifPresent(x -> visit(key, x));
    }

    /**
     * A helper method that makes {@link PropertySet#traverse(PropertyVisitor)} implementations cleaner. Equivalent to
     * {@code value.ifPresent(x -> visit(key, x))}. Must not be overridden.
     *
     * @param key the key
     * @param value the optional value
     */
    default void maybeVisit(String key, @SuppressWarnings("OptionalUsedAsFieldOrParameterType") OptionalInt value) {
        value.ifPresent(x -> visit(key, x));
    }

    /**
     * A helper method that makes {@link PropertySet#traverse(PropertyVisitor)} implementations cleaner. Equivalent to
     * {@code value.ifPresent(x -> visit(key, x))}. Must not be overridden.
     *
     * @param key the key
     * @param value the optional value
     */
    default void maybeVisit(String key, @SuppressWarnings("OptionalUsedAsFieldOrParameterType") OptionalLong value) {
        value.ifPresent(x -> visit(key, x));
    }

    /**
     * A helper method that makes {@link PropertySet#traverse(PropertyVisitor)} implementations cleaner. Equivalent to
     * {@code properties.ifPresent(x -> visitProperties(key, x))}. Must not be overridden.
     *
     * @param key the key
     * @param properties the optional value
     */
    default void maybeVisitProperties(String key,
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType") Optional<? extends PropertySet> properties) {
        properties.ifPresent(x -> visitProperties(key, x));
    }
}
