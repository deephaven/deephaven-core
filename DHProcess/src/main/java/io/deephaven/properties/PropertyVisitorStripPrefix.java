package io.deephaven.properties;

import java.util.Objects;

/**
 * A property visitor which expects and strips a given prefix from all keys, and then delegates to
 * the given {@link PropertyVisitor}.
 *
 * For example, if the delegate knows how to parse the property keys {@code foo} and {@code bar},
 * but the properties are currently in a different context {@code parent.foo} and
 * {@code parent.bar}, a {@link PropertyVisitorStripPrefix} can be provide with prefix "parent.".
 */
class PropertyVisitorStripPrefix implements PropertyVisitor {
    private final String prefix;
    private final PropertyVisitor delegate;

    PropertyVisitorStripPrefix(String prefix, PropertyVisitor delegate) {
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (prefix.isEmpty()) {
            throw new IllegalArgumentException("Expected non-empty prefix");
        }
    }

    private String strip(String key) {
        if (!key.startsWith(prefix)) {
            throw new IllegalArgumentException(
                String.format("Key '%s' does not start with prefix '%s'", key, prefix));
        }
        return key.substring(prefix.length());
    }

    @Override
    public void visit(String key, String value) {
        delegate.visit(strip(key), value);
    }

    @Override
    public void visit(String key, int value) {
        delegate.visit(strip(key), value);
    }

    @Override
    public void visit(String key, long value) {
        delegate.visit(strip(key), value);
    }

    @Override
    public void visit(String key, boolean value) {
        delegate.visit(strip(key), value);
    }
}
