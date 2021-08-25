package io.deephaven.properties;

import java.util.Objects;

class PropertyVisitorPrefixed implements PropertyVisitor {
    private final String prefix;
    private final PropertyVisitor delegate;

    PropertyVisitorPrefixed(String prefix, PropertyVisitor delegate) {
        this.prefix = Objects.requireNonNull(prefix, "prefix");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (prefix.isEmpty()) {
            throw new IllegalArgumentException("Expected non-empty prefix");
        }
    }

    private String prefix(String key) {
        return prefix + key;
    }

    @Override
    public void visit(String key, String value) {
        delegate.visit(prefix(key), value);
    }

    @Override
    public void visit(String key, int value) {
        delegate.visit(prefix(key), value);
    }

    @Override
    public void visit(String key, long value) {
        delegate.visit(prefix(key), value);
    }

    @Override
    public void visit(String key, boolean value) {
        delegate.visit(prefix(key), value);
    }

    @Override
    public void visitProperties(String key, PropertySet properties) {
        new PropertyVisitorPrefixed(prefix + key + SEPARATOR, delegate)
                .visitProperties(properties);
    }
}
