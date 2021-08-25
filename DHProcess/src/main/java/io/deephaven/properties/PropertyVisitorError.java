package io.deephaven.properties;

/**
 * A {@link PropertyVisitor} whose methods all throw {@link IllegalStateException} with the relevant
 * key and value context. Meant to be a utility class that other {@link PropertyVisitor}s can use,
 * for example in the case of parsing an unknown key.
 */
public enum PropertyVisitorError implements PropertyVisitor {
    INSTANCE;

    private static final int MAX_UNTRUNCATED_STRING_LENGTH = 128;

    @Override
    public void visit(String key, String value) {
        if (value.length() > MAX_UNTRUNCATED_STRING_LENGTH) {
            throw new IllegalStateException(
                String.format("Unexpected key/string-value: %s='%s...' (truncated)", key,
                    value.substring(0, 128)));
        }
        throw new IllegalStateException(
            String.format("Unexpected key/string-value: %s='%s'", key, value));
    }

    @Override
    public void visit(String key, int value) {
        throw new IllegalStateException(
            String.format("Unexpected key/int-value: %s=%d", key, value));
    }

    @Override
    public void visit(String key, long value) {
        throw new IllegalStateException(
            String.format("Unexpected key/long-value: %s=%dL", key, value));
    }

    @Override
    public void visit(String key, boolean value) {
        throw new IllegalStateException(
            String.format("Unexpected key/boolean-value: %s=%b", key, value));
    }
}
