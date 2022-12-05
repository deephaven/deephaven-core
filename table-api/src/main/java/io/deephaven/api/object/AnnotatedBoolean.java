package io.deephaven.api.object;

public class AnnotatedBoolean implements AnnotatedObject {
    private final boolean x;

    public AnnotatedBoolean(boolean x) {
        this.x = x;
    }

    @Override
    public final <T> T visit(Visitor<T> visitor) {
        return visitor.visit(x);
    }
}
