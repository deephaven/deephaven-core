package io.deephaven.api.object;

public interface AnnotatedObject {

    <T> T visit(Visitor<T> visitor);

    interface Visitor<T> {
        T visit(boolean x);

        T visit(char x);

        T visit(byte x);

        T visit(short x);

        T visit(int x);

        T visit(long x);

        T visit(float x);

        T visit(double x);

        T visit(Object x);
    }
}
