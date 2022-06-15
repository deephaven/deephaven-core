/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.properties;

import java.io.PrintStream;
import java.util.Objects;

public class PropertyVisitorPrintStream implements PropertyVisitor {

    private final PrintStream out;

    public PropertyVisitorPrintStream(PrintStream out) {
        this.out = Objects.requireNonNull(out, "out");
    }

    @Override
    public void visit(String key, String value) {
        out.println(String.format("%s='%s'", key, value));
    }

    @Override
    public void visit(String key, int value) {
        out.println(String.format("%s=%d", key, value));
    }

    @Override
    public void visit(String key, long value) {
        out.println(String.format("%s=%dL", key, value));
    }

    @Override
    public void visit(String key, boolean value) {
        out.println(String.format("%s=%b", key, value));
    }
}
