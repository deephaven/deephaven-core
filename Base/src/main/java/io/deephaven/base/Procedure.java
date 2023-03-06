/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.base;

public class Procedure {

    @FunctionalInterface
    public interface UnaryByte {
        void call(byte arg);
    }

    @FunctionalInterface
    public interface UnaryChar {
        void call(char arg);
    }

    @FunctionalInterface
    public interface UnaryFloat {
        void call(float arg);
    }

    @FunctionalInterface
    public interface UnaryShort {
        void call(short arg);
    }
}
