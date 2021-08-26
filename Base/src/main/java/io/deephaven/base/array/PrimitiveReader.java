/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.array;

public interface PrimitiveReader {
    int remaining();

    boolean nextBoolean();

    byte nextByte();

    char nextChar();

    short nextShort();

    int nextInt();

    long nextLong();

    float nextFloat();

    double nextDouble();
}
