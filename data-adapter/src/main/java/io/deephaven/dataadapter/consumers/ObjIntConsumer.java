//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.consumers;

public interface ObjIntConsumer<R> extends java.util.function.ObjIntConsumer<R> {
    void accept(R record, int colValue);
}
