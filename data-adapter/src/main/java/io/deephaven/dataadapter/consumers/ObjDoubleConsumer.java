//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.consumers;

public interface ObjDoubleConsumer<R> extends java.util.function.ObjDoubleConsumer<R> {
    void accept(R record, double colValue);
}
