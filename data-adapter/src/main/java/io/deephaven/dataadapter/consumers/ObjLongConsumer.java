//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.consumers;

public interface ObjLongConsumer<R> extends java.util.function.ObjLongConsumer<R> {
    void accept(R record, long colValue);
}
