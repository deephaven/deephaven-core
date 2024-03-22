//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.consumers;

public interface ObjByteConsumer<R> {
    void accept(R record, byte colValue);
}
