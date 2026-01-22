//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.consumers;

public interface ObjFloatConsumer<R> {
    void accept(R record, float colValue);
}
