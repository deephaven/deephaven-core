//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.consumers;

public interface ObjShortConsumer<R> {
    void accept(R record, short colValue);
}
