//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.dataadapter.consumers;

public interface ObjCharConsumer<R> {
    void accept(R record, char colValue);
}
