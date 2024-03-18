//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.kafka.ingest;

import io.deephaven.processor.function.ObjectProcessorFunctions;
import io.deephaven.function.TypedFunction;

import java.util.List;

public final class FieldCopierAdapter {

    public static FieldCopier of(TypedFunction<Object> f) {
        return new FieldCopierProcessorImpl(ObjectProcessorFunctions.of(List.of(f)));
    }
}
