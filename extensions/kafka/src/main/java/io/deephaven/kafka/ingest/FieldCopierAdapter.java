/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.processor.functions.ObjectProcessorFunctions;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.function.ToBooleanFunction;
import io.deephaven.function.ToByteFunction;
import io.deephaven.function.ToCharFunction;
import io.deephaven.function.ToDoubleFunction;
import io.deephaven.function.ToFloatFunction;
import io.deephaven.function.ToIntFunction;
import io.deephaven.function.ToLongFunction;
import io.deephaven.function.ToObjectFunction;
import io.deephaven.function.ToPrimitiveFunction;
import io.deephaven.function.ToShortFunction;
import io.deephaven.function.TypedFunction;

import java.util.List;

public final class FieldCopierAdapter {

    public static FieldCopier of(TypedFunction<Object> f) {
        return new FieldCopierProcessorImpl(ObjectProcessorFunctions.of(List.of(f)));
    }
}
