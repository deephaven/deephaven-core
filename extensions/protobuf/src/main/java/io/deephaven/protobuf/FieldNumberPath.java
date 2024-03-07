//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.protobuf;

import io.deephaven.annotations.SimpleStyle;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

@Immutable
@SimpleStyle
public abstract class FieldNumberPath {

    public static FieldNumberPath of(int... path) {
        return ImmutableFieldNumberPath.of(path);
    }

    @Parameter
    public abstract int[] path();
}
