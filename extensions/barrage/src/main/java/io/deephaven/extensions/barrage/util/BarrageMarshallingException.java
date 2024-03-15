//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

/**
 * This exception is thrown when Barrage encounters unexpected state while marshalling a gRPC message.
 **/
public final class BarrageMarshallingException extends UncheckedDeephavenException {
    public BarrageMarshallingException(@NotNull final String details) {
        super("Barrage encountered an error while parsing Flight data: " + details);
    }
}
