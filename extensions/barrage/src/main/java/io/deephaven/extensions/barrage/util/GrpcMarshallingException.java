/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage.util;

import io.deephaven.UncheckedDeephavenException;

public class GrpcMarshallingException extends UncheckedDeephavenException {
    public GrpcMarshallingException(final String reason, final Exception cause) {
        super(reason, cause);
    }
}
