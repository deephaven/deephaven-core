//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.updategraph;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

/**
 * Runtime exception thrown when a thread would wait for an {@link UpdateGraph}, either to acquire one of its locks or
 * for its update cycle to satisfy a dependency, while {@link UpdateGraphWaitGuard#forbidWaits() forbidden} to do so.
 * The work that was in progress is abandoned, and may be retried by a caller that is permitted to wait.
 */
public class UpdateGraphWaitForbiddenException extends UncheckedDeephavenException {

    public UpdateGraphWaitForbiddenException(@NotNull final String message) {
        super(message);
    }
}
