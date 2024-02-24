package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Wrapper for a {@link JsonNode} to process into a subtable as well as parameters to be used during parsing &
 * processing.
 */
class SubtableData<T> {
    @NotNull
    final SubtableProcessingParameters<T> subtableParameters;

    @Nullable
    final JsonNode subtableNode;

    SubtableData(@NotNull SubtableProcessingParameters<T> subtableParameters,
            @Nullable JsonNode subtableNode) {
        this.subtableParameters = subtableParameters;
        this.subtableNode = subtableNode;
    }

}
