package io.deephaven.jsoningester;

import com.fasterxml.jackson.databind.JsonNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 *
 * @param <T> Adapter type (i.e. either {@link JSONToTableWriterAdapter} or {@link JSONToStreamPublisherAdapter}
 */
class SubtableProcessingParameters<T> {

    /**
     * The field to process into a subtable
     */
    @Nullable
    final String fieldName;
    @NotNull
    final T subtableAdapter;

    /**
     * Predicate to evaluate to determine whether to actually process the corresponding field for a given row
     */
    @Nullable
    final Predicate<JsonNode> subtablePredicate;

    @NotNull
    final AtomicLong subtableMessageCounter;

    final boolean subtableKeyAllowedMissing;

    final boolean subtableKeyAllowedNull;

    /**
     * Whether the subtable node is expected to be an ArrayNode (as opposed to an ObjectNode). This should be
     * {@code true} for 'routed' subtables (where we just send the original node to the subtable adapter) and
     * {@code false} for regular (nested) subtables (where many rows are expected).
     */
    final boolean isArrayNodeExpected;

    SubtableProcessingParameters(
            @Nullable String fieldName,
            @NotNull T subtableAdapter,
            @Nullable Predicate<JsonNode> subtablePredicate,
            @NotNull AtomicLong subtableMessageCounter,
            boolean subtableKeyAllowedMissing,
            boolean subtableKeyAllowedNull, boolean isArrayNodeExpected) {
        this.fieldName = fieldName;
        this.subtableAdapter = subtableAdapter;
        this.subtablePredicate = subtablePredicate;
        this.subtableMessageCounter = subtableMessageCounter;
        this.subtableKeyAllowedMissing = subtableKeyAllowedMissing;
        this.subtableKeyAllowedNull = subtableKeyAllowedNull;
        this.isArrayNodeExpected = isArrayNodeExpected;
    }
}
