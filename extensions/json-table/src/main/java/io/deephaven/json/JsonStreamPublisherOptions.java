//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json;

import io.deephaven.annotations.BuildableStyle;
import org.immutables.value.Value.Immutable;

@Immutable
@BuildableStyle
public abstract class JsonStreamPublisherOptions {

    public static Builder builder() {
        return ImmutableJsonStreamPublisherOptions.builder();
    }

    /**
     * The JSON options.
     */
    public abstract ValueOptions options();

    /**
     * If multi-value JSON sources should be parsed.
     */
    public abstract boolean multiValueSupport();

    /**
     * The chunk size.
     */
    public abstract int chunkSize();

    /**
     * Creates the json stream publisher with the default json publishing provider. Equivalent to
     * {@code JsonPublishingProvider.serviceLoader().of(this)}.
     *
     * @return the json stream publisher
     * @see JsonPublishingProvider#serviceLoader()
     */
    public final JsonStreamPublisher execute() {
        return JsonPublishingProvider.serviceLoader().of(this);
    }

    public interface Builder {
        Builder options(ValueOptions options);

        Builder multiValueSupport(boolean multiValueSupport);

        Builder chunkSize(int chunkSize);

        JsonStreamPublisherOptions build();
    }
}
