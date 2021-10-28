package io.deephaven.client.examples;

import io.deephaven.uri.DeephavenTarget;
import picocli.CommandLine.ITypeConverter;

import java.net.URI;

class DeephavenTargetConverter implements ITypeConverter<DeephavenTarget> {

    @Override
    public DeephavenTarget convert(String value) {
        return DeephavenTarget.of(URI.create(value));
    }
}
