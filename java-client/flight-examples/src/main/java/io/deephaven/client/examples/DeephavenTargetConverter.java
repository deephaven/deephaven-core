package io.deephaven.client.examples;

import io.deephaven.uri.DeephavenTarget;
import picocli.CommandLine.ITypeConverter;

class DeephavenTargetConverter implements ITypeConverter<DeephavenTarget> {

    @Override
    public DeephavenTarget convert(String value) {
        return DeephavenTarget.of(value);
    }
}
