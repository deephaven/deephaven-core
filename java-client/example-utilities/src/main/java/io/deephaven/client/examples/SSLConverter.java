/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.ssl.config.SSLConfig;
import picocli.CommandLine.ITypeConverter;

import java.io.IOException;
import java.nio.file.Paths;

class SSLConverter implements ITypeConverter<SSLConfig> {

    @Override
    public SSLConfig convert(String file) throws IOException {
        return SSLConfig.parseJson(Paths.get(file));
    }
}
