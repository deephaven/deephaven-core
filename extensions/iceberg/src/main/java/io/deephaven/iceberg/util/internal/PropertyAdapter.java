//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.util.internal;

import java.util.Map;
import java.util.ServiceLoader;

public interface PropertyAdapter {

    static void serviceLoaderAdapt(Object dataInstructions, Map<String, String> propertiesOut) {
        for (PropertyAdapter propertyAdapter : ServiceLoader.load(PropertyAdapter.class)) {
            propertyAdapter.adapt(dataInstructions, propertiesOut);
        }
    }

    void adapt(Object dataInstructions, Map<String, String> propertiesOut);
}
