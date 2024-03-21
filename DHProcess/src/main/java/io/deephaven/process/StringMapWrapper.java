//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import java.util.Map;

public abstract class StringMapWrapper extends Wrapper<Map<String, String>> implements PropertySet {

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visitProperties(PropertySet.of(value()));
    }
}
