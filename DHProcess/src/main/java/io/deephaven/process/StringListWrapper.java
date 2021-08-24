package io.deephaven.process;

import io.deephaven.properties.PropertySet;
import io.deephaven.properties.PropertyVisitor;
import java.util.List;

public abstract class StringListWrapper extends Wrapper<List<String>> implements PropertySet {

    @Override
    public final void traverse(PropertyVisitor visitor) {
        visitor.visitProperties(PropertySet.of(value()));
    }
}
