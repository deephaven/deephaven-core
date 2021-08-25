package io.deephaven.process;

import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

@Value.Style(
        visibility = ImplementationVisibility.PACKAGE,
        defaults = @Value.Immutable(copy = false),
        strictBuilder = true)
public @interface ProcessStyle {
}
