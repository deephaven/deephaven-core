package io.deephaven.process;

import org.immutables.value.Value;
import org.immutables.value.Value.Style.ImplementationVisibility;

// declare style as meta annotation as shown
// or on package/top-level class
// This is just an example, adapt to your taste however you like
@Value.Style(
        // Detect names starting with underscore
        typeAbstract = "_*",
        // Generate without any suffix, just raw detected name
        typeImmutable = "*",
        // Make generated public, leave underscored as package private
        visibility = ImplementationVisibility.PUBLIC,
        defaults = @Value.Immutable(copy = false, builder = false))
@interface Wrapped {
}
