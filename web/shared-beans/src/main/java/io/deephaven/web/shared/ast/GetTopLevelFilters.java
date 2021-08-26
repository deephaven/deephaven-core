package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;
import io.deephaven.web.shared.data.FilterDescriptor.FilterOperation;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Recursively removes top-level AND operations, if any, resulting in an array of filters to run, potentially only
 * containing a single filter if the top-level filter is not an AND.
 *
 * This can be run before the FilterPrinter to provide multiple strings to execute individually. It should be run after
 * other optimizations which might merge operations, and should be run before nested Match operations are rewritten to
 * invocations.
 */
public class GetTopLevelFilters {

    public static List<FilterDescriptor> get(FilterDescriptor descriptor) {
        if (descriptor.getOperation() == FilterOperation.AND) {
            return Arrays.asList(descriptor.getChildren());
        }
        return Collections.singletonList(descriptor);
    }

}
