package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;

import java.util.ArrayList;
import java.util.List;

/**
 * Any AND nested within another AND or OR nested within another OR should be flattened into just a single level.
 *
 * This should be run after NOTs are distributed (so that (A AND B AND !(C OR D)) is first normalized to (A AND B AND
 * (!C AND !D))).
 */
public class MergeNestedBinaryOperations extends ReplacingVisitor {
    public static FilterDescriptor execute(FilterDescriptor descriptor) {
        return new MergeNestedBinaryOperations().visit(descriptor);
    }

    @Override
    public FilterDescriptor onAnd(FilterDescriptor descriptor) {
        ArrayList<FilterDescriptor> topLevel = new ArrayList<>();
        handleItem(topLevel, descriptor, FilterDescriptor.FilterOperation.AND);

        if (descriptor.getChildren().length == topLevel.size()) {
            // we made no changes, since any nested AND or OR must contain at least two children
            return super.onAnd(descriptor);
        }

        FilterDescriptor replacement =
                new FilterDescriptor(descriptor.getOperation(), null, null, topLevel.toArray(new FilterDescriptor[0]));

        return super.onAnd(replacement);
    }

    @Override
    public FilterDescriptor onOr(FilterDescriptor descriptor) {
        ArrayList<FilterDescriptor> topLevel = new ArrayList<>();
        handleItem(topLevel, descriptor, FilterDescriptor.FilterOperation.OR);

        if (descriptor.getChildren().length == topLevel.size()) {
            // we made no changes, since any nested AND or OR must contain at least two children
            return super.onOr(descriptor);
        }

        FilterDescriptor replacement =
                new FilterDescriptor(descriptor.getOperation(), null, null, topLevel.toArray(new FilterDescriptor[0]));

        return super.onOr(replacement);
    }

    private void handleItem(List<FilterDescriptor> topLevel, FilterDescriptor descriptor,
            FilterDescriptor.FilterOperation operation) {
        if (descriptor.getOperation() == operation) {
            for (FilterDescriptor child : descriptor.getChildren()) {
                handleItem(topLevel, child, operation);
            }
        } else {
            topLevel.add(descriptor);
        }
    }
}
