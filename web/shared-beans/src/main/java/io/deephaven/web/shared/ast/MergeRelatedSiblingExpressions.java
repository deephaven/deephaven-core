package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * If two or more IN-like expressions are in the same OR (or NOT-INs in the same AND), join them
 * together into a single expression. This may change the order of expressions, but should not have
 * side effects - execution order of null checks vs invoke for example, should not be affected by
 * this. All IN expressions are moved to the front of the list.
 *
 * Examples: o A in 1 AND B in 2 - left as is o A not in 1 AND A not in 2 - rewritten as A not in 1,
 * 2 o A == B OR A in 1 OR A == C OR A in 2 - rewritten as A in 1, 2 OR A == B OR A == C
 *
 * This assumes that all nested ORs and ANDs have been flattened already, NOTs normalized, and that
 * we're happy with EQs vs INs and their children.
 */
public class MergeRelatedSiblingExpressions extends ReplacingVisitor {
    public static FilterDescriptor execute(final FilterDescriptor filter) {
        return new MergeRelatedSiblingExpressions().visit(filter);
    }

    @Override
    public FilterDescriptor onAnd(final FilterDescriptor descriptor) {
        FilterDescriptor result =
            mergeChildren(descriptor, FilterDescriptor.FilterOperation.NOT_IN);
        return mergeChildren(result, FilterDescriptor.FilterOperation.NOT_IN_ICASE);
    }

    @Override
    public FilterDescriptor onOr(final FilterDescriptor descriptor) {
        FilterDescriptor result = mergeChildren(descriptor, FilterDescriptor.FilterOperation.IN);
        return mergeChildren(result, FilterDescriptor.FilterOperation.IN_ICASE);
    }

    private FilterDescriptor mergeChildren(FilterDescriptor descriptor,
        FilterDescriptor.FilterOperation op) {
        // Examine each child and group by reference name and operation,
        // and combine those into a single operation
        final Map<FilterDescriptor, Set<FilterDescriptor>> collected = new LinkedHashMap<>();
        final List<FilterDescriptor> leftover = new ArrayList<>();
        for (final FilterDescriptor child : descriptor.getChildren()) {
            if (child.getOperation() == op) {
                assert child.getChildren()[0]
                    .getOperation() == FilterDescriptor.FilterOperation.REFERENCE;
                collected.computeIfAbsent(child.getChildren()[0], ignore -> new LinkedHashSet<>())
                    .addAll(Arrays.stream(child.getChildren()).skip(1)
                        .collect(Collectors.toCollection(LinkedHashSet::new)));
            } else {
                leftover.add(child);
            }
        }

        int newChildCount = leftover.size() + collected.size();

        if (newChildCount == descriptor.getChildren().length) {
            // no items can be merged, check children and move on
            return visitChildren(descriptor);
        }

        if (newChildCount == 1) {
            assert leftover.isEmpty()
                : "Must be empty since collected is non-empty since the new and old child counts differ";
            // only one expression remains from the collection work

            Map.Entry<FilterDescriptor, Set<FilterDescriptor>> nameAndValues =
                collected.entrySet().iterator().next();
            return mergedFilterDescriptor(op, nameAndValues.getKey(), nameAndValues.getValue());
        }

        FilterDescriptor[] replacementChildren = new FilterDescriptor[newChildCount];

        // produce new IN-type expressions
        int i = 0;
        for (Map.Entry<FilterDescriptor, Set<FilterDescriptor>> nameAndValues : collected
            .entrySet()) {
            replacementChildren[i++] =
                mergedFilterDescriptor(op, nameAndValues.getKey(), nameAndValues.getValue());
        }

        // copy in the remaining leftovers
        for (i = collected.size(); i < newChildCount; i++) {
            // check if this leftover itself can be merged, then append it
            replacementChildren[i] = visit(leftover.get(i - collected.size()));
        }

        return new FilterDescriptor(descriptor.getOperation(), null, null, replacementChildren);
    }

    private FilterDescriptor mergedFilterDescriptor(FilterDescriptor.FilterOperation op,
        FilterDescriptor column, Set<FilterDescriptor> values) {
        return new FilterDescriptor(op, null, null, Stream.concat(
            Stream.of(column),
            values.stream()).toArray(FilterDescriptor[]::new));
    }
}
