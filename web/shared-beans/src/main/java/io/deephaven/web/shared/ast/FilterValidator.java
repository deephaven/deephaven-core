package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;
import io.deephaven.web.shared.data.FilterDescriptor.FilterOperation;
import io.deephaven.web.shared.data.FilterDescriptor.Kind;
import io.deephaven.web.shared.data.FilterDescriptor.ValueType;
import io.deephaven.web.shared.util.ParseUtils;

import java.util.Arrays;
import java.util.Stack;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * Sanity checks a filter and its children, to confirm that each object makes sense with regard to its fields, child
 * count, etc.
 */
public class FilterValidator implements FilterDescriptor.Visitor {
    private Stack<FilterDescriptor> stack = new Stack<>();
    private final BiPredicate<String, FilterDescriptor[]> invokeCheck;
    private final Predicate<String> columnCheck;

    public FilterValidator(BiPredicate<String, FilterDescriptor[]> invokeCheck, Predicate<String> columnCheck) {
        this.invokeCheck = invokeCheck;
        this.columnCheck = columnCheck;
    }


    private void check(boolean assertion, String description) {
        if (!assertion) {
            throw new IllegalStateException(description + " at " + stackToString());
        }
    }

    private String stackToString() {
        return stack.toString();
    }

    @Override
    public void onAnd(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length >= 2, descriptor.getChildren().length + " > 2");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            check(child.getOperation().expressionKind == Kind.Condition,
                    child.getOperation().expressionKind + " == Condition");
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onOr(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length >= 2, descriptor.getChildren().length + " > 2");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            check(child.getOperation().expressionKind == Kind.Condition,
                    child.getOperation().expressionKind + " == Condition");
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onNot(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length == 1, descriptor.getChildren().length + " == 1");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            check(child.getOperation().expressionKind == Kind.Condition,
                    child.getOperation().expressionKind + " == Condition");
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onLessThan(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length == 2, descriptor.getChildren().length + " == 2");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            check(child.getOperation().expressionKind == Kind.Value, child.getOperation().expressionKind + " == Value");
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onGreaterThan(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length == 2, descriptor.getChildren().length + " == 2");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            check(child.getOperation().expressionKind == Kind.Value, child.getOperation().expressionKind + " == Value");
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onLessThanOrEqualTo(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length == 2, descriptor.getChildren().length + " == 2");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            check(child.getOperation().expressionKind == Kind.Value, child.getOperation().expressionKind + " == Value");
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onGreaterThanOrEqualTo(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length == 2, descriptor.getChildren().length + " == 2");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            check(child.getOperation().expressionKind == Kind.Value, child.getOperation().expressionKind + " == Value");
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onEqual(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length == 2, descriptor.getChildren().length + " == 2");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            check(child.getOperation().expressionKind == Kind.Value, child.getOperation().expressionKind + " == Value");
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onEqualIgnoreCase(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length == 2, descriptor.getChildren().length + " == 2");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            check(child.getOperation().expressionKind == Kind.Value, child.getOperation().expressionKind + " == Value");
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onNotEqual(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length == 2, descriptor.getChildren().length + " == 2");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            check(child.getOperation().expressionKind == Kind.Value, child.getOperation().expressionKind + " == Value");
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onNotEqualIgnoreCase(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length == 2, descriptor.getChildren().length + " == 2");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            check(child.getOperation().expressionKind == Kind.Value, child.getOperation().expressionKind + " == Value");
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onIn(FilterDescriptor descriptor) {
        validateIn(descriptor);
    }

    @Override
    public void onInIgnoreCase(FilterDescriptor descriptor) {
        validateIn(descriptor);
    }

    @Override
    public void onNotIn(FilterDescriptor descriptor) {
        validateIn(descriptor);
    }

    @Override
    public void onNotInIgnoreCase(FilterDescriptor descriptor) {
        validateIn(descriptor);
    }

    private void validateIn(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length >= 2, descriptor.getChildren().length + " >= 2");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onIsNull(FilterDescriptor descriptor) {
        stack.push(descriptor);
        check(descriptor.getChildren().length == 1, descriptor.getChildren().length + " == 1");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            child.accept(this);
        }
        stack.pop();
    }

    @Override
    public void onInvoke(FilterDescriptor descriptor) {
        stack.push(descriptor);

        check(descriptor.getValue() != null, "value != null");

        // check name+args against known whitelist - may be impl'd different on client than server
        check(invokeCheck.test(descriptor.getValue(), descriptor.getChildren()),
                "User filters are not permitted to use method " + descriptor.getValue());

        check(descriptor.getChildren().length > 0,
                "Invocation  is poorly formed, must have at least one child representing the instance");

        boolean isStatic = descriptor.getChildren()[0] == null;

        for (int i = 0; i < descriptor.getChildren().length; i++) {
            if (i == 0 && isStatic) {
                continue;
            }
            FilterDescriptor child = descriptor.getChildren()[i];
            check(child != null, "Invoke parameter may not be null");
            child.accept(this);
        }
        stack.pop();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    public void onLiteral(FilterDescriptor descriptor) {
        check(descriptor.getValue() != null, "value != null");
        check(descriptor.getType() != null, "type != null");

        switch (descriptor.getType()) {
            case String:
                // no checks required
                break;
            case Number:
                // verify it parses as a double
                Double.parseDouble(descriptor.getValue());
                break;
            case Boolean:
                // verify it parses as a boolean
                ParseUtils.parseBoolean(descriptor.getValue());
                break;
            case Datetime:
            case Long:
                // verify it parses as a long
                Long.parseLong(descriptor.getValue());
                break;
            case Other:
                // fail, not currently supported
                throw new IllegalStateException("Not currently supported");
        }

        check(descriptor.getChildren().length == 0, descriptor.getChildren().length + " == 0");
    }

    @Override
    public void onReference(FilterDescriptor descriptor) {
        check(descriptor.getValue() != null, "value != null");

        check(descriptor.getChildren().length == 0, descriptor.getChildren().length + " == 0");

        // verify that this reference points to a column name in the current table
        check(columnCheck.test(descriptor.getValue()), "Unknown column name");
    }

    @Override
    public void onContains(FilterDescriptor descriptor) {
        validatePatternFilter(descriptor, "Contains");
    }

    @Override
    public void onContainsIgnoreCase(FilterDescriptor descriptor) {
        validatePatternFilter(descriptor, "ContainsIgnoreCase");
    }

    @Override
    public void onPattern(FilterDescriptor descriptor) {
        validatePatternFilter(descriptor, "Pattern");
    }

    @Override
    public void onPatternIgnoreCase(FilterDescriptor descriptor) {
        validatePatternFilter(descriptor, "PatternIgnoreCase");
    }

    private void validatePatternFilter(FilterDescriptor descriptor, String name) {
        check(descriptor.getChildren().length == 2, name + " must have one column reference and one string parameter");
        final FilterDescriptor col = descriptor.getChildren()[0];
        final FilterDescriptor param = descriptor.getChildren()[1];

        // note that the REFERENCE/LITERAL restrictions could be relaxed
        check(col != null, name + " must not be called on a null value");
        check(col.getOperation() == FilterOperation.REFERENCE, name + " can only be called on a column reference");
        onReference(col);

        check(param != null, name + " must not be passed a null parameter");
        check(param.getType() == ValueType.String && param.getOperation() == FilterOperation.LITERAL,
                name + " must be given a string literal parameter");
        onLiteral(param);
    }

    @Override
    public void onSearch(FilterDescriptor descriptor) {
        // verify we aren't nested in a NOT
        if (stack.size() > 0) {
            check(stack.peek().getOperation() != FilterOperation.NOT, "Not(Search) is not supported");
        }

        check(descriptor.getChildren().length >= 1, "Search must have at least one param");
        FilterDescriptor param = descriptor.getChildren()[0];

        check(param != null, "Search must not be passed a null value");
        check(param.getType() == ValueType.String && param.getOperation() == FilterOperation.LITERAL,
                "Search must be given a string literal parameter");
        onLiteral(param);

        Arrays.stream(descriptor.getChildren()).skip(1).forEach(col -> {
            check(col != null, "Search column must not be null");
            check(col.getOperation() == FilterOperation.REFERENCE, "Search column must be a column reference");
            onReference(col);
        });
    }
}
