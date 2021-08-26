package io.deephaven.web.shared.ast;

import io.deephaven.web.shared.data.FilterDescriptor;
import io.deephaven.web.shared.data.FilterDescriptor.Kind;
import io.deephaven.web.shared.util.ParseUtils;

import java.util.function.UnaryOperator;

/**
 * Prints a readable string representing this filter. Note that this is presently only suitable for debugging purposes,
 * and doesn't generate something that the SelectFilterFactory can handle at this time. However, this can be used to
 * produce individual java snippets of simple conditions, and is used on the JVM by the visitor that builds
 * SelectFilters recursively from the filter AST.
 */
public class FilterPrinter implements FilterDescriptor.Visitor {
    private final StringBuilder sb = new StringBuilder();
    private final UnaryOperator<String> stringEscape;

    public FilterPrinter(UnaryOperator<String> stringEscape) {
        this.stringEscape = stringEscape;
    }


    public String print(FilterDescriptor descriptor) {
        assert sb.length() == 0 : "sb.isEmpty()";
        descriptor.accept(this);
        return sb.toString();
    }

    @Override
    public void onAnd(FilterDescriptor descriptor) {
        sb.append("(");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            if (i != 0) {
                sb.append(" && ");
            }
            child.accept(this);
        }
        sb.append(")");
    }

    @Override
    public void onOr(FilterDescriptor descriptor) {
        sb.append("(");
        for (int i = 0; i < descriptor.getChildren().length; i++) {
            FilterDescriptor child = descriptor.getChildren()[i];
            if (i != 0) {
                sb.append(" || ");
            }
            child.accept(this);
        }
        sb.append(")");
    }

    @Override
    public void onNot(FilterDescriptor descriptor) {
        sb.append("!(");
        assert descriptor.getChildren().length == 1;
        descriptor.getChildren()[0].accept(this);
        sb.append(")");
    }

    @Override
    public void onLessThan(FilterDescriptor descriptor) {
        assert descriptor.getChildren().length == 2;
        descriptor.getChildren()[0].accept(this);
        sb.append(" < ");
        descriptor.getChildren()[1].accept(this);
    }

    @Override
    public void onGreaterThan(FilterDescriptor descriptor) {
        assert descriptor.getChildren().length == 2;
        descriptor.getChildren()[0].accept(this);
        sb.append(" > ");
        descriptor.getChildren()[1].accept(this);
    }

    @Override
    public void onLessThanOrEqualTo(FilterDescriptor descriptor) {
        assert descriptor.getChildren().length == 2;
        descriptor.getChildren()[0].accept(this);
        sb.append(" <= ");
        descriptor.getChildren()[1].accept(this);
    }

    @Override
    public void onGreaterThanOrEqualTo(FilterDescriptor descriptor) {
        assert descriptor.getChildren().length == 2;
        descriptor.getChildren()[0].accept(this);
        sb.append(" >= ");
        descriptor.getChildren()[1].accept(this);
    }

    @Override
    public void onEqual(FilterDescriptor descriptor) {
        assert descriptor.getChildren().length == 2;
        descriptor.getChildren()[0].accept(this);
        sb.append(" == ");
        descriptor.getChildren()[1].accept(this);
    }

    @Override
    public void onEqualIgnoreCase(FilterDescriptor descriptor) {
        assert descriptor.getChildren().length == 2;
        descriptor.getChildren()[0].accept(this);
        sb.append(" == (ignore case) ");
        descriptor.getChildren()[1].accept(this);
    }

    @Override
    public void onNotEqual(FilterDescriptor descriptor) {
        assert descriptor.getChildren().length == 2;
        descriptor.getChildren()[0].accept(this);
        sb.append(" != ");
        descriptor.getChildren()[1].accept(this);
    }

    @Override
    public void onNotEqualIgnoreCase(FilterDescriptor descriptor) {
        assert descriptor.getChildren().length == 2;
        descriptor.getChildren()[0].accept(this);
        sb.append(" != (ignore case) ");
        descriptor.getChildren()[1].accept(this);
    }

    @Override
    public void onIn(FilterDescriptor descriptor) {
        descriptor.getChildren()[0].accept(this);
        sb.append(" in ");
        for (int i = 1; i < descriptor.getChildren().length; i++) {
            if (i != 1) {
                sb.append(", ");
            }
            descriptor.getChildren()[i].accept(this);
        }
    }

    @Override
    public void onInIgnoreCase(FilterDescriptor descriptor) {
        descriptor.getChildren()[0].accept(this);
        sb.append(" icase in ");
        for (int i = 1; i < descriptor.getChildren().length; i++) {
            if (i != 1) {
                sb.append(", ");
            }
            descriptor.getChildren()[i].accept(this);
        }
    }

    @Override
    public void onNotIn(FilterDescriptor descriptor) {
        descriptor.getChildren()[0].accept(this);
        sb.append(" not in ");
        for (int i = 1; i < descriptor.getChildren().length; i++) {
            if (i != 1) {
                sb.append(", ");
            }
            descriptor.getChildren()[i].accept(this);
        }
    }

    @Override
    public void onNotInIgnoreCase(FilterDescriptor descriptor) {
        descriptor.getChildren()[0].accept(this);
        sb.append(" icase not in ");
        for (int i = 1; i < descriptor.getChildren().length; i++) {
            if (i != 1) {
                sb.append(", ");
            }
            descriptor.getChildren()[i].accept(this);
        }
    }

    @Override
    public void onIsNull(FilterDescriptor descriptor) {
        sb.append("isNull(");
        assert descriptor.getChildren().length == 1;
        descriptor.getChildren()[0].accept(this);
        sb.append(")");
    }

    @Override
    public void onInvoke(FilterDescriptor descriptor) {
        assert descriptor.getChildren().length >= 1 : "expecting at least one child, even if it is a null value";
        if (descriptor.getChildren()[0] != null) {
            assert descriptor.getChildren()[0].getOperation().expressionKind == Kind.Value;
            descriptor.getChildren()[0].accept(this);
            sb.append(".");
        }
        sb.append(descriptor.getValue());
        sb.append("(");
        for (int i = 1; i < descriptor.getChildren().length; i++) {
            if (i != 1) {
                sb.append(", ");
            }
            descriptor.getChildren()[i].accept(this);
        }
        sb.append(")");
    }

    @Override
    public void onLiteral(FilterDescriptor descriptor) {
        // switch on type, correctly escape any string
        switch (descriptor.getType()) {
            case String:
                sb.append(stringEscape.apply(descriptor.getValue()));
                break;
            case Boolean:
                ParseUtils.parseBoolean(descriptor.getValue());
                sb.append(descriptor.getValue());
                break;
            case Datetime:
                // noinspection ResultOfMethodCallIgnored
                Long.parseLong(descriptor.getValue());
                sb.append("new DBDateTime(").append(descriptor.getValue()).append(")");
                break;
            case Long:
                // noinspection ResultOfMethodCallIgnored
                Long.parseLong(descriptor.getValue());
                sb.append(descriptor.getValue());
                break;
            case Number:
                // noinspection ResultOfMethodCallIgnored
                Double.parseDouble(descriptor.getValue());
                sb.append(descriptor.getValue());
                break;
            case Other:
                // print as a string, not sure how to handle otherwise
                sb.append(stringEscape.apply(descriptor.getValue()));
                break;
        }
    }

    @Override
    public void onReference(FilterDescriptor descriptor) {
        sb.append(descriptor.getValue());
    }

    @Override
    public void onContains(FilterDescriptor descriptor) {
        sb.append("contains(");
        FilterDescriptor col = descriptor.getChildren()[0];
        col.accept(this);
        sb.append(", ");
        FilterDescriptor val = descriptor.getChildren()[1];
        val.accept(this);
        sb.append(")");
    }

    @Override
    public void onContainsIgnoreCase(FilterDescriptor descriptor) {
        sb.append("containsIgnoreCase(");
        FilterDescriptor col = descriptor.getChildren()[0];
        col.accept(this);
        sb.append(", ");
        FilterDescriptor val = descriptor.getChildren()[1];
        val.accept(this);
        sb.append(")");
    }

    @Override
    public void onPattern(FilterDescriptor descriptor) {
        sb.append("pattern(");
        FilterDescriptor col = descriptor.getChildren()[0];
        col.accept(this);
        sb.append(", ");
        FilterDescriptor val = descriptor.getChildren()[1];
        val.accept(this);
        sb.append(")");
    }

    @Override
    public void onPatternIgnoreCase(FilterDescriptor descriptor) {
        sb.append("patternIgnoreCase(");
        FilterDescriptor col = descriptor.getChildren()[0];
        col.accept(this);
        sb.append(", ");
        FilterDescriptor val = descriptor.getChildren()[1];
        val.accept(this);
        sb.append(")");
    }

    @Override
    public void onSearch(FilterDescriptor descriptor) {
        sb.append("searchTableColumns(");
        descriptor.getChildren()[0].accept(this);
        sb.append(")");
    }
}
