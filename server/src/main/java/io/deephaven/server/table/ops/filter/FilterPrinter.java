package io.deephaven.server.table.ops.filter;

import io.deephaven.proto.backplane.grpc.CaseSensitivity;
import io.deephaven.proto.backplane.grpc.CompareCondition;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.Literal;
import io.deephaven.proto.backplane.grpc.MatchType;
import io.deephaven.proto.backplane.grpc.Reference;
import io.deephaven.proto.backplane.grpc.Value;
import org.apache.commons.text.StringEscapeUtils;

import java.util.List;

public class FilterPrinter implements FilterVisitor<Void> {
    private final StringBuilder sb = new StringBuilder();
    private final boolean escapeStrings;

    public static String print(Condition condition) {
        FilterPrinter visitor = new FilterPrinter(true);
        FilterVisitor.accept(condition, visitor);

        return visitor.sb.toString();
    }

    public static String printNoEscape(Literal literal) {
        FilterPrinter visitor = new FilterPrinter(false);
        visitor.onLiteral(literal);

        return "\"" + visitor.sb.toString() + "\"";
    }

    public FilterPrinter(boolean escapeStrings) {
        this.escapeStrings = escapeStrings;
    }

    private String stringEscape(String str) {
        if (escapeStrings) {
            return "\"" + StringEscapeUtils.escapeJava(str) + "\"";
        }
        return str;
    }

    @Override
    public Void onAnd(List<Condition> filtersList) {
        if (filtersList.isEmpty()) {
            // should be pruned earlier
            return null;
        }
        if (filtersList.size() == 1) {
            // should have been stripped earlier
            FilterVisitor.accept(filtersList.get(0), this);
            return null;
        }
        sb.append("(");

        // at least 2 entries, handle the first, then the rest
        FilterVisitor.accept(filtersList.get(0), this);
        filtersList.stream().skip(1).forEach(condition -> {
            sb.append(" && ");
            FilterVisitor.accept(condition, this);
        });

        sb.append(")");
        return null;
    }

    @Override
    public Void onOr(List<Condition> filtersList) {
        if (filtersList.isEmpty()) {
            // should be pruned earlier
            return null;
        }
        if (filtersList.size() == 1) {
            // should have been stripped earlier
            FilterVisitor.accept(filtersList.get(0), this);
            return null;
        }
        sb.append("(");

        // at least 2 entries, handle the first, then the rest
        FilterVisitor.accept(filtersList.get(0), this);
        filtersList.stream().skip(1).forEach(condition -> {
            sb.append(" || ");
            FilterVisitor.accept(condition, this);
        });

        sb.append(")");
        return null;
    }

    @Override
    public Void onNot(Condition filter) {
        sb.append("!(");
        FilterVisitor.accept(filter, this);
        sb.append(")");
        return null;
    }

    @Override
    public Void onComparison(CompareCondition.CompareOperation operation, CaseSensitivity caseSensitivity, Value lhs,
            Value rhs) {
        accept(lhs);
        switch (operation) {
            case LESS_THAN:
                sb.append(" < ");
                break;
            case LESS_THAN_OR_EQUAL:
                sb.append(" <= ");
                break;
            case GREATER_THAN:
                sb.append(" > ");
                break;
            case GREATER_THAN_OR_EQUAL:
                sb.append(" >= ");
                break;
            case EQUALS:
                sb.append(" == ");
                if (caseSensitivity == CaseSensitivity.IGNORE_CASE) {
                    sb.append("(ignore case)");
                }
                break;
            case NOT_EQUALS:
                sb.append(" != ");
                if (caseSensitivity == CaseSensitivity.IGNORE_CASE) {
                    sb.append("(ignore case)");
                }
                break;
            case UNRECOGNIZED:
            default:
                throw new UnsupportedOperationException("Unknown operation " + operation);
        }
        accept(rhs);
        return null;
    }

    @Override
    public Void onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity, MatchType matchType) {
        if (candidatesList.isEmpty()) {
            // should have already been pruned
            return null;
        }
        accept(target);
        if (caseSensitivity == CaseSensitivity.IGNORE_CASE) {
            sb.append(" icase");
        }
        if (matchType == MatchType.INVERTED) {
            sb.append(" not");
        }
        sb.append(" in ");
        accept(candidatesList.get(0));
        for (int i = 1; i < candidatesList.size(); i++) {
            sb.append(", ");
            accept(candidatesList.get(i));
        }
        return null;
    }

    @Override
    public Void onIsNull(Reference reference) {
        sb.append("isNull(");
        onReference(reference);
        sb.append(")");
        return null;
    }

    @Override
    public Void onInvoke(String method, Value target, List<Value> argumentsList) {
        if (target != null) {
            accept(target);
            sb.append(".");
        }
        sb.append(method).append("(");
        for (int i = 0; i < argumentsList.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            accept(argumentsList.get(i));
        }
        sb.append(")");
        return null;
    }

    @Override
    public Void onContains(Reference reference, String searchString, CaseSensitivity caseSensitivity,
            MatchType matchType) {
        if (matchType == MatchType.INVERTED) {
            sb.append("!");
        }
        sb.append("contains");
        if (caseSensitivity == CaseSensitivity.IGNORE_CASE) {
            sb.append("IgnoreCase");
        }
        sb.append("(");
        onReference(reference);
        sb.append(",");
        sb.append(stringEscape(searchString));
        sb.append(")");
        return null;
    }

    @Override
    public Void onMatches(Reference reference, String regex, CaseSensitivity caseSensitivity, MatchType matchType) {
        if (matchType == MatchType.INVERTED) {
            sb.append("!");
        }
        sb.append("matches");
        if (caseSensitivity == CaseSensitivity.IGNORE_CASE) {
            sb.append("IgnoreCase");
        }
        sb.append("(");
        onReference(reference);
        sb.append(",");
        sb.append(stringEscape(regex));
        sb.append(")");
        return null;
    }

    @Override
    public Void onSearch(String searchString, List<Reference> optionalReferencesList) {
        sb.append("searchTableColumns(");
        sb.append(stringEscape(searchString));
        for (Reference reference : optionalReferencesList) {
            sb.append(",");
            onReference(reference);
        }
        sb.append(")");
        return null;
    }

    private void accept(Value value) {
        switch (value.getDataCase()) {
            case REFERENCE:
                onReference(value.getReference());
                break;
            case LITERAL:
                onLiteral(value.getLiteral());
                break;
            case DATA_NOT_SET:
            default:
                throw new UnsupportedOperationException("Unknown value " + value);
        }
    }

    private void onReference(Reference reference) {
        sb.append(reference.getColumnName());
    }

    private void onLiteral(Literal literal) {
        switch (literal.getValueCase()) {
            case STRING_VALUE:
                sb.append(stringEscape(literal.getStringValue()));
                break;
            case DOUBLE_VALUE:
                double doubleVal = literal.getDoubleValue();
                if (doubleVal == Double.NEGATIVE_INFINITY) {
                    sb.append("Double.NEGATIVE_INFINITY");
                } else if (doubleVal == Double.POSITIVE_INFINITY) {
                    sb.append("Double.POSITIVE_INFINITY");
                } else if (Double.isNaN(doubleVal)) {
                    sb.append("Double.NaN");
                } else {
                    // Cast the double value to a long, then test to see if they actually compare to the same
                    // value - if they do not, we have some decimal value and need the entire double to be
                    // appended, if they do, then we just append the integer instead.
                    long longVal = (long) doubleVal;
                    if (longVal - doubleVal != 0) {
                        // has a decimal value
                        sb.append(doubleVal);
                    } else {
                        sb.append(longVal);
                    }
                }
                break;
            case BOOL_VALUE:
                sb.append(literal.getBoolValue());
                break;
            case LONG_VALUE:
                sb.append(literal.getLongValue());
                break;
            case NANO_TIME_VALUE:
                sb.append("new DateTime(").append(literal.getNanoTimeValue()).append(")");
                break;
            case VALUE_NOT_SET:
            default:
                throw new UnsupportedOperationException("Unknown literal " + literal);
        }
    }
}
