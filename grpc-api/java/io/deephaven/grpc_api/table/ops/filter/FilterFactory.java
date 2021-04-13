package io.deephaven.grpc_api.table.ops.filter;

import com.illumon.iris.db.tables.Table;
import com.illumon.iris.db.tables.select.SelectFilterFactory;
import com.illumon.iris.db.tables.utils.DBDateTime;
import com.illumon.iris.db.tables.utils.DBTimeZone;
import com.illumon.iris.db.v2.select.ConjunctiveFilter;
import com.illumon.iris.db.v2.select.DisjunctiveFilter;
import com.illumon.iris.db.v2.select.FormulaParserConfiguration;
import com.illumon.iris.db.v2.select.MatchFilter;
import com.illumon.iris.db.v2.select.RangeConditionFilter;
import com.illumon.iris.db.v2.select.RegexFilter;
import com.illumon.iris.db.v2.select.SelectFilter;
import com.illumon.iris.db.v2.select.SelectNoneFilter;
import com.illumon.iris.db.v2.select.StringContainsFilter;
import io.deephaven.proto.backplane.grpc.CaseSensitivity;
import io.deephaven.proto.backplane.grpc.CompareCondition;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.InvokeCondition;
import io.deephaven.proto.backplane.grpc.IsNullCondition;
import io.deephaven.proto.backplane.grpc.Literal;
import io.deephaven.proto.backplane.grpc.MatchType;
import io.deephaven.proto.backplane.grpc.NotCondition;
import io.deephaven.proto.backplane.grpc.Reference;
import io.deephaven.proto.backplane.grpc.Value;
import org.apache.commons.text.StringEscapeUtils;
import org.jetbrains.annotations.NotNull;

import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

public class FilterFactory implements FilterVisitor<SelectFilter> {
    private final Table table;

    public FilterFactory(Table table) {
        this.table = table;
    }

    public SelectFilter makeFilter(Condition condition) {
        return FilterVisitor.accept(condition, this);
    }

    @Override
    public SelectFilter onAnd(List<Condition> filtersList) {
        final SelectFilter[] items = filtersList.stream()
                .map(cond -> FilterVisitor.accept(cond, this))
                .toArray(SelectFilter[]::new);
        return ConjunctiveFilter.makeConjunctiveFilter(items);
    }

    @Override
    public SelectFilter onOr(List<Condition> filtersList) {
        final SelectFilter[] items = filtersList.stream()
                .map(cond -> FilterVisitor.accept(cond, this))
                .toArray(SelectFilter[]::new);
        return DisjunctiveFilter.makeDisjunctiveFilter(items);
    }

    private SelectFilter generateConditionFilter(Condition filter) {
        FilterPrinter printer = makePrinter();
        return SelectFilterFactory.getExpression(printer.print(filter));
    }

    @NotNull
    private FilterPrinter makePrinter() {
        return new FilterPrinter(str -> "\"" + StringEscapeUtils.escapeJava(str) + "\"");
    }

    private FilterPrinter makePrinterNoEscape() {
        return new FilterPrinter(str -> "\"" + str + "\"");
    }

    @Override
    public SelectFilter onNot(Condition filter) {
        // already must have optimized out any nested operations that we can flatten this into
        return generateConditionFilter(Condition.newBuilder().setNot(NotCondition.newBuilder()
                .setFilter(filter)
                .build()).build());
    }

    @Override
    public SelectFilter onComparison(CompareCondition.CompareOperation operation, Value lhs, Value rhs) {
        switch (operation) {
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return generateNumericConditionFilter(operation, lhs, rhs);
            case EQUALS:
            case NOT_EQUALS:
                throw new IllegalStateException("probably not possible here, needs to be converted to an IN before this point");
            case UNRECOGNIZED:
            default:
                throw new IllegalStateException("Can't handle compare operation " + operation);
        }
    }

    private SelectFilter generateNumericConditionFilter(CompareCondition.CompareOperation operation, Value lhs, Value rhs) {
        boolean invert;
        String columName;
        Literal value;
        if (lhs.getDataCase() == Value.DataCase.LITERAL && rhs.getDataCase() == Value.DataCase.REFERENCE) {
            invert = true;
            value = lhs.getLiteral();
            columName = rhs.getReference().getColumnName();
        } else if (lhs.getDataCase() == Value.DataCase.REFERENCE && rhs.getDataCase() == Value.DataCase.LITERAL) {
            invert = false;
            columName = lhs.getReference().getColumnName();
            value = rhs.getLiteral();
        } else {
            // both are references or literals, handle as a condition filter, not range
            return generateConditionFilter(Condition.newBuilder().setCompare(CompareCondition.newBuilder()
                    .setOperation(operation)
                    .setLhs(lhs)
                    .setRhs(rhs)
                    .build()).build());
        }
        String valueString;
        switch (value.getValueCase()) {
            case STRINGVALUE:
                valueString = value.getStringValue();
                break;
            case DOUBLEVALUE:
                DecimalFormat format = new DecimalFormat("##0");
                format.setDecimalSeparatorAlwaysShown(false);
                format.setGroupingUsed(false);
                valueString = format.format(value.getDoubleValue());
                break;
            case BOOLVALUE:
                valueString = Boolean.toString(value.getBoolValue());
                break;
            case LONGVALUE:
                valueString = Long.toString(value.getLongValue());
                break;
            case NANOTIMEVALUE:
                valueString = Long.toString(value.getNanoTimeValue());
                break;
            case VALUE_NOT_SET:
            default:
                throw new IllegalStateException("Range filter can't handle literal type " + value.getValueCase());
        }
        return new RangeConditionFilter(columName, rangeCondition(operation, invert), valueString, null,
                FormulaParserConfiguration.parser);
    }

    private com.illumon.iris.gui.table.filters.Condition rangeCondition(CompareCondition.CompareOperation operation, boolean invert) {
        switch (operation) {
            case LESS_THAN:
                return invert ? com.illumon.iris.gui.table.filters.Condition.GREATER_THAN_OR_EQUAL : com.illumon.iris.gui.table.filters.Condition.LESS_THAN;
            case LESS_THAN_OR_EQUAL:
                return invert ? com.illumon.iris.gui.table.filters.Condition.GREATER_THAN : com.illumon.iris.gui.table.filters.Condition.LESS_THAN_OR_EQUAL;
            case GREATER_THAN:
                return invert ? com.illumon.iris.gui.table.filters.Condition.LESS_THAN_OR_EQUAL : com.illumon.iris.gui.table.filters.Condition.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return invert ? com.illumon.iris.gui.table.filters.Condition.LESS_THAN : com.illumon.iris.gui.table.filters.Condition.GREATER_THAN_OR_EQUAL;
            case EQUALS:
            case NOT_EQUALS:
            case UNRECOGNIZED:
            default:
                throw new IllegalStateException("Can't handle compare operation " + operation + " in range operation");
        }
    }

    @Override
    public SelectFilter onIn(Value target, List<Value> candidatesList, CaseSensitivity caseSensitivity, MatchType matchType) {
        assert target.getDataCase() == Value.DataCase.REFERENCE;
        Reference reference = target.getReference();
        String[] values = new String[candidatesList.size()];
        for (int i = 1; i < candidatesList.size(); i++) {
            Value d = candidatesList.get(i);
            assert d.getDataCase() == Value.DataCase.LITERAL;
            Literal literal = d.getLiteral();
            // all other literals get created from a toString except DateTime
            if (literal.getValueCase() == Literal.ValueCase.NANOTIMEVALUE) {
                values[i - 1] = "'" + new DBDateTime(literal.getNanoTimeValue()).toString(DBTimeZone.TZ_DEFAULT) + "'";
            } else {
                FilterPrinter printer = makePrinterNoEscape();
                values[i - 1] = printer.print(literal);
            }
        }
        return new MatchFilter(caseSensitivity(caseSensitivity), matchType(matchType), reference.getColumnName(), values);
    }

    private MatchFilter.CaseSensitivity caseSensitivity(CaseSensitivity caseSensitivity) {
        switch (caseSensitivity) {
            case MATCH_CASE:
                return MatchFilter.CaseSensitivity.MatchCase;
            case IGNORE_CASE:
                return MatchFilter.CaseSensitivity.IgnoreCase;
            case UNRECOGNIZED:
            default:
                throw new IllegalStateException("Can't handle compare case sensitivity " + caseSensitivity);
        }
    }

    private MatchFilter.MatchType matchType(MatchType matchType) {
        switch (matchType) {
            case REGULAR:
                return MatchFilter.MatchType.Regular;
            case INVERTED:
                return MatchFilter.MatchType.Inverted;
            case UNRECOGNIZED:
            default:
                throw new IllegalStateException("Can't handle compare match type " + matchType);
        }
    }

    @Override
    public SelectFilter onIsNull(Reference reference) {
        return generateConditionFilter(Condition.newBuilder().setIsNull(IsNullCondition.newBuilder()
                .setReference(reference)
                .build()).build());
    }

    @Override
    public SelectFilter onInvoke(String method, Value target, List<Value> argumentsList) {
        return generateConditionFilter(Condition.newBuilder().setInvoke(InvokeCondition.newBuilder()
                .setMethod(method)
                .setTarget(target)
                .addAllArguments(argumentsList)
                .build()).build());
    }

    @Override
    public SelectFilter onContains(Reference reference, String searchString, CaseSensitivity caseSensitivity, MatchType matchType) {
        return new StringContainsFilter(caseSensitivity(caseSensitivity), matchType(matchType), reference.getColumnName(), searchString);
    }

    @Override
    public SelectFilter onMatches(Reference reference, String regex, CaseSensitivity caseSensitivity, MatchType matchType) {
        return new RegexFilter(caseSensitivity(caseSensitivity), matchType(matchType), reference.getColumnName(), regex);
    }

    @Override
    public SelectFilter onSearch(String searchString, List<Reference> optionalReferencesList) {
        final Set<String> columnNames = optionalReferencesList.stream().map(Reference::getColumnName).collect(Collectors.toSet());
        SelectFilter[] selectFilters = SelectFilterFactory.expandQuickFilter(table, searchString, columnNames);
        if (selectFilters == null || selectFilters.length == 0) {
            return SelectNoneFilter.INSTANCE;
        }
        return DisjunctiveFilter.makeDisjunctiveFilter(selectFilters);
    }
}
