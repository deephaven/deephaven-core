//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.filter;

import elemental2.core.JsArray;
import io.deephaven.proto.backplane.grpc.AndCondition;
import io.deephaven.proto.backplane.grpc.CompareCondition;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.InCondition;
import io.deephaven.proto.backplane.grpc.InvokeCondition;
import io.deephaven.proto.backplane.grpc.NotCondition;
import io.deephaven.proto.backplane.grpc.OrCondition;
import io.deephaven.proto.backplane.grpc.SearchCondition;
import io.deephaven.proto.backplane.grpc.Value;
import io.deephaven.web.client.api.Column;
import jsinterop.annotations.*;

import java.util.Arrays;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Describes a filter which can be applied to a table. Replacing these instances may be more expensive than reusing
 * them. These instances are immutable - all operations that compose them to build bigger expressions return a new
 * instance.
 */
@JsType(namespace = "dh")
public class FilterCondition {

    @JsIgnore
    private final Condition descriptor;

    /**
     * a filter condition invoking a static function with the given parameters. Currently supported Deephaven static
     * functions:
     * <ul>
     * <li>{@code inRange}: Given three comparable values, returns {@code true} if the first value falls between the
     * second and third values, which are the inclusive lower and upper bounds of the range. Returns {@code false} if
     * the first value is null.</li>
     * <li>{@code isInf}: Returns {@code true} if the given number is <i>infinity</i>.</li>
     * <li>{@code isNaN}: Returns {@code true} if the given number is <i>not a number</i>.</li>
     * <li>{@code in}: Returns {@code true} if the first value is equal to any of the values that follow it. The
     * candidate values are passed as individual arguments rather than as a single array. Typically the
     * {@link FilterValue#in(FilterValue[])} method should be used instead.</li>
     * </ul>
     * <p>
     * Because no target value is supplied, only static functions can be invoked here. To invoke a method <i>on</i> a
     * value - such as {@code startsWith} or {@code endsWith} on a string - use
     * {@link FilterValue#invoke(String, FilterValue...)} instead. For regular expression and substring matching, prefer
     * the dedicated {@link FilterValue#matches(FilterValue)} and {@link FilterValue#contains(FilterValue)} methods (or
     * their case-insensitive variants).
     *
     * @param function
     * @param args
     * @return {@link FilterCondition}
     */
    @JsMethod(namespace = "dh.FilterCondition")
    public static FilterCondition invoke(String function, FilterValue... args) {
        InvokeCondition invoke = InvokeCondition.newBuilder()
                .setMethod(function)
                .addAllArguments(Arrays.stream(args).map(v -> v.descriptor).collect(Collectors.toList()))
                .build();

        Condition c = Condition.newBuilder()
                .setInvoke(invoke)
                .build();

        return createAndValidate(c);
    }

    /**
     * A filter condition which will check if the given value can be found in any supported column on whatever table
     * this {@link FilterCondition} is passed to. This {@link FilterCondition} is somewhat unique in that it need not be
     * given a column instance, but will adapt to any table. On numeric columns, with a value passed in which can be
     * parsed as a number, the column will be filtered to numbers which equal, or can be "rounded" effectively to this
     * number. On String columns, the given value will match any column which contains this string in a case-insensitive
     * search. An optional second argument can be passed, an array of {@link FilterValue} from the columns to limit this
     * search to (see {@link Column#filter}).
     * 
     * @param value
     * @param columns
     * @return {@link FilterCondition}
     */
    @JsMethod(namespace = "dh.FilterCondition")
    public static FilterCondition search(FilterValue value, @JsOptional @JsNullable FilterValue[] columns) {
        SearchCondition.Builder search = SearchCondition.newBuilder();
        search.setSearchString(value.descriptor.getLiteral().getStringValue());
        if (columns != null) {
            search.addAllOptionalReferences(
                    Arrays.stream(columns).map(v -> v.descriptor.getReference()).collect(Collectors.toList()));
        }

        Condition c = Condition.newBuilder()
                .setSearch(search)
                .build();

        return createAndValidate(c);
    }

    @JsIgnore
    public FilterCondition(Condition descriptor) {
        this.descriptor = descriptor;
    }

    /**
     * The opposite of this condition.
     * 
     * @return {@link FilterCondition}
     */
    public FilterCondition not() {
        NotCondition not = NotCondition.newBuilder()
                .setFilter(descriptor)
                .build();

        Condition c = Condition.newBuilder()
                .setNot(not)
                .build();

        return createAndValidate(c);
    }

    @JsIgnore
    protected static FilterCondition createAndValidate(Condition descriptor) {
        // TODO (deephaven-core#723) re-introduce client-side validation so that a client knows right away when
        // they build something invalid
        return new FilterCondition(descriptor);
    }

    /**
     * A condition representing the current condition logically ANDed with the other parameters.
     * 
     * @param filters
     * @return {@link FilterCondition}
     */
    public FilterCondition and(FilterCondition... filters) {
        AndCondition and = AndCondition.newBuilder()
                .addAllFilters(Stream.concat(Stream.of(descriptor), Arrays.stream(filters).map(v -> v.descriptor))
                        .collect(Collectors.toList()))
                .build();

        Condition c = Condition.newBuilder()
                .setAnd(and)
                .build();

        return createAndValidate(c);
    }


    /**
     * A condition representing the current condition logically ORed with the other parameters.
     * 
     * @param filters
     * @return {@link FilterCondition}
     */
    public FilterCondition or(FilterCondition... filters) {
        OrCondition or = OrCondition.newBuilder()
                .addAllFilters(Stream.concat(Stream.of(descriptor), Arrays.stream(filters).map(v -> v.descriptor))
                        .collect(Collectors.toList()))
                .build();

        Condition c = Condition.newBuilder()
                .setOr(or)
                .build();

        return createAndValidate(c);
    }

    @JsIgnore
    public Condition makeDescriptor() {
        // these are immutable, so we can just return the instance
        return descriptor;
    }

    /**
     * A string suitable for debugging showing the details of this condition.
     *
     * @return String.
     */
    @JsMethod
    public String toString() {
        return toString(descriptor);
    }

    private static String toString(Condition descriptor) {
        Function<Condition, String> condString = FilterCondition::toString;
        Function<Value, String> valueString = FilterCondition::toString;
        return switch (descriptor.getDataCase()) {
            case AND -> descriptor.getAnd().getFiltersList().stream()
                    .map(condString)
                    .collect(Collectors.joining(" && "));
            case OR -> descriptor.getOr().getFiltersList().stream()
                    .map(condString)
                    .collect(Collectors.joining(" || "));
            case NOT -> "!(" + toString(descriptor.getNot().getFilter()) + ")";
            case COMPARE -> {
                CompareCondition compare = descriptor.getCompare();
                yield toString(compare.getLhs()) + " " + compare.getCaseSensitivity() + " " + compare.getOperation()
                        + " " + toString(compare.getRhs());
            }
            case IN -> {
                InCondition in = descriptor.getIn();
                yield toString(in.getTarget()) + " " + in.getMatchType() + " " + in.getCaseSensitivity() + " "
                        + in.getNanComparison() + " in "
                        + in.getCandidatesList().stream().map(valueString).collect(Collectors.joining(", "));
            }
            case INVOKE -> {
                InvokeCondition invoke = descriptor.getInvoke();
                if (invoke.hasTarget()) {
                    yield toString(invoke.getTarget()) + "." + invoke.getMethod() + "("
                            + invoke.getArgumentsList().stream().map(valueString).collect(Collectors.joining(", "))
                            + ")";
                }
                yield invoke.getMethod() + "("
                        + invoke.getArgumentsList().stream().map(valueString).collect(Collectors.joining(", ")) + ")";
            }
            case IS_NULL -> "isNull(" + descriptor.getIsNull().getReference().getColumnName() + ")";
            case MATCHES -> "matches(" + descriptor.getMatches().getReference().getColumnName() + ", "
                    + descriptor.getMatches().getRegex() + ", " + descriptor.getMatches().getCaseSensitivity() + ", "
                    + descriptor.getMatches().getMatchType() + ")";
            case CONTAINS -> "contains(" + descriptor.getContains().getReference().getColumnName() + ", "
                    + descriptor.getContains().getSearchString() + ", " + descriptor.getContains().getCaseSensitivity()
                    + ", " + descriptor.getContains().getMatchType() + ")";
            case SEARCH -> "searchTableColumns(" + descriptor.getSearch().getSearchString() + ", "
                    + descriptor.getSearch().getOptionalReferencesList() + ")";
            case IS_NAN -> "isNaN(" + descriptor.getIsNan().getReference().getColumnName() + ")";
            case DATA_NOT_SET -> "unknown";
        };
    }

    private static String toString(Value value) {
        if (value.hasReference()) {
            return value.getReference().getColumnName();
        }
        return value.getLiteral().getStringValue();
    }

    @JsProperty
    public JsArray<Column> getColumns() {
        return new JsArray<>();
    }

    @Override
    @JsIgnore
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final FilterCondition that = (FilterCondition) o;

        return descriptor.equals(that.descriptor);
    }

    @Override
    @JsIgnore
    public int hashCode() {
        return descriptor.hashCode();
    }
}
