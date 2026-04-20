//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.filter;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import io.deephaven.proto.backplane.grpc.CaseSensitivity;
import io.deephaven.proto.backplane.grpc.CompareCondition;
import io.deephaven.proto.backplane.grpc.Condition;
import io.deephaven.proto.backplane.grpc.ContainsCondition;
import io.deephaven.proto.backplane.grpc.InCondition;
import io.deephaven.proto.backplane.grpc.InvokeCondition;
import io.deephaven.proto.backplane.grpc.IsNullCondition;
import io.deephaven.proto.backplane.grpc.Literal;
import io.deephaven.proto.backplane.grpc.MatchType;
import io.deephaven.proto.backplane.grpc.MatchesCondition;
import io.deephaven.proto.backplane.grpc.Reference;
import io.deephaven.proto.backplane.grpc.Value;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.DateWrapper;
import io.deephaven.web.client.api.LongWrapper;
import io.deephaven.web.client.api.TableData;
import io.deephaven.web.client.api.i18n.JsTimeZone;
import javaemul.internal.annotations.DoNotAutobox;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Any;
import jsinterop.base.Js;

import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Describes data that can be filtered - either a column reference or a literal value. Used this way, the type of a
 * value can be specified so that values which are ambiguous or not well supported in JS will not be confused with
 * Strings or imprecise numbers (e.g., nanosecond-precision date values). Additionally, once wrapped in this way,
 * methods can be called on these value literal instances. These instances are immutable - any method called on them
 * returns a new instance.
 */
@JsType(namespace = "dh")
public class FilterValue {
    protected final Value descriptor;

    /**
     * Constructs a string for the filter API from the given parameter.
     *
     * @param input
     * @return
     */
    @JsMethod(namespace = "dh.FilterValue")
    public static FilterValue ofString(@TsTypeRef(Any.class) Object input) {
        Objects.requireNonNull(input);
        final String string;
        if (Js.typeof(input).equals("string")) {
            string = (String) input;
        } else {
            string = input.toString();
        }
        Literal lit = Literal.newBuilder()
                .setStringValue(string)
                .build();
        return new FilterValue(lit);
    }

    @TsUnion
    @JsType(name = "?", namespace = JsPackage.GLOBAL, isNative = true)
    public interface OfNumberUnionParam {
        @JsOverlay
        static OfNumberUnionParam of(@DoNotAutobox Object value) {
            return Js.cast(value);
        }

        @JsOverlay
        default boolean isLongWrapper() {
            return this instanceof LongWrapper;
        }

        @JsOverlay
        default boolean isNumber() {
            return (Object) this instanceof Double;
        }

        @TsUnionMember
        @JsOverlay
        default LongWrapper asLongWrapper() {
            return Js.cast(this);
        }

        @TsUnionMember
        @JsOverlay
        default double asNumber() {
            return Js.asDouble(this);
        }
    }

    @JsIgnore
    public static FilterValue ofNumber(double input) {
        return ofNumber(Js.cast(input));
    }

    /**
     * Constructs a number for the filter API from the given parameter. Can also be used on the values returned from
     * {@link io.deephaven.web.client.api.TableData.Row#get(TableData.RowPositionUnion)} for DateTime values. To create
     * a filter with a date, use <b>dh.DateWrapper.ofJsDate</b> or
     * {@link io.deephaven.web.client.api.i18n.JsDateTimeFormat#parse(String, JsTimeZone)}. To create a filter with a
     * 64-bit long integer, use {@link LongWrapper#ofString(String)}.
     *
     * @param input the number to wrap as a FilterValue
     * @return an immutable FilterValue that can be built into a filter
     */
    public static FilterValue ofNumber(OfNumberUnionParam input) {
        Objects.requireNonNull(input);
        if (input.isLongWrapper()) {
            LongWrapper value = input.asLongWrapper();
            if (value instanceof DateWrapper) {
                Literal lit = Literal.newBuilder()
                        .setNanoTimeValue(((DateWrapper) input).getWrapped())
                        .build();
                return new FilterValue(lit);
            } else {
                Literal lit = Literal.newBuilder()
                        .setLongValue(((LongWrapper) input).getWrapped())
                        .build();
                return new FilterValue(lit);
            }
        } else if (input.isNumber()) {
            Literal lit = Literal.newBuilder()
                    .setDoubleValue(input.asNumber())
                    .build();
            return new FilterValue(lit);
        } else {
            // not sure what the input is, try to toString(), then parse to Double, and use that
            Literal lit = Literal.newBuilder()
                    .setDoubleValue(Double.parseDouble(input.toString()))
                    .build();
            return new FilterValue(lit);
        }
    }

    /**
     * Constructs a boolean for the filter API from the given parameter.
     *
     * @param b
     * @return
     */
    @JsMethod(namespace = "dh.FilterValue")
    public static FilterValue ofBoolean(Boolean b) {
        Objects.requireNonNull(b);

        Literal lit = Literal.newBuilder()
                .setBoolValue(b)
                .build();
        return new FilterValue(lit);
    }

    private FilterValue(Literal literal) {
        descriptor = Value.newBuilder().setLiteral(literal).build();
    }

    @JsIgnore
    public FilterValue(Column column) {
        descriptor = Value.newBuilder()
                .setReference(Reference.newBuilder()
                        .setColumnName(column.getName()))
                .build();
    }

    @JsIgnore // hidden until implemented
    public FilterValue abs() {
        // TODO just sugar for invoke? special operation?
        return this;
    }

    /**
     * A filter condition checking if the current value is equal to the given parameter.
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition eq(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.EQUALS);
    }

    private FilterCondition makeCompare(FilterValue term, CompareCondition.CompareOperation operation) {
        CompareCondition compare = CompareCondition.newBuilder()
                .setLhs(descriptor)
                .setRhs(term.descriptor)

                .setOperation(operation)
                .build();

        Condition c = Condition.newBuilder()
                .setCompare(compare)
                .build();

        return FilterCondition.createAndValidate(c);
    }

    /**
     * A filter condition checking if the current value is equal to the given parameter, ignoring differences of upper
     * vs lower case.
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition eqIgnoreCase(FilterValue term) {
        return inIgnoreCase(new FilterValue[] {term});
    }

    /**
     * a filter condition checking if the current value is not equal to the given parameter
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition notEq(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.NOT_EQUALS);
    }

    /**
     * A filter condition checking if the current value is not equal to the given parameter, ignoring differences of
     * upper vs lower case.
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition notEqIgnoreCase(FilterValue term) {
        return notInIgnoreCase(new FilterValue[] {term});
    }

    /**
     * A filter condition checking if the current value is greater than the given parameter.
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition greaterThan(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.GREATER_THAN);
    }

    /**
     * A filter condition checking if the current value is less than the given parameter.
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition lessThan(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.LESS_THAN);
    }

    /**
     * A filter condition checking if the current value is greater than or equal to the given parameter.
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition greaterThanOrEqualTo(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.GREATER_THAN_OR_EQUAL);
    }

    /**
     * A filter condition checking if the current value is less than or equal to the given parameter.
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition lessThanOrEqualTo(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.LESS_THAN_OR_EQUAL);
    }

    /**
     * A filter condition checking if the current value is in the given set of values.
     * 
     * @param terms
     * @return {@link FilterCondition}
     */
    public FilterCondition in(FilterValue[] terms) {
        return makeIn(terms, MatchType.REGULAR, CaseSensitivity.MATCH_CASE);
    }

    private FilterCondition makeIn(FilterValue[] terms, MatchType matchType, CaseSensitivity casesensitivity) {
        InCondition value = InCondition.newBuilder()
                .setTarget(descriptor)
                .setMatchType(matchType)
                .setCaseSensitivity(casesensitivity)
                .addAllCandidates(Arrays.stream(terms).map(v -> v.descriptor).collect(Collectors.toList()))
                .build();

        Condition c = Condition.newBuilder()
                .setIn(value)
                .build();
        return FilterCondition.createAndValidate(c);
    }

    /**
     * A filter condition checking if the current value is in the given set of values, ignoring differences of upper vs
     * lower case.
     * 
     * @param terms
     * @return {@link FilterCondition}
     */
    public FilterCondition inIgnoreCase(FilterValue[] terms) {
        return makeIn(terms, MatchType.REGULAR, CaseSensitivity.IGNORE_CASE);
    }

    /**
     * A filter condition checking that the current value is not in the given set of values.
     * 
     * @param terms
     * @return {@link FilterCondition}
     */
    public FilterCondition notIn(FilterValue[] terms) {
        return makeIn(terms, MatchType.INVERTED, CaseSensitivity.MATCH_CASE);
    }

    /**
     * A filter condition checking that the current value is not in the given set of values, ignoring differences of
     * upper vs lower case.
     * 
     * @param terms
     * @return {@link FilterCondition}
     */
    public FilterCondition notInIgnoreCase(FilterValue[] terms) {
        return makeIn(terms, MatchType.INVERTED, CaseSensitivity.IGNORE_CASE);
    }

    /**
     * A filter condition checking if the given value contains the given string value.
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition contains(FilterValue term) {
        return makeContains(term, CaseSensitivity.MATCH_CASE);
    }

    /**
     * A filter condition checking if the given value contains the given string value, ignoring differences of upper vs
     * lower case.
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition containsIgnoreCase(FilterValue term) {
        return makeContains(term, CaseSensitivity.IGNORE_CASE);
    }

    private FilterCondition makeContains(FilterValue term, CaseSensitivity caseSensitivity) {
        ContainsCondition contains = ContainsCondition.newBuilder()
                .setReference(this.descriptor.getReference())
                .setSearchString(term.descriptor.getLiteral().getStringValue())
                .setCaseSensitivity(caseSensitivity)
                .build();

        Condition c = Condition.newBuilder()
                .setContains(contains)
                .build();
        return FilterCondition.createAndValidate(c);
    }

    /**
     * A filter condition checking if the given value matches the provided regular expressions string. Regex patterns
     * use Java regex syntax.
     * 
     * @param pattern
     * @return {@link FilterCondition}
     */
    public FilterCondition matches(FilterValue pattern) {
        return makeMatches(pattern, CaseSensitivity.MATCH_CASE);
    }

    /**
     * A filter condition checking if the given value matches the provided regular expressions string, ignoring
     * differences of upper vs lower case. Regex patterns use Java regex syntax.
     * 
     * @param pattern
     * @return {@link FilterCondition}
     */
    public FilterCondition matchesIgnoreCase(FilterValue pattern) {
        return makeMatches(pattern, CaseSensitivity.IGNORE_CASE);
    }

    private FilterCondition makeMatches(FilterValue term, CaseSensitivity caseSensitivity) {
        MatchesCondition contains = MatchesCondition.newBuilder()
                .setReference(this.descriptor.getReference())
                .setRegex(term.descriptor.getLiteral().getStringValue())
                .setCaseSensitivity(caseSensitivity)
                .build();

        Condition c = Condition.newBuilder()
                .setMatches(contains)
                .build();
        return FilterCondition.createAndValidate(c);
    }

    /**
     * A filter condition checking if the current value is a true boolean.
     * 
     * @return {@link FilterCondition}
     */
    public FilterCondition isTrue() {
        return eq(FilterValue.ofBoolean(true));
    }

    /**
     * A filter condition checking if the current value is a false boolean.
     * 
     * @return {@link FilterCondition}
     */
    public FilterCondition isFalse() {
        return eq(FilterValue.ofBoolean(false));
    }

    /**
     * A filter condition checking if the current value is a null value.
     * 
     * @return {@link FilterCondition}
     */
    public FilterCondition isNull() {
        IsNullCondition isNull = IsNullCondition.newBuilder()
                .setReference(this.descriptor.getReference())
                .build();

        Condition c = Condition.newBuilder()
                .setIsNull(isNull)
                .build();
        return FilterCondition.createAndValidate(c);
    }

    /**
     * A filter condition invoking the given method on the current value, with the given parameters. Currently supported
     * functions that can be invoked on a String:
     * <ul>
     * <li>{@code startsWith}: Returns {@code true} if the current string value starts with the supplied string
     * argument.</li>
     * <li>{@code endsWith}: Returns {@code true} if the current string value ends with the supplied string
     * argument.</li>
     * <li>{@code matches}: Returns {@code true} if the current string value matches the supplied string argument used
     * as a Java regular expression.</li>
     * <li>{@code contains}: Returns {@code true} if the current string value contains the supplied string argument.
     * <p>
     * When invoking against a constant, this should be avoided in favor of {@link FilterValue#contains}.
     * </p>
     * </li>
     * </ul>
     *
     * @param method
     * @param args
     * @return
     */
    public FilterCondition invoke(String method, FilterValue... args) {
        InvokeCondition invoke = InvokeCondition.newBuilder()
                .setMethod(method)
                .setTarget(descriptor)
                .addAllArguments(Arrays.stream(args).map(v -> v.descriptor).collect(Collectors.toList()))
                .build();

        Condition c = Condition.newBuilder()
                .setInvoke(invoke)
                .build();
        return FilterCondition.createAndValidate(c);
    }

    @Override
    public String toString() {
        // TODO (deephaven-core#723) implement a readable tostring rather than turning the pb object into a string
        return descriptor.toString();
    }
}
