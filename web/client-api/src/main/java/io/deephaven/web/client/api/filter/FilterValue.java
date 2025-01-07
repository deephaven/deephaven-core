//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.filter;

import com.vertispan.tsdefs.annotations.TsTypeRef;
import com.vertispan.tsdefs.annotations.TsUnion;
import com.vertispan.tsdefs.annotations.TsUnionMember;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.Table_pb;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.CompareCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.Condition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ContainsCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.InCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.InvokeCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.IsNullCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.Literal;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.MatchesCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.Reference;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.Value;
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

/**
 * Describes data that can be filtered, either a column reference or a literal value. Used this way, the type of a value
 * can be specified so that values which are ambiguous or not well supported in JS will not be confused with Strings or
 * imprecise numbers (e.g., nanosecond-precision date values). Additionally, once wrapped in this way, methods can be
 * called on these value literal instances. These instances are immutable - any method called on them returns a new
 * instance.
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
        Literal lit = new Literal();
        lit.setStringValue(string);
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
                Literal lit = new Literal();
                lit.setNanoTimeValue(((DateWrapper) input).valueOf());
                return new FilterValue(lit);
            } else {
                Literal lit = new Literal();
                lit.setLongValue(((LongWrapper) input).valueOf());
                return new FilterValue(lit);
            }
        } else if (input.isNumber()) {
            Literal lit = new Literal();
            lit.setDoubleValue(input.asNumber());
            return new FilterValue(lit);
        } else {
            // not sure what the input is, try to toString(), then parse to Double, and use that
            Literal lit = new Literal();
            lit.setDoubleValue(Double.parseDouble(input.toString()));
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

        Literal lit = new Literal();
        lit.setBoolValue(b);
        return new FilterValue(lit);
    }

    private FilterValue(Literal literal) {
        descriptor = new Value();
        descriptor.setLiteral(literal);
    }

    @JsIgnore
    public FilterValue(Column column) {
        Reference ref = new Reference();
        ref.setColumnName(column.getName());

        descriptor = new Value();
        descriptor.setReference(ref);
    }

    @JsIgnore // hidden until implemented
    public FilterValue abs() {
        // TODO just sugar for invoke? special operation?
        return this;
    }

    /**
     * a filter condition checking if the current value is equal to the given parameter
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition eq(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.getEQUALS());
    }

    private FilterCondition makeCompare(FilterValue term, double operation) {
        CompareCondition compare = new CompareCondition();
        compare.setLhs(descriptor);
        compare.setRhs(term.descriptor);

        compare.setOperation(operation);

        Condition c = new Condition();
        c.setCompare(compare);

        return FilterCondition.createAndValidate(c);
    }

    /**
     * a filter condition checking if the current value is equal to the given parameter, ignoring differences of upper
     * vs lower case
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
        return makeCompare(term, CompareCondition.CompareOperation.getNOT_EQUALS());
    }

    /**
     * a filter condition checking if the current value is not equal to the given parameter, ignoring differences of
     * upper vs lower case
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition notEqIgnoreCase(FilterValue term) {
        return notInIgnoreCase(new FilterValue[] {term});
    }

    /**
     * a filter condition checking if the current value is greater than the given parameter
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition greaterThan(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.getGREATER_THAN());
    }

    /**
     * a filter condition checking if the current value is less than the given parameter
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition lessThan(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.getLESS_THAN());
    }

    /**
     * a filter condition checking if the current value is greater than or equal to the given parameter
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition greaterThanOrEqualTo(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.getGREATER_THAN_OR_EQUAL());
    }

    /**
     * a filter condition checking if the current value is less than or equal to the given parameter
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition lessThanOrEqualTo(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.getLESS_THAN_OR_EQUAL());
    }

    /**
     * a filter condition checking if the current value is in the given set of values
     * 
     * @param terms
     * @return {@link FilterCondition}
     */
    public FilterCondition in(FilterValue[] terms) {
        return makeIn(terms, Table_pb.MatchType.getREGULAR(), Table_pb.CaseSensitivity.getMATCH_CASE());
    }

    private FilterCondition makeIn(FilterValue[] terms, double matchType, double casesensitivity) {
        InCondition value = new InCondition();
        value.setTarget(descriptor);
        value.setMatchType(matchType);
        value.setCaseSensitivity(casesensitivity);
        value.setCandidatesList(Arrays.stream(terms).map(v -> v.descriptor).toArray(Value[]::new));

        Condition c = new Condition();
        c.setIn(value);
        return FilterCondition.createAndValidate(c);
    }

    /**
     * a filter condition checking if the current value is in the given set of values, ignoring differences of upper vs
     * lower case
     * 
     * @param terms
     * @return {@link FilterCondition}
     */
    public FilterCondition inIgnoreCase(FilterValue[] terms) {
        return makeIn(terms, Table_pb.MatchType.getREGULAR(), Table_pb.CaseSensitivity.getIGNORE_CASE());
    }

    /**
     * a filter condition checking that the current value is not in the given set of values
     * 
     * @param terms
     * @return {@link FilterCondition}
     */
    public FilterCondition notIn(FilterValue[] terms) {
        return makeIn(terms, Table_pb.MatchType.getINVERTED(), Table_pb.CaseSensitivity.getMATCH_CASE());
    }

    /**
     * a filter condition checking that the current value is not in the given set of values, ignoring differences of
     * upper vs lower case
     * 
     * @param terms
     * @return {@link FilterCondition}
     */
    public FilterCondition notInIgnoreCase(FilterValue[] terms) {
        return makeIn(terms, Table_pb.MatchType.getINVERTED(), Table_pb.CaseSensitivity.getIGNORE_CASE());
    }

    /**
     * a filter condition checking if the given value contains the given string value
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition contains(FilterValue term) {
        return makeContains(term, Table_pb.CaseSensitivity.getMATCH_CASE());
    }

    /**
     * a filter condition checking if the given value contains the given string value, ignoring differences of upper vs
     * lower case
     * 
     * @param term
     * @return {@link FilterCondition}
     */
    public FilterCondition containsIgnoreCase(FilterValue term) {
        return makeContains(term, Table_pb.CaseSensitivity.getIGNORE_CASE());
    }

    private FilterCondition makeContains(FilterValue term, double casesensitivity) {
        ContainsCondition contains = new ContainsCondition();
        contains.setReference(this.descriptor.getReference());
        contains.setSearchString(term.descriptor.getLiteral().getStringValue());
        contains.setCaseSensitivity(casesensitivity);

        Condition c = new Condition();
        c.setContains(contains);
        return FilterCondition.createAndValidate(c);
    }

    /**
     * a filter condition checking if the given value matches the provided regular expressions string. Regex patterns
     * use Java regex syntax
     * 
     * @param pattern
     * @return {@link FilterCondition}
     */
    public FilterCondition matches(FilterValue pattern) {
        return makeMatches(pattern, Table_pb.CaseSensitivity.getMATCH_CASE());
    }

    /**
     * a filter condition checking if the given value matches the provided regular expressions string, ignoring
     * differences of upper vs lower case. Regex patterns use Java regex syntax
     * 
     * @param pattern
     * @return {@link FilterCondition}
     */
    public FilterCondition matchesIgnoreCase(FilterValue pattern) {
        return makeMatches(pattern, Table_pb.CaseSensitivity.getIGNORE_CASE());
    }

    private FilterCondition makeMatches(FilterValue term, double casesensitivity) {
        MatchesCondition contains = new MatchesCondition();

        contains.setReference(this.descriptor.getReference());
        contains.setRegex(term.descriptor.getLiteral().getStringValue());
        contains.setCaseSensitivity(casesensitivity);

        Condition c = new Condition();
        c.setMatches(contains);
        return FilterCondition.createAndValidate(c);
    }

    /**
     * a filter condition checking if the current value is a true boolean
     * 
     * @return {@link FilterCondition}
     */
    public FilterCondition isTrue() {
        return eq(FilterValue.ofBoolean(true));
    }

    /**
     * a filter condition checking if the current value is a false boolean
     * 
     * @return {@link FilterCondition}
     */
    public FilterCondition isFalse() {
        return eq(FilterValue.ofBoolean(false));
    }

    /**
     * a filter condition checking if the current value is a null value
     * 
     * @return {@link FilterCondition}
     */
    public FilterCondition isNull() {
        IsNullCondition isNull = new IsNullCondition();
        isNull.setReference(this.descriptor.getReference());

        Condition c = new Condition();
        c.setIsNull(isNull);
        return FilterCondition.createAndValidate(c);
    }

    /**
     * a filter condition invoking the given method on the current value, with the given parameters. Currently supported
     * functions that can be invoked on a String:
     * <ul>
     * <li><b>startsWith</b>: Returns true if the current string value starts with the supplied string argument</li>
     * <li><b>endsWith</b>: Returns true if the current string value ends with the supplied string argument</li>
     * <li><b>matches</b>: Returns true if the current string value matches the supplied string argument used as a Java
     * regular expression</li>
     * <li><b>contains</b>: Returns true if the current string value contains the supplied string argument
     * <p>
     * When invoking against a constant, this should be avoided in favor of FilterValue.contains
     * </p>
     * </li>
     * </ul>
     *
     * @param method
     * @param args
     * @return
     */
    public FilterCondition invoke(String method, FilterValue... args) {
        InvokeCondition invoke = new InvokeCondition();
        invoke.setMethod(method);
        invoke.setTarget(descriptor);
        invoke.setArgumentsList(Arrays.stream(args).map(v -> v.descriptor).toArray(Value[]::new));

        Condition c = new Condition();
        c.setInvoke(invoke);
        return FilterCondition.createAndValidate(c);
    }

    @Override
    public String toString() {
        // TODO (deephaven-core#723) implement a readable tostring rather than turning the pb object into a string
        return descriptor.toString();
    }
}
