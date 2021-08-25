package io.deephaven.web.client.api.filter;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.Table_pb;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.CompareCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.Condition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ContainsCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.InCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.InvokeCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.IsNullCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.Literal;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.MatchesCondition;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.Reference;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.Value;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.DateWrapper;
import io.deephaven.web.client.api.LongWrapper;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.util.Arrays;
import java.util.Objects;

@JsType(namespace = "dh")
public class FilterValue {
    protected final Value descriptor;

    @JsMethod(namespace = "dh.FilterValue")
    public static FilterValue ofString(Object input) {
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

    @JsMethod(namespace = "dh.FilterValue")
    public static FilterValue ofNumber(Object input) {
        Objects.requireNonNull(input);
        if (input instanceof DateWrapper) {
            Literal lit = new Literal();
            lit.setNanoTimeValue(((DateWrapper) input).valueOf());
            return new FilterValue(lit);
        } else if (input instanceof LongWrapper) {
            Literal lit = new Literal();
            lit.setLongValue(((LongWrapper) input).valueOf());
            return new FilterValue(lit);
        } else if (Js.typeof(input).equals("number")) {
            Literal lit = new Literal();
            lit.setDoubleValue(Js.asDouble(input));
            return new FilterValue(lit);
        } else {
            // not sure what the input is, try to toString(), then parse to Double, and use that
            Literal lit = new Literal();
            lit.setDoubleValue(Double.parseDouble(input.toString()));
            return new FilterValue(lit);
        }
    }

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

    public FilterCondition eqIgnoreCase(FilterValue term) {
        return inIgnoreCase(new FilterValue[] {term});
    }

    public FilterCondition notEq(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.getNOT_EQUALS());
    }

    public FilterCondition notEqIgnoreCase(FilterValue term) {
        return notInIgnoreCase(new FilterValue[] {term});
    }

    public FilterCondition greaterThan(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.getGREATER_THAN());
    }

    public FilterCondition lessThan(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.getLESS_THAN());
    }

    public FilterCondition greaterThanOrEqualTo(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.getGREATER_THAN_OR_EQUAL());
    }

    public FilterCondition lessThanOrEqualTo(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.getLESS_THAN_OR_EQUAL());
    }

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

    public FilterCondition inIgnoreCase(FilterValue[] terms) {
        return makeIn(terms, Table_pb.MatchType.getREGULAR(), Table_pb.CaseSensitivity.getIGNORE_CASE());
    }

    public FilterCondition notIn(FilterValue[] terms) {
        return makeIn(terms, Table_pb.MatchType.getINVERTED(), Table_pb.CaseSensitivity.getMATCH_CASE());
    }

    public FilterCondition notInIgnoreCase(FilterValue[] terms) {
        return makeIn(terms, Table_pb.MatchType.getINVERTED(), Table_pb.CaseSensitivity.getIGNORE_CASE());
    }

    public FilterCondition contains(FilterValue term) {
        return makeContains(term, Table_pb.CaseSensitivity.getMATCH_CASE());
    }

    public FilterCondition containsIgnoreCase(FilterValue term) {
        return makeContains(term, Table_pb.CaseSensitivity.getIGNORE_CASE());
    }

    private FilterCondition makeContains(FilterValue term, double casesensitivity) {
        ContainsCondition contains = new ContainsCondition();
        contains.setSearchString(term.descriptor.getLiteral().getStringValue());
        contains.setCaseSensitivity(casesensitivity);

        Condition c = new Condition();
        c.setContains(contains);
        return FilterCondition.createAndValidate(c);
    }

    public FilterCondition matches(FilterValue pattern) {
        return makeMatches(pattern, Table_pb.CaseSensitivity.getMATCH_CASE());
    }

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

    public FilterCondition isTrue() {
        return eq(FilterValue.ofBoolean(true));
    }

    public FilterCondition isFalse() {
        return eq(FilterValue.ofBoolean(false));
    }

    public FilterCondition isNull() {
        IsNullCondition isNull = new IsNullCondition();
        isNull.setReference(this.descriptor.getReference());

        Condition c = new Condition();
        c.setIsNull(isNull);
        return FilterCondition.createAndValidate(c);
    }

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
