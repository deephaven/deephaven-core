package io.deephaven.web.client.api.filter;

import elemental2.core.Global;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.DateWrapper;
import io.deephaven.web.client.api.LongWrapper;
import io.deephaven.web.shared.ast.FilterPrinter;
import io.deephaven.web.shared.data.FilterDescriptor;
import io.deephaven.web.shared.data.FilterDescriptor.FilterOperation;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.Table_pb;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.*;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.comparecondition.CompareOperationMap;
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
        lit.setStringvalue(string);
        return new FilterValue(lit);
    }
    @JsMethod(namespace = "dh.FilterValue")
    public static FilterValue ofNumber(Object input) {
        Objects.requireNonNull(input);
        if (input instanceof DateWrapper) {
            Literal lit = new Literal();
            lit.setNanotimevalue(((DateWrapper) input).getWrapped());
            return new FilterValue(lit);
        } else if (input instanceof LongWrapper) {
            Literal lit = new Literal();
            lit.setLongvalue(((LongWrapper) input).getWrapped());
            return new FilterValue(lit);
        } else if (Js.typeof(input).equals("number")) {
            Literal lit = new Literal();
            lit.setDoublevalue(input.toString());
            return new FilterValue(lit);
        } else {
            //not sure what the input is, try to toString(), then parse to Double, and use that
            Literal lit = new Literal();
            lit.setDoublevalue(Double.parseDouble(input.toString()));
            return new FilterValue(lit);
        }
    }
    @JsMethod(namespace = "dh.FilterValue")
    public static FilterValue ofBoolean(Boolean b) {
        Objects.requireNonNull(b);

        Literal lit = new Literal();
        lit.setBoolvalue(b);
        return new FilterValue(lit);
    }

    private FilterValue(Literal literal) {
        descriptor = new Value();
        descriptor.setLiteral(literal);
    }

    @JsIgnore
    public FilterValue(Column column) {
        Reference ref = new Reference();
        ref.setColumnname(column.getName());

        descriptor = new Value();
        descriptor.setReference(ref);
    }

    @JsIgnore//hidden until implemented
    public FilterValue abs() {
        //TODO just sugar for invoke? special operation?
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
        //TODO this doesn't work, needs to be an IN, or need to make a way to do this
        return makeCompare(term, CompareCondition.CompareOperation.getEQUALS());
    }
    public FilterCondition notEq(FilterValue term) {
        return makeCompare(term, CompareCondition.CompareOperation.getNOT_EQUALS());
    }
    public FilterCondition notEqIgnoreCase(FilterValue term) {
        //TODO this doesn't work, needs to be an IN, or need to make a way to do this
        return makeCompare(term, CompareCondition.CompareOperation.getNOT_EQUALS());
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
        double matchType = Table_pb.MatchType.getREGULAR();
        double casesensitivity = Table_pb.CaseSensitivity.getMATCH_CASE();

        return makeIn(terms, matchType, casesensitivity);
    }

    public FilterCondition makeIn(FilterValue[] terms, double matchType, double casesensitivity) {
        InCondition value = new InCondition();
        value.setTarget(descriptor);
        value.setMatchtype(matchType);
        value.setCasesensitivity(casesensitivity);
        value.setCandidatesList(Arrays.stream(terms).map(v -> v.descriptor).toArray(Value[]::new));

        Condition c = new Condition();
        c.setIn(value);

        return FilterCondition.createAndValidate(c);
    }

    public FilterCondition inIgnoreCase(FilterValue[] terms) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.IN_ICASE);
        descriptor.setChildren(buildDescriptors(this.descriptor, terms));
        return FilterCondition.createAndValidate(descriptor);
    }

    public FilterCondition notIn(FilterValue[] terms) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.NOT_IN);
        descriptor.setChildren(buildDescriptors(this.descriptor, terms));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition notInIgnoreCase(FilterValue[] terms) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.NOT_IN_ICASE);
        descriptor.setChildren(buildDescriptors(this.descriptor, terms));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition contains(FilterValue term) {


        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.CONTAINS);
        descriptor.setChildren(buildDescriptors(this.descriptor, term));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition containsIgnoreCase(FilterValue term) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.CONTAINS_ICASE);
        descriptor.setChildren(buildDescriptors(this.descriptor, term));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition matches(FilterValue pattern) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.MATCHES);
        descriptor.setChildren(buildDescriptors(this.descriptor, pattern));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition matchesIgnoreCase(FilterValue pattern) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.MATCHES_ICASE);
        descriptor.setChildren(buildDescriptors(this.descriptor, pattern));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition isTrue() {
        return eq(FilterValue.ofBoolean(true));
    }
    public FilterCondition isFalse() {
        return eq(FilterValue.ofBoolean(false));
    }
    public FilterCondition isNull() {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.IS_NULL);
        descriptor.setChildren(new FilterDescriptor[] { this.descriptor });
        return FilterCondition.createAndValidate(descriptor);
    }

    public FilterCondition invoke(String method, FilterValue... args) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.INVOKE);
        descriptor.setValue(method);
        descriptor.setChildren(buildDescriptors(this.descriptor, args));
        return FilterCondition.createAndValidate(descriptor);
    }

    @JsIgnore
    private static FilterDescriptor[] buildDescriptors(FilterDescriptor one, FilterValue two) {
        return new FilterDescriptor[]{one, two.descriptor};
    }
    @JsIgnore
    protected static FilterDescriptor[] buildDescriptors(FilterDescriptor one, FilterValue... more) {
        FilterDescriptor[] descriptors = new FilterDescriptor[1];
        descriptors[0] = one;
        for (int i = 0; i < more.length; i++) {
            descriptors[i + 1] = more[i].descriptor;
        }
        return descriptors;
    }

    @Override
    public String toString() {
        return new FilterPrinter(Global.JSON::stringify).print(descriptor);
    }
}
