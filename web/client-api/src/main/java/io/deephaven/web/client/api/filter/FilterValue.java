package io.deephaven.web.client.api.filter;

import elemental2.core.Global;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.DateWrapper;
import io.deephaven.web.client.api.LongWrapper;
import io.deephaven.web.shared.ast.FilterPrinter;
import io.deephaven.web.shared.data.FilterDescriptor;
import io.deephaven.web.shared.data.FilterDescriptor.FilterOperation;
import io.deephaven.web.shared.data.FilterDescriptor.ValueType;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsMethod;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

import java.util.Objects;

@JsType(namespace = "dh")
public class FilterValue {
    protected final FilterDescriptor descriptor;

    @JsMethod(namespace = "dh.FilterValue")
    public static FilterValue ofString(Object input) {
        Objects.requireNonNull(input);
        final String string;
        if (Js.typeof(input).equals("string")) {
            string = (String) input;
        } else {
            string = input.toString();
        }
        return new FilterValue(string, ValueType.String);
    }
    @JsMethod(namespace = "dh.FilterValue")
    public static FilterValue ofNumber(Object input) {
        Objects.requireNonNull(input);
        if (input instanceof DateWrapper) {
            return new FilterValue(input.toString(), ValueType.Datetime);
        } else if (input instanceof LongWrapper) {
            return new FilterValue(input.toString(), ValueType.Long);
        } else if (Js.typeof(input).equals("number")) {
            return new FilterValue(input.toString(), ValueType.Number);
        } else {
            //not sure what the input is, try to toString(), then parse to Double, and use that
            return new FilterValue(Double.valueOf(input.toString()).toString(), ValueType.Number);
        }
    }
    @JsMethod(namespace = "dh.FilterValue")
    public static FilterValue ofBoolean(Boolean b) {
        Objects.requireNonNull(b);
        return new FilterValue(Boolean.toString(b), ValueType.Boolean);
    }

    private FilterValue(String string, FilterDescriptor.ValueType type) {
        descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.LITERAL);
        descriptor.setType(type);
        descriptor.setValue(string);
        descriptor.setChildren(new FilterDescriptor[0]);
    }

    @JsIgnore
    public FilterValue(Column column) {
        descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.REFERENCE);
        descriptor.setValue(column.getName());
        descriptor.setChildren(new FilterDescriptor[0]);
    }

    @JsIgnore//hidden until implemented
    public FilterValue abs() {
        //TODO just sugar for invoke? special operation?
        return this;
    }

    public FilterCondition eq(FilterValue term) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.EQ);
        descriptor.setChildren(buildDescriptors(this.descriptor, term));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition eqIgnoreCase(FilterValue term) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.EQ_ICASE);
        descriptor.setChildren(buildDescriptors(this.descriptor, term));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition notEq(FilterValue term) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.NEQ);
        descriptor.setChildren(buildDescriptors(this.descriptor, term));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition notEqIgnoreCase(FilterValue term) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.NEQ_ICASE);
        descriptor.setChildren(buildDescriptors(this.descriptor, term));
        return FilterCondition.createAndValidate(descriptor);
    }

    public FilterCondition greaterThan(FilterValue term) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.GT);
        descriptor.setChildren(buildDescriptors(this.descriptor, term));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition lessThan(FilterValue term) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.LT);
        descriptor.setChildren(buildDescriptors(this.descriptor, term));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition greaterThanOrEqualTo(FilterValue term) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.GTE);
        descriptor.setChildren(buildDescriptors(this.descriptor, term));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition lessThanOrEqualTo(FilterValue term) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.LTE);
        descriptor.setChildren(buildDescriptors(this.descriptor, term));
        return FilterCondition.createAndValidate(descriptor);
    }
    public FilterCondition in(FilterValue[] terms) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.IN);
        descriptor.setChildren(buildDescriptors(this.descriptor, terms));
        return FilterCondition.createAndValidate(descriptor);
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
