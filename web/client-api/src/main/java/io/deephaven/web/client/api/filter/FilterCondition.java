package io.deephaven.web.client.api.filter;

import elemental2.core.Global;
import elemental2.core.JsArray;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.shared.ast.FilterPrinter;
import io.deephaven.web.shared.ast.FilterValidator;
import io.deephaven.web.shared.data.FilterDescriptor;
import io.deephaven.web.shared.data.FilterDescriptor.FilterOperation;
import jsinterop.annotations.*;

@JsType(namespace = "dh")
public class FilterCondition {

    @JsIgnore
    private final FilterDescriptor descriptor;

    @JsMethod(namespace = "dh.FilterCondition")
    public static FilterCondition invoke(String function, FilterValue... params) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.INVOKE);
        descriptor.setValue(function);
        descriptor.setChildren(FilterValue.buildDescriptors(null, params));

        return createAndValidate(descriptor);
    }

    @JsMethod(namespace = "dh.FilterCondition")
    public static FilterCondition search(FilterValue value, @JsOptional FilterValue[] columns) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.SEARCH);
        if (columns == null) {
            descriptor.setChildren(new FilterDescriptor[]{value.descriptor});
        } else {
            descriptor.setChildren(FilterValue.buildDescriptors(value.descriptor, columns));
        }

        return createAndValidate(descriptor);
    }

    @JsIgnore
    public FilterCondition(FilterDescriptor descriptor) {
        this.descriptor = descriptor;
    }

    public FilterCondition not() {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.NOT);
        descriptor.setChildren(new FilterDescriptor[]{this.descriptor});
        return createAndValidate(descriptor);
    }

    @JsIgnore
    protected static FilterCondition createAndValidate(FilterDescriptor descriptor) {
        descriptor.accept(new FilterValidator((a, b) -> true, a -> true));
        return new FilterCondition(descriptor);
    }

    public FilterCondition and(FilterCondition one, FilterCondition... more) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.AND);
        descriptor.setChildren(buildDescriptors(this.descriptor, one, more));
        return createAndValidate(descriptor);
    }


    public FilterCondition or(FilterCondition one, FilterCondition... more) {
        FilterDescriptor descriptor = new FilterDescriptor();
        descriptor.setOperation(FilterOperation.OR);
        descriptor.setChildren(buildDescriptors(this.descriptor, one, more));
        return createAndValidate(descriptor);
    }

    @JsIgnore
    public FilterDescriptor makeDescriptor() {
        // these are immutable, so we can just return the instance
        return descriptor;
    }

    @JsMethod
    public String toString() {
        return new FilterPrinter(Global.JSON::stringify).print(descriptor);
    }

    @JsProperty
    public JsArray<Column> getColumns() {
        return new JsArray<>();
    }

    @JsIgnore
    private static FilterDescriptor[] buildDescriptors(FilterDescriptor descriptor, FilterCondition one, FilterCondition[] more) {
        FilterDescriptor[] descriptors = new FilterDescriptor[more.length + 2];
        descriptors[0] = descriptor;
        descriptors[1] = one.descriptor;
        for (int i = 0; i < more.length; i++) {
            descriptors[i + 2] = more[i].descriptor;
        }
        return descriptors;
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
