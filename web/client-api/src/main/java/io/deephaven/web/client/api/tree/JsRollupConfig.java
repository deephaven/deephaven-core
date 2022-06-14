/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.tree;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.core.JsString;
import io.deephaven.web.shared.fu.JsArrays;
import io.deephaven.web.shared.requests.RollupTableRequest;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

import java.util.ArrayList;

@JsType(name = "RollupConfig", namespace = "dh")
public class JsRollupConfig {

    public JsArray<JsString> groupingColumns = null;
    public JsPropertyMap<JsArray<JsString>> aggregations = Js.cast(JsObject.create(null));
    public boolean includeConstituents = false;
    public boolean includeOriginalColumns = false;
    public boolean includeDescriptions = true;

    @JsConstructor
    public JsRollupConfig() {}

    @JsIgnore
    public JsRollupConfig(JsPropertyMap<Object> source) {
        this();

        if (source.has("aggregations")) {
            aggregations = source.getAsAny("aggregations").cast();
        }
        if (source.has("groupingColumns")) {
            groupingColumns = source.getAsAny("groupingColumns").cast();
        }
        if (source.has("includeConstituents")) {
            includeConstituents = source.getAsAny("includeConstituents").asBoolean();
        }
        if (source.has("includeOriginalColumns")) {
            includeOriginalColumns = source.getAsAny("includeOriginalColumns").asBoolean();
        }
        if (source.has("includeDescriptions")) {
            includeDescriptions = source.getAsAny("includeDescriptions").asBoolean();
        }
    }

    @JsIgnore
    public RollupTableRequest buildRequest() {
        RollupTableRequest rollupRequest = new RollupTableRequest();
        ArrayList<String> aggregations = new ArrayList<>();
        this.aggregations.forEach(key -> aggregations
                .add("" + key + "=" + String.join(",", JsArrays.toStringArray(this.aggregations.get(key)))));
        rollupRequest.setAggregations(aggregations.toArray(new String[0]));
        JsArrays.setArray(groupingColumns, rollupRequest::setGroupingColumns);
        rollupRequest.setIncludeConstituents(includeConstituents);
        rollupRequest.setIncludeOriginalColumns(includeOriginalColumns);
        rollupRequest.setIncludeDescriptions(includeDescriptions);
        return rollupRequest;
    }
}
