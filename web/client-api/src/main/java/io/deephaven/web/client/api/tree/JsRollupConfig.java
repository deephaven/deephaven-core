/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.web.client.api.tree;

import elemental2.core.JsArray;
import elemental2.core.JsObject;
import elemental2.core.JsString;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.hierarchicaltable_pb.RollupRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.AggSpec;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.Aggregation;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggregation.AggregationColumns;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggregation.AggregationCount;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecAbsSum;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecAvg;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecCountDistinct;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecDistinct;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecFirst;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecLast;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecMax;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecMin;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecStd;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecSum;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecUnique;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.aggspec.AggSpecVar;
import io.deephaven.web.client.api.Column;
import io.deephaven.web.client.api.tree.enums.JsAggregationOperation;
import io.deephaven.web.client.fu.JsLog;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

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
            if (!includeOriginalColumns) {
                JsLog.warn("includeDescriptions=false will be ignored");
            }
        }
        if (source.has("includeDescriptions")) {
            includeDescriptions = source.getAsAny("includeDescriptions").asBoolean();
            if (!includeDescriptions) {
                JsLog.warn("includeDescriptions=false will be ignored");
            }
        }
    }

    @JsIgnore
    public RollupRequest buildRequest(JsArray<Column> tableColumns) {
        RollupRequest request = new RollupRequest();

        request.setGroupByColumnsList(Js.<JsArray<String>>uncheckedCast(groupingColumns));
        request.setIncludeConstituents(includeConstituents);
        JsArray<Aggregation> aggregations = new JsArray<>();
        request.setAggregationsList(aggregations);

        this.aggregations.forEach(key -> {
            Aggregation agg = new Aggregation();
            aggregations.push(agg);
            JsArray<String> aggColumns = Js.cast(this.aggregations.get(key));
            switch (key) {
                case JsAggregationOperation.COUNT: {
                    AggregationCount count = new AggregationCount();
                    count.setColumnName("count");// TODO use this
                    agg.setCount(count);
                    break;
                }
                case JsAggregationOperation.COUNT_DISTINCT: {
                    AggregationColumns columns = new AggregationColumns();
                    AggSpec spec = new AggSpec();
                    spec.setCountDistinct(new AggSpecCountDistinct());
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.DISTINCT: {
                    AggregationColumns columns = new AggregationColumns();
                    AggSpec spec = new AggSpec();
                    spec.setDistinct(new AggSpecDistinct());
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.MIN: {
                    AggregationColumns columns = new AggregationColumns();
                    AggSpec spec = new AggSpec();
                    spec.setMin(new AggSpecMin());
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.MAX: {
                    AggregationColumns columns = new AggregationColumns();
                    AggSpec spec = new AggSpec();
                    spec.setMax(new AggSpecMax());
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.SUM: {
                    AggregationColumns columns = new AggregationColumns();
                    AggSpec spec = new AggSpec();
                    spec.setSum(new AggSpecSum());
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.ABS_SUM: {
                    AggregationColumns columns = new AggregationColumns();
                    AggSpec spec = new AggSpec();
                    spec.setAbsSum(new AggSpecAbsSum());
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.VAR: {
                    AggregationColumns columns = new AggregationColumns();
                    AggSpec spec = new AggSpec();
                    spec.setVar(new AggSpecVar());
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.AVG: {
                    AggregationColumns columns = new AggregationColumns();
                    AggSpec spec = new AggSpec();
                    spec.setAvg(new AggSpecAvg());
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.STD: {
                    AggregationColumns columns = new AggregationColumns();
                    AggSpec spec = new AggSpec();
                    spec.setStd(new AggSpecStd());
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.FIRST: {
                    AggregationColumns columns = new AggregationColumns();
                    AggSpec spec = new AggSpec();
                    spec.setFirst(new AggSpecFirst());
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.LAST: {
                    AggregationColumns columns = new AggregationColumns();
                    AggSpec spec = new AggSpec();
                    spec.setLast(new AggSpecLast());
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                case JsAggregationOperation.UNIQUE: {
                    AggregationColumns columns = new AggregationColumns();
                    AggSpec spec = new AggSpec();
                    spec.setUnique(new AggSpecUnique());
                    columns.setSpec(spec);
                    agg.setColumns(columns);
                    columns.setMatchPairsList(aggColumns);
                    break;
                }
                // case JsAggregationOperation.SORTED_FIRST: {
                // // TODO support this
                // }
                // case JsAggregationOperation.SORTED_LAST: {
                // // TODO support this=
                // }
                // case JsAggregationOperation.WSUM: {
                // //TODO support this
                // }
                case JsAggregationOperation.SKIP: {
                    // TODO mark this as not getting the default applied to it
                }
            }
        });
        return request;
    }
}
