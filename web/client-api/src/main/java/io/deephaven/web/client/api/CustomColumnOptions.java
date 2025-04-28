//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.vertispan.tsdefs.annotations.TsTypeDef;
import com.vertispan.tsdefs.annotations.TsTypeRef;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsNullable;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

/**
 * Options for custom columns.
 */
@JsType(namespace = "dh")
public class CustomColumnOptions {
    /**
     * When specified for custom columns on a rollup table, specifies if the formula apply to rollup or constituent
     * nodes.
     */
    @JsNullable
    public @TsTypeRef(RollupNodeType.class) String rollupNodeType;

    public CustomColumnOptions() {

    }

    @JsIgnore
    public CustomColumnOptions(JsPropertyMap<Object> options) {
        this();
        if (options.has("rollupNodeType")) {
            this.rollupNodeType = options.getAsAny("rollupNodeType").asString();
        }
    }

    /**
     * Describes the type of node in a rollup table.
     */
    @JsType(namespace = "dh")
    @TsTypeDef(tsType = "string")
    public static class RollupNodeType {
        /**
         * In a rollup with constituents visible, this is all non-leaf nodes. In a rollup without constituents, all
         * levels are aggregate nodes.
         */
        public static final String ROLLUP_NODE_TYPE_AGGREGATED = "aggregated";
        /**
         * Only present in rollups with constituents visible. This represents the leaf level of the rollup.
         */
        public static final String ROLLUP_NODE_TYPE_CONSTITUENT = "constituent";
    }
}
