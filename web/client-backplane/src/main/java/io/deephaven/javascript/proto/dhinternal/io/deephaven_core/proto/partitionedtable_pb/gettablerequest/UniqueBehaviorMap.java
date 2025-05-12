//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.partitionedtable_pb.gettablerequest;

import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.partitionedtable_pb.GetTableRequest.UniqueBehaviorMap",
        namespace = JsPackage.GLOBAL)
public interface UniqueBehaviorMap {
    @JsOverlay
    static UniqueBehaviorMap create() {
        return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty(name = "NOT_SET_UNIQUE_BEHAVIOR")
    int getNOT_SET_UNIQUE_BEHAVIOR();

    @JsProperty(name = "PERMIT_MULTIPLE_KEYS")
    int getPERMIT_MULTIPLE_KEYS();

    @JsProperty(name = "REQUIRE_UNIQUE_RESULTS_STATIC_SINGLE_KEY")
    int getREQUIRE_UNIQUE_RESULTS_STATIC_SINGLE_KEY();

    @JsProperty(name = "NOT_SET_UNIQUE_BEHAVIOR")
    void setNOT_SET_UNIQUE_BEHAVIOR(int NOT_SET_UNIQUE_BEHAVIOR);

    @JsProperty(name = "PERMIT_MULTIPLE_KEYS")
    void setPERMIT_MULTIPLE_KEYS(int PERMIT_MULTIPLE_KEYS);

    @JsProperty(name = "REQUIRE_UNIQUE_RESULTS_STATIC_SINGLE_KEY")
    void setREQUIRE_UNIQUE_RESULTS_STATIC_SINGLE_KEY(int REQUIRE_UNIQUE_RESULTS_STATIC_SINGLE_KEY);
}
