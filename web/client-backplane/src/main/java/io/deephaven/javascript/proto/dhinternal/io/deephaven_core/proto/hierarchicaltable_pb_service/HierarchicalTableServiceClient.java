//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.HierarchicalTableApplyRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.HierarchicalTableApplyResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.HierarchicalTableSourceExportRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.HierarchicalTableViewRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.HierarchicalTableViewResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.RollupRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.RollupResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.TreeRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.hierarchicaltable_pb.TreeResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven_core.proto.table_pb.ExportedTableCreationResponse;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven_core.proto.hierarchicaltable_pb_service.HierarchicalTableServiceClient",
        namespace = JsPackage.GLOBAL)
public class HierarchicalTableServiceClient {
    @JsFunction
    public interface ApplyCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static HierarchicalTableServiceClient.ApplyCallbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                HierarchicalTableServiceClient.ApplyCallbackFn.P0Type p0,
                HierarchicalTableApplyResponse p1);
    }

    @JsFunction
    public interface ApplyMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static HierarchicalTableServiceClient.ApplyMetadata_or_callbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                HierarchicalTableServiceClient.ApplyMetadata_or_callbackFn.P0Type p0,
                HierarchicalTableApplyResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ApplyMetadata_or_callbackUnionType {
        @JsOverlay
        static HierarchicalTableServiceClient.ApplyMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default HierarchicalTableServiceClient.ApplyMetadata_or_callbackFn asApplyMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isApplyMetadata_or_callbackFn() {
            return (Object) this instanceof HierarchicalTableServiceClient.ApplyMetadata_or_callbackFn;
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }
    }

    @JsFunction
    public interface ExportSourceCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static HierarchicalTableServiceClient.ExportSourceCallbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                HierarchicalTableServiceClient.ExportSourceCallbackFn.P0Type p0,
                ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface ExportSourceMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackFn.P0Type p0,
                ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ExportSourceMetadata_or_callbackUnionType {
        @JsOverlay
        static HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackFn asExportSourceMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isExportSourceMetadata_or_callbackFn() {
            return (Object) this instanceof HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface RollupCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static HierarchicalTableServiceClient.RollupCallbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(HierarchicalTableServiceClient.RollupCallbackFn.P0Type p0, RollupResponse p1);
    }

    @JsFunction
    public interface RollupMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static HierarchicalTableServiceClient.RollupMetadata_or_callbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                HierarchicalTableServiceClient.RollupMetadata_or_callbackFn.P0Type p0, RollupResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface RollupMetadata_or_callbackUnionType {
        @JsOverlay
        static HierarchicalTableServiceClient.RollupMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default HierarchicalTableServiceClient.RollupMetadata_or_callbackFn asRollupMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isRollupMetadata_or_callbackFn() {
            return (Object) this instanceof HierarchicalTableServiceClient.RollupMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface TreeCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static HierarchicalTableServiceClient.TreeCallbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(HierarchicalTableServiceClient.TreeCallbackFn.P0Type p0, TreeResponse p1);
    }

    @JsFunction
    public interface TreeMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static HierarchicalTableServiceClient.TreeMetadata_or_callbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                HierarchicalTableServiceClient.TreeMetadata_or_callbackFn.P0Type p0, TreeResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TreeMetadata_or_callbackUnionType {
        @JsOverlay
        static HierarchicalTableServiceClient.TreeMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default HierarchicalTableServiceClient.TreeMetadata_or_callbackFn asTreeMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isTreeMetadata_or_callbackFn() {
            return (Object) this instanceof HierarchicalTableServiceClient.TreeMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface ViewCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static HierarchicalTableServiceClient.ViewCallbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                HierarchicalTableServiceClient.ViewCallbackFn.P0Type p0, HierarchicalTableViewResponse p1);
    }

    @JsFunction
    public interface ViewMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static HierarchicalTableServiceClient.ViewMetadata_or_callbackFn.P0Type create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            double getCode();

            @JsProperty
            String getMessage();

            @JsProperty
            BrowserHeaders getMetadata();

            @JsProperty
            void setCode(double code);

            @JsProperty
            void setMessage(String message);

            @JsProperty
            void setMetadata(BrowserHeaders metadata);
        }

        void onInvoke(
                HierarchicalTableServiceClient.ViewMetadata_or_callbackFn.P0Type p0,
                HierarchicalTableViewResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ViewMetadata_or_callbackUnionType {
        @JsOverlay
        static HierarchicalTableServiceClient.ViewMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default HierarchicalTableServiceClient.ViewMetadata_or_callbackFn asViewMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isViewMetadata_or_callbackFn() {
            return (Object) this instanceof HierarchicalTableServiceClient.ViewMetadata_or_callbackFn;
        }
    }

    public String serviceHost;

    public HierarchicalTableServiceClient(String serviceHost, Object options) {}

    public HierarchicalTableServiceClient(String serviceHost) {}

    @JsOverlay
    public final UnaryResponse apply(
            HierarchicalTableApplyRequest requestMessage,
            HierarchicalTableServiceClient.ApplyMetadata_or_callbackFn metadata_or_callback,
            HierarchicalTableServiceClient.ApplyCallbackFn callback) {
        return apply(
                requestMessage,
                Js.<HierarchicalTableServiceClient.ApplyMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse apply(
            HierarchicalTableApplyRequest requestMessage,
            HierarchicalTableServiceClient.ApplyMetadata_or_callbackFn metadata_or_callback) {
        return apply(
                requestMessage,
                Js.<HierarchicalTableServiceClient.ApplyMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse apply(
            HierarchicalTableApplyRequest requestMessage,
            HierarchicalTableServiceClient.ApplyMetadata_or_callbackUnionType metadata_or_callback,
            HierarchicalTableServiceClient.ApplyCallbackFn callback);

    public native UnaryResponse apply(
            HierarchicalTableApplyRequest requestMessage,
            HierarchicalTableServiceClient.ApplyMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse apply(
            HierarchicalTableApplyRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            HierarchicalTableServiceClient.ApplyCallbackFn callback) {
        return apply(
                requestMessage,
                Js.<HierarchicalTableServiceClient.ApplyMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse apply(
            HierarchicalTableApplyRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return apply(
                requestMessage,
                Js.<HierarchicalTableServiceClient.ApplyMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse exportSource(
            HierarchicalTableSourceExportRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            HierarchicalTableServiceClient.ExportSourceCallbackFn callback) {
        return exportSource(
                requestMessage,
                Js.<HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse exportSource(
            HierarchicalTableSourceExportRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return exportSource(
                requestMessage,
                Js.<HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse exportSource(
            HierarchicalTableSourceExportRequest requestMessage,
            HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackFn metadata_or_callback,
            HierarchicalTableServiceClient.ExportSourceCallbackFn callback) {
        return exportSource(
                requestMessage,
                Js.<HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse exportSource(
            HierarchicalTableSourceExportRequest requestMessage,
            HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackFn metadata_or_callback) {
        return exportSource(
                requestMessage,
                Js.<HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse exportSource(
            HierarchicalTableSourceExportRequest requestMessage,
            HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackUnionType metadata_or_callback,
            HierarchicalTableServiceClient.ExportSourceCallbackFn callback);

    public native UnaryResponse exportSource(
            HierarchicalTableSourceExportRequest requestMessage,
            HierarchicalTableServiceClient.ExportSourceMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse rollup(
            RollupRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            HierarchicalTableServiceClient.RollupCallbackFn callback) {
        return rollup(
                requestMessage,
                Js.<HierarchicalTableServiceClient.RollupMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse rollup(
            RollupRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return rollup(
                requestMessage,
                Js.<HierarchicalTableServiceClient.RollupMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse rollup(
            RollupRequest requestMessage,
            HierarchicalTableServiceClient.RollupMetadata_or_callbackFn metadata_or_callback,
            HierarchicalTableServiceClient.RollupCallbackFn callback) {
        return rollup(
                requestMessage,
                Js.<HierarchicalTableServiceClient.RollupMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse rollup(
            RollupRequest requestMessage,
            HierarchicalTableServiceClient.RollupMetadata_or_callbackFn metadata_or_callback) {
        return rollup(
                requestMessage,
                Js.<HierarchicalTableServiceClient.RollupMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse rollup(
            RollupRequest requestMessage,
            HierarchicalTableServiceClient.RollupMetadata_or_callbackUnionType metadata_or_callback,
            HierarchicalTableServiceClient.RollupCallbackFn callback);

    public native UnaryResponse rollup(
            RollupRequest requestMessage,
            HierarchicalTableServiceClient.RollupMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse tree(
            TreeRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            HierarchicalTableServiceClient.TreeCallbackFn callback) {
        return tree(
                requestMessage,
                Js.<HierarchicalTableServiceClient.TreeMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse tree(TreeRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return tree(
                requestMessage,
                Js.<HierarchicalTableServiceClient.TreeMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse tree(
            TreeRequest requestMessage,
            HierarchicalTableServiceClient.TreeMetadata_or_callbackFn metadata_or_callback,
            HierarchicalTableServiceClient.TreeCallbackFn callback) {
        return tree(
                requestMessage,
                Js.<HierarchicalTableServiceClient.TreeMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse tree(
            TreeRequest requestMessage,
            HierarchicalTableServiceClient.TreeMetadata_or_callbackFn metadata_or_callback) {
        return tree(
                requestMessage,
                Js.<HierarchicalTableServiceClient.TreeMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse tree(
            TreeRequest requestMessage,
            HierarchicalTableServiceClient.TreeMetadata_or_callbackUnionType metadata_or_callback,
            HierarchicalTableServiceClient.TreeCallbackFn callback);

    public native UnaryResponse tree(
            TreeRequest requestMessage,
            HierarchicalTableServiceClient.TreeMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse view(
            HierarchicalTableViewRequest requestMessage,
            BrowserHeaders metadata_or_callback,
            HierarchicalTableServiceClient.ViewCallbackFn callback) {
        return view(
                requestMessage,
                Js.<HierarchicalTableServiceClient.ViewMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse view(
            HierarchicalTableViewRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return view(
                requestMessage,
                Js.<HierarchicalTableServiceClient.ViewMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse view(
            HierarchicalTableViewRequest requestMessage,
            HierarchicalTableServiceClient.ViewMetadata_or_callbackFn metadata_or_callback,
            HierarchicalTableServiceClient.ViewCallbackFn callback) {
        return view(
                requestMessage,
                Js.<HierarchicalTableServiceClient.ViewMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback),
                callback);
    }

    @JsOverlay
    public final UnaryResponse view(
            HierarchicalTableViewRequest requestMessage,
            HierarchicalTableServiceClient.ViewMetadata_or_callbackFn metadata_or_callback) {
        return view(
                requestMessage,
                Js.<HierarchicalTableServiceClient.ViewMetadata_or_callbackUnionType>uncheckedCast(
                        metadata_or_callback));
    }

    public native UnaryResponse view(
            HierarchicalTableViewRequest requestMessage,
            HierarchicalTableServiceClient.ViewMetadata_or_callbackUnionType metadata_or_callback,
            HierarchicalTableServiceClient.ViewCallbackFn callback);

    public native UnaryResponse view(
            HierarchicalTableViewRequest requestMessage,
            HierarchicalTableServiceClient.ViewMetadata_or_callbackUnionType metadata_or_callback);
}
