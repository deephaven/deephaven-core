package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb_service;

import io.deephaven.javascript.proto.dhinternal.browserheaders.BrowserHeaders;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.AsOfJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.BatchTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ComboAggregateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.CrossJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.DropColumnsRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.EmptyTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExactJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableUpdateMessage;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.ExportedTableUpdatesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.FilterTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.FlattenRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.HeadOrTailByRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.HeadOrTailRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.LeftJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.MergeTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.NaturalJoinTablesRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.RunChartDownsampleRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SelectDistinctRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SelectOrUpdateRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SnapshotTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.SortTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.TimeTableRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.UngroupRequest;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.UnstructuredFilterTableRequest;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb_service.TableServiceClient",
    namespace = JsPackage.GLOBAL)
public class TableServiceClient {
    @JsFunction
    public interface AsOfJoinTablesCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.AsOfJoinTablesCallbackFn.P0Type create() {
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
            TableServiceClient.AsOfJoinTablesCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface AsOfJoinTablesMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.AsOfJoinTablesMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.AsOfJoinTablesMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AsOfJoinTablesMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.AsOfJoinTablesMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default TableServiceClient.AsOfJoinTablesMetadata_or_callbackFn asAsOfJoinTablesMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isAsOfJoinTablesMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.AsOfJoinTablesMetadata_or_callbackFn;
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }
    }

    @JsFunction
    public interface ComboAggregateCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.ComboAggregateCallbackFn.P0Type create() {
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
            TableServiceClient.ComboAggregateCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface ComboAggregateMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.ComboAggregateMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.ComboAggregateMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ComboAggregateMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.ComboAggregateMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.ComboAggregateMetadata_or_callbackFn asComboAggregateMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isComboAggregateMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.ComboAggregateMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface CrossJoinTablesCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.CrossJoinTablesCallbackFn.P0Type create() {
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
            TableServiceClient.CrossJoinTablesCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface CrossJoinTablesMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.CrossJoinTablesMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.CrossJoinTablesMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface CrossJoinTablesMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.CrossJoinTablesMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.CrossJoinTablesMetadata_or_callbackFn asCrossJoinTablesMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isCrossJoinTablesMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.CrossJoinTablesMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface DropColumnsCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.DropColumnsCallbackFn.P0Type create() {
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
            TableServiceClient.DropColumnsCallbackFn.P0Type p0, ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface DropColumnsMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.DropColumnsMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.DropColumnsMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface DropColumnsMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.DropColumnsMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.DropColumnsMetadata_or_callbackFn asDropColumnsMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isDropColumnsMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.DropColumnsMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface EmptyTableCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.EmptyTableCallbackFn.P0Type create() {
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
            TableServiceClient.EmptyTableCallbackFn.P0Type p0, ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface EmptyTableMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.EmptyTableMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.EmptyTableMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface EmptyTableMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.EmptyTableMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.EmptyTableMetadata_or_callbackFn asEmptyTableMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isEmptyTableMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.EmptyTableMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface ExactJoinTablesCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.ExactJoinTablesCallbackFn.P0Type create() {
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
            TableServiceClient.ExactJoinTablesCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface ExactJoinTablesMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.ExactJoinTablesMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.ExactJoinTablesMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ExactJoinTablesMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.ExactJoinTablesMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.ExactJoinTablesMetadata_or_callbackFn asExactJoinTablesMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isExactJoinTablesMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.ExactJoinTablesMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface FilterCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.FilterCallbackFn.P0Type create() {
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

        void onInvoke(TableServiceClient.FilterCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface FilterMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.FilterMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.FilterMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FilterMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.FilterMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.FilterMetadata_or_callbackFn asFilterMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isFilterMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.FilterMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface FlattenCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.FlattenCallbackFn.P0Type create() {
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

        void onInvoke(TableServiceClient.FlattenCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface FlattenMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.FlattenMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.FlattenMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FlattenMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.FlattenMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.FlattenMetadata_or_callbackFn asFlattenMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isFlattenMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.FlattenMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface HeadByCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.HeadByCallbackFn.P0Type create() {
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

        void onInvoke(TableServiceClient.HeadByCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface HeadByMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.HeadByMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.HeadByMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface HeadByMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.HeadByMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.HeadByMetadata_or_callbackFn asHeadByMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isHeadByMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.HeadByMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface HeadCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.HeadCallbackFn.P0Type create() {
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

        void onInvoke(TableServiceClient.HeadCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface HeadMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.HeadMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.HeadMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface HeadMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.HeadMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.HeadMetadata_or_callbackFn asHeadMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isHeadMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.HeadMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface LazyUpdateCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.LazyUpdateCallbackFn.P0Type create() {
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
            TableServiceClient.LazyUpdateCallbackFn.P0Type p0, ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface LazyUpdateMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.LazyUpdateMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.LazyUpdateMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LazyUpdateMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.LazyUpdateMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.LazyUpdateMetadata_or_callbackFn asLazyUpdateMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isLazyUpdateMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.LazyUpdateMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface LeftJoinTablesCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.LeftJoinTablesCallbackFn.P0Type create() {
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
            TableServiceClient.LeftJoinTablesCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface LeftJoinTablesMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.LeftJoinTablesMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.LeftJoinTablesMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface LeftJoinTablesMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.LeftJoinTablesMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.LeftJoinTablesMetadata_or_callbackFn asLeftJoinTablesMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isLeftJoinTablesMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.LeftJoinTablesMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface MergeTablesCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.MergeTablesCallbackFn.P0Type create() {
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
            TableServiceClient.MergeTablesCallbackFn.P0Type p0, ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface MergeTablesMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.MergeTablesMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.MergeTablesMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface MergeTablesMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.MergeTablesMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.MergeTablesMetadata_or_callbackFn asMergeTablesMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isMergeTablesMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.MergeTablesMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface NaturalJoinTablesCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.NaturalJoinTablesCallbackFn.P0Type create() {
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
            TableServiceClient.NaturalJoinTablesCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface NaturalJoinTablesMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.NaturalJoinTablesMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.NaturalJoinTablesMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface NaturalJoinTablesMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.NaturalJoinTablesMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.NaturalJoinTablesMetadata_or_callbackFn asNaturalJoinTablesMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isNaturalJoinTablesMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.NaturalJoinTablesMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface RunChartDownsampleCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.RunChartDownsampleCallbackFn.P0Type create() {
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
            TableServiceClient.RunChartDownsampleCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface RunChartDownsampleMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.RunChartDownsampleMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.RunChartDownsampleMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface RunChartDownsampleMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.RunChartDownsampleMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.RunChartDownsampleMetadata_or_callbackFn asRunChartDownsampleMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isRunChartDownsampleMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.RunChartDownsampleMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface SelectCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.SelectCallbackFn.P0Type create() {
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

        void onInvoke(TableServiceClient.SelectCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface SelectDistinctCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.SelectDistinctCallbackFn.P0Type create() {
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
            TableServiceClient.SelectDistinctCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface SelectDistinctMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.SelectDistinctMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.SelectDistinctMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SelectDistinctMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.SelectDistinctMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.SelectDistinctMetadata_or_callbackFn asSelectDistinctMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isSelectDistinctMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.SelectDistinctMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface SelectMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.SelectMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.SelectMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SelectMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.SelectMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.SelectMetadata_or_callbackFn asSelectMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isSelectMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.SelectMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface SnapshotCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.SnapshotCallbackFn.P0Type create() {
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
            TableServiceClient.SnapshotCallbackFn.P0Type p0, ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface SnapshotMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.SnapshotMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.SnapshotMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SnapshotMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.SnapshotMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.SnapshotMetadata_or_callbackFn asSnapshotMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isSnapshotMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.SnapshotMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface SortCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.SortCallbackFn.P0Type create() {
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

        void onInvoke(TableServiceClient.SortCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface SortMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.SortMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.SortMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SortMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.SortMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.SortMetadata_or_callbackFn asSortMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isSortMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.SortMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface TailByCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.TailByCallbackFn.P0Type create() {
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

        void onInvoke(TableServiceClient.TailByCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface TailByMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.TailByMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.TailByMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TailByMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.TailByMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.TailByMetadata_or_callbackFn asTailByMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isTailByMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.TailByMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface TailCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.TailCallbackFn.P0Type create() {
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

        void onInvoke(TableServiceClient.TailCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface TailMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.TailMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.TailMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TailMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.TailMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.TailMetadata_or_callbackFn asTailMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isTailMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.TailMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface TimeTableCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.TimeTableCallbackFn.P0Type create() {
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
            TableServiceClient.TimeTableCallbackFn.P0Type p0, ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface TimeTableMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.TimeTableMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.TimeTableMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TimeTableMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.TimeTableMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.TimeTableMetadata_or_callbackFn asTimeTableMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isTimeTableMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.TimeTableMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface UngroupCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.UngroupCallbackFn.P0Type create() {
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

        void onInvoke(TableServiceClient.UngroupCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface UngroupMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.UngroupMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.UngroupMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UngroupMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.UngroupMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.UngroupMetadata_or_callbackFn asUngroupMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isUngroupMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.UngroupMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface UnstructuredFilterCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.UnstructuredFilterCallbackFn.P0Type create() {
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
            TableServiceClient.UnstructuredFilterCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface UnstructuredFilterMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.UnstructuredFilterMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.UnstructuredFilterMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UnstructuredFilterMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.UnstructuredFilterMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.UnstructuredFilterMetadata_or_callbackFn asUnstructuredFilterMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isUnstructuredFilterMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.UnstructuredFilterMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface UpdateCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.UpdateCallbackFn.P0Type create() {
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

        void onInvoke(TableServiceClient.UpdateCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface UpdateMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.UpdateMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.UpdateMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UpdateMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.UpdateMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.UpdateMetadata_or_callbackFn asUpdateMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isUpdateMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.UpdateMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface UpdateViewCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.UpdateViewCallbackFn.P0Type create() {
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
            TableServiceClient.UpdateViewCallbackFn.P0Type p0, ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface UpdateViewMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.UpdateViewMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.UpdateViewMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UpdateViewMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.UpdateViewMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.UpdateViewMetadata_or_callbackFn asUpdateViewMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isUpdateViewMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.UpdateViewMetadata_or_callbackFn;
        }
    }

    @JsFunction
    public interface ViewCallbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.ViewCallbackFn.P0Type create() {
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

        void onInvoke(TableServiceClient.ViewCallbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsFunction
    public interface ViewMetadata_or_callbackFn {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface P0Type {
            @JsOverlay
            static TableServiceClient.ViewMetadata_or_callbackFn.P0Type create() {
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
            TableServiceClient.ViewMetadata_or_callbackFn.P0Type p0,
            ExportedTableCreationResponse p1);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ViewMetadata_or_callbackUnionType {
        @JsOverlay
        static TableServiceClient.ViewMetadata_or_callbackUnionType of(Object o) {
            return Js.cast(o);
        }

        @JsOverlay
        default BrowserHeaders asBrowserHeaders() {
            return Js.cast(this);
        }

        @JsOverlay
        default TableServiceClient.ViewMetadata_or_callbackFn asViewMetadata_or_callbackFn() {
            return Js.cast(this);
        }

        @JsOverlay
        default boolean isBrowserHeaders() {
            return (Object) this instanceof BrowserHeaders;
        }

        @JsOverlay
        default boolean isViewMetadata_or_callbackFn() {
            return (Object) this instanceof TableServiceClient.ViewMetadata_or_callbackFn;
        }
    }

    public String serviceHost;

    public TableServiceClient(String serviceHost, Object options) {}

    public TableServiceClient(String serviceHost) {}

    @JsOverlay
    public final UnaryResponse asOfJoinTables(
        AsOfJoinTablesRequest requestMessage,
        TableServiceClient.AsOfJoinTablesMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.AsOfJoinTablesCallbackFn callback) {
        return asOfJoinTables(
            requestMessage,
            Js.<TableServiceClient.AsOfJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse asOfJoinTables(
        AsOfJoinTablesRequest requestMessage,
        TableServiceClient.AsOfJoinTablesMetadata_or_callbackFn metadata_or_callback) {
        return asOfJoinTables(
            requestMessage,
            Js.<TableServiceClient.AsOfJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse asOfJoinTables(
        AsOfJoinTablesRequest requestMessage,
        TableServiceClient.AsOfJoinTablesMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.AsOfJoinTablesCallbackFn callback);

    public native UnaryResponse asOfJoinTables(
        AsOfJoinTablesRequest requestMessage,
        TableServiceClient.AsOfJoinTablesMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse asOfJoinTables(
        AsOfJoinTablesRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.AsOfJoinTablesCallbackFn callback) {
        return asOfJoinTables(
            requestMessage,
            Js.<TableServiceClient.AsOfJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse asOfJoinTables(
        AsOfJoinTablesRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return asOfJoinTables(
            requestMessage,
            Js.<TableServiceClient.AsOfJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native ResponseStream<ExportedTableCreationResponse> batch(
        BatchTableRequest requestMessage, BrowserHeaders metadata);

    public native ResponseStream<ExportedTableCreationResponse> batch(
        BatchTableRequest requestMessage);

    @JsOverlay
    public final UnaryResponse comboAggregate(
        ComboAggregateRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.ComboAggregateCallbackFn callback) {
        return comboAggregate(
            requestMessage,
            Js.<TableServiceClient.ComboAggregateMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse comboAggregate(
        ComboAggregateRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return comboAggregate(
            requestMessage,
            Js.<TableServiceClient.ComboAggregateMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse comboAggregate(
        ComboAggregateRequest requestMessage,
        TableServiceClient.ComboAggregateMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.ComboAggregateCallbackFn callback) {
        return comboAggregate(
            requestMessage,
            Js.<TableServiceClient.ComboAggregateMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse comboAggregate(
        ComboAggregateRequest requestMessage,
        TableServiceClient.ComboAggregateMetadata_or_callbackFn metadata_or_callback) {
        return comboAggregate(
            requestMessage,
            Js.<TableServiceClient.ComboAggregateMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse comboAggregate(
        ComboAggregateRequest requestMessage,
        TableServiceClient.ComboAggregateMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.ComboAggregateCallbackFn callback);

    public native UnaryResponse comboAggregate(
        ComboAggregateRequest requestMessage,
        TableServiceClient.ComboAggregateMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse crossJoinTables(
        CrossJoinTablesRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.CrossJoinTablesCallbackFn callback) {
        return crossJoinTables(
            requestMessage,
            Js.<TableServiceClient.CrossJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse crossJoinTables(
        CrossJoinTablesRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return crossJoinTables(
            requestMessage,
            Js.<TableServiceClient.CrossJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse crossJoinTables(
        CrossJoinTablesRequest requestMessage,
        TableServiceClient.CrossJoinTablesMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.CrossJoinTablesCallbackFn callback) {
        return crossJoinTables(
            requestMessage,
            Js.<TableServiceClient.CrossJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse crossJoinTables(
        CrossJoinTablesRequest requestMessage,
        TableServiceClient.CrossJoinTablesMetadata_or_callbackFn metadata_or_callback) {
        return crossJoinTables(
            requestMessage,
            Js.<TableServiceClient.CrossJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse crossJoinTables(
        CrossJoinTablesRequest requestMessage,
        TableServiceClient.CrossJoinTablesMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.CrossJoinTablesCallbackFn callback);

    public native UnaryResponse crossJoinTables(
        CrossJoinTablesRequest requestMessage,
        TableServiceClient.CrossJoinTablesMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse dropColumns(
        DropColumnsRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.DropColumnsCallbackFn callback) {
        return dropColumns(
            requestMessage,
            Js.<TableServiceClient.DropColumnsMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse dropColumns(
        DropColumnsRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return dropColumns(
            requestMessage,
            Js.<TableServiceClient.DropColumnsMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse dropColumns(
        DropColumnsRequest requestMessage,
        TableServiceClient.DropColumnsMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.DropColumnsCallbackFn callback) {
        return dropColumns(
            requestMessage,
            Js.<TableServiceClient.DropColumnsMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse dropColumns(
        DropColumnsRequest requestMessage,
        TableServiceClient.DropColumnsMetadata_or_callbackFn metadata_or_callback) {
        return dropColumns(
            requestMessage,
            Js.<TableServiceClient.DropColumnsMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse dropColumns(
        DropColumnsRequest requestMessage,
        TableServiceClient.DropColumnsMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.DropColumnsCallbackFn callback);

    public native UnaryResponse dropColumns(
        DropColumnsRequest requestMessage,
        TableServiceClient.DropColumnsMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse emptyTable(
        EmptyTableRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.EmptyTableCallbackFn callback) {
        return emptyTable(
            requestMessage,
            Js.<TableServiceClient.EmptyTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse emptyTable(
        EmptyTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return emptyTable(
            requestMessage,
            Js.<TableServiceClient.EmptyTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse emptyTable(
        EmptyTableRequest requestMessage,
        TableServiceClient.EmptyTableMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.EmptyTableCallbackFn callback) {
        return emptyTable(
            requestMessage,
            Js.<TableServiceClient.EmptyTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse emptyTable(
        EmptyTableRequest requestMessage,
        TableServiceClient.EmptyTableMetadata_or_callbackFn metadata_or_callback) {
        return emptyTable(
            requestMessage,
            Js.<TableServiceClient.EmptyTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse emptyTable(
        EmptyTableRequest requestMessage,
        TableServiceClient.EmptyTableMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.EmptyTableCallbackFn callback);

    public native UnaryResponse emptyTable(
        EmptyTableRequest requestMessage,
        TableServiceClient.EmptyTableMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse exactJoinTables(
        ExactJoinTablesRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.ExactJoinTablesCallbackFn callback) {
        return exactJoinTables(
            requestMessage,
            Js.<TableServiceClient.ExactJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse exactJoinTables(
        ExactJoinTablesRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return exactJoinTables(
            requestMessage,
            Js.<TableServiceClient.ExactJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse exactJoinTables(
        ExactJoinTablesRequest requestMessage,
        TableServiceClient.ExactJoinTablesMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.ExactJoinTablesCallbackFn callback) {
        return exactJoinTables(
            requestMessage,
            Js.<TableServiceClient.ExactJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse exactJoinTables(
        ExactJoinTablesRequest requestMessage,
        TableServiceClient.ExactJoinTablesMetadata_or_callbackFn metadata_or_callback) {
        return exactJoinTables(
            requestMessage,
            Js.<TableServiceClient.ExactJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse exactJoinTables(
        ExactJoinTablesRequest requestMessage,
        TableServiceClient.ExactJoinTablesMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.ExactJoinTablesCallbackFn callback);

    public native UnaryResponse exactJoinTables(
        ExactJoinTablesRequest requestMessage,
        TableServiceClient.ExactJoinTablesMetadata_or_callbackUnionType metadata_or_callback);

    public native ResponseStream<ExportedTableUpdateMessage> exportedTableUpdates(
        ExportedTableUpdatesRequest requestMessage, BrowserHeaders metadata);

    public native ResponseStream<ExportedTableUpdateMessage> exportedTableUpdates(
        ExportedTableUpdatesRequest requestMessage);

    @JsOverlay
    public final UnaryResponse filter(
        FilterTableRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.FilterCallbackFn callback) {
        return filter(
            requestMessage,
            Js.<TableServiceClient.FilterMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse filter(
        FilterTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return filter(
            requestMessage,
            Js.<TableServiceClient.FilterMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse filter(
        FilterTableRequest requestMessage,
        TableServiceClient.FilterMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.FilterCallbackFn callback) {
        return filter(
            requestMessage,
            Js.<TableServiceClient.FilterMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse filter(
        FilterTableRequest requestMessage,
        TableServiceClient.FilterMetadata_or_callbackFn metadata_or_callback) {
        return filter(
            requestMessage,
            Js.<TableServiceClient.FilterMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse filter(
        FilterTableRequest requestMessage,
        TableServiceClient.FilterMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.FilterCallbackFn callback);

    public native UnaryResponse filter(
        FilterTableRequest requestMessage,
        TableServiceClient.FilterMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse flatten(
        FlattenRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.FlattenCallbackFn callback) {
        return flatten(
            requestMessage,
            Js.<TableServiceClient.FlattenMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse flatten(
        FlattenRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return flatten(
            requestMessage,
            Js.<TableServiceClient.FlattenMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse flatten(
        FlattenRequest requestMessage,
        TableServiceClient.FlattenMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.FlattenCallbackFn callback) {
        return flatten(
            requestMessage,
            Js.<TableServiceClient.FlattenMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse flatten(
        FlattenRequest requestMessage,
        TableServiceClient.FlattenMetadata_or_callbackFn metadata_or_callback) {
        return flatten(
            requestMessage,
            Js.<TableServiceClient.FlattenMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse flatten(
        FlattenRequest requestMessage,
        TableServiceClient.FlattenMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.FlattenCallbackFn callback);

    public native UnaryResponse flatten(
        FlattenRequest requestMessage,
        TableServiceClient.FlattenMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse head(
        HeadOrTailRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.HeadCallbackFn callback) {
        return head(
            requestMessage,
            Js.<TableServiceClient.HeadMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse head(
        HeadOrTailRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return head(
            requestMessage,
            Js.<TableServiceClient.HeadMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse head(
        HeadOrTailRequest requestMessage,
        TableServiceClient.HeadMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.HeadCallbackFn callback) {
        return head(
            requestMessage,
            Js.<TableServiceClient.HeadMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse head(
        HeadOrTailRequest requestMessage,
        TableServiceClient.HeadMetadata_or_callbackFn metadata_or_callback) {
        return head(
            requestMessage,
            Js.<TableServiceClient.HeadMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse head(
        HeadOrTailRequest requestMessage,
        TableServiceClient.HeadMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.HeadCallbackFn callback);

    public native UnaryResponse head(
        HeadOrTailRequest requestMessage,
        TableServiceClient.HeadMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse headBy(
        HeadOrTailByRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.HeadByCallbackFn callback) {
        return headBy(
            requestMessage,
            Js.<TableServiceClient.HeadByMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse headBy(
        HeadOrTailByRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return headBy(
            requestMessage,
            Js.<TableServiceClient.HeadByMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse headBy(
        HeadOrTailByRequest requestMessage,
        TableServiceClient.HeadByMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.HeadByCallbackFn callback) {
        return headBy(
            requestMessage,
            Js.<TableServiceClient.HeadByMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse headBy(
        HeadOrTailByRequest requestMessage,
        TableServiceClient.HeadByMetadata_or_callbackFn metadata_or_callback) {
        return headBy(
            requestMessage,
            Js.<TableServiceClient.HeadByMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse headBy(
        HeadOrTailByRequest requestMessage,
        TableServiceClient.HeadByMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.HeadByCallbackFn callback);

    public native UnaryResponse headBy(
        HeadOrTailByRequest requestMessage,
        TableServiceClient.HeadByMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse lazyUpdate(
        SelectOrUpdateRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.LazyUpdateCallbackFn callback) {
        return lazyUpdate(
            requestMessage,
            Js.<TableServiceClient.LazyUpdateMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse lazyUpdate(
        SelectOrUpdateRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return lazyUpdate(
            requestMessage,
            Js.<TableServiceClient.LazyUpdateMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse lazyUpdate(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.LazyUpdateMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.LazyUpdateCallbackFn callback) {
        return lazyUpdate(
            requestMessage,
            Js.<TableServiceClient.LazyUpdateMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse lazyUpdate(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.LazyUpdateMetadata_or_callbackFn metadata_or_callback) {
        return lazyUpdate(
            requestMessage,
            Js.<TableServiceClient.LazyUpdateMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse lazyUpdate(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.LazyUpdateMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.LazyUpdateCallbackFn callback);

    public native UnaryResponse lazyUpdate(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.LazyUpdateMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse leftJoinTables(
        LeftJoinTablesRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.LeftJoinTablesCallbackFn callback) {
        return leftJoinTables(
            requestMessage,
            Js.<TableServiceClient.LeftJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse leftJoinTables(
        LeftJoinTablesRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return leftJoinTables(
            requestMessage,
            Js.<TableServiceClient.LeftJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse leftJoinTables(
        LeftJoinTablesRequest requestMessage,
        TableServiceClient.LeftJoinTablesMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.LeftJoinTablesCallbackFn callback) {
        return leftJoinTables(
            requestMessage,
            Js.<TableServiceClient.LeftJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse leftJoinTables(
        LeftJoinTablesRequest requestMessage,
        TableServiceClient.LeftJoinTablesMetadata_or_callbackFn metadata_or_callback) {
        return leftJoinTables(
            requestMessage,
            Js.<TableServiceClient.LeftJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse leftJoinTables(
        LeftJoinTablesRequest requestMessage,
        TableServiceClient.LeftJoinTablesMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.LeftJoinTablesCallbackFn callback);

    public native UnaryResponse leftJoinTables(
        LeftJoinTablesRequest requestMessage,
        TableServiceClient.LeftJoinTablesMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse mergeTables(
        MergeTablesRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.MergeTablesCallbackFn callback) {
        return mergeTables(
            requestMessage,
            Js.<TableServiceClient.MergeTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse mergeTables(
        MergeTablesRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return mergeTables(
            requestMessage,
            Js.<TableServiceClient.MergeTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse mergeTables(
        MergeTablesRequest requestMessage,
        TableServiceClient.MergeTablesMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.MergeTablesCallbackFn callback) {
        return mergeTables(
            requestMessage,
            Js.<TableServiceClient.MergeTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse mergeTables(
        MergeTablesRequest requestMessage,
        TableServiceClient.MergeTablesMetadata_or_callbackFn metadata_or_callback) {
        return mergeTables(
            requestMessage,
            Js.<TableServiceClient.MergeTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse mergeTables(
        MergeTablesRequest requestMessage,
        TableServiceClient.MergeTablesMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.MergeTablesCallbackFn callback);

    public native UnaryResponse mergeTables(
        MergeTablesRequest requestMessage,
        TableServiceClient.MergeTablesMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse naturalJoinTables(
        NaturalJoinTablesRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.NaturalJoinTablesCallbackFn callback) {
        return naturalJoinTables(
            requestMessage,
            Js.<TableServiceClient.NaturalJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse naturalJoinTables(
        NaturalJoinTablesRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return naturalJoinTables(
            requestMessage,
            Js.<TableServiceClient.NaturalJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse naturalJoinTables(
        NaturalJoinTablesRequest requestMessage,
        TableServiceClient.NaturalJoinTablesMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.NaturalJoinTablesCallbackFn callback) {
        return naturalJoinTables(
            requestMessage,
            Js.<TableServiceClient.NaturalJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse naturalJoinTables(
        NaturalJoinTablesRequest requestMessage,
        TableServiceClient.NaturalJoinTablesMetadata_or_callbackFn metadata_or_callback) {
        return naturalJoinTables(
            requestMessage,
            Js.<TableServiceClient.NaturalJoinTablesMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse naturalJoinTables(
        NaturalJoinTablesRequest requestMessage,
        TableServiceClient.NaturalJoinTablesMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.NaturalJoinTablesCallbackFn callback);

    public native UnaryResponse naturalJoinTables(
        NaturalJoinTablesRequest requestMessage,
        TableServiceClient.NaturalJoinTablesMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse runChartDownsample(
        RunChartDownsampleRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.RunChartDownsampleCallbackFn callback) {
        return runChartDownsample(
            requestMessage,
            Js.<TableServiceClient.RunChartDownsampleMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse runChartDownsample(
        RunChartDownsampleRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return runChartDownsample(
            requestMessage,
            Js.<TableServiceClient.RunChartDownsampleMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse runChartDownsample(
        RunChartDownsampleRequest requestMessage,
        TableServiceClient.RunChartDownsampleMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.RunChartDownsampleCallbackFn callback) {
        return runChartDownsample(
            requestMessage,
            Js.<TableServiceClient.RunChartDownsampleMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse runChartDownsample(
        RunChartDownsampleRequest requestMessage,
        TableServiceClient.RunChartDownsampleMetadata_or_callbackFn metadata_or_callback) {
        return runChartDownsample(
            requestMessage,
            Js.<TableServiceClient.RunChartDownsampleMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse runChartDownsample(
        RunChartDownsampleRequest requestMessage,
        TableServiceClient.RunChartDownsampleMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.RunChartDownsampleCallbackFn callback);

    public native UnaryResponse runChartDownsample(
        RunChartDownsampleRequest requestMessage,
        TableServiceClient.RunChartDownsampleMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse select(
        SelectOrUpdateRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.SelectCallbackFn callback) {
        return select(
            requestMessage,
            Js.<TableServiceClient.SelectMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse select(
        SelectOrUpdateRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return select(
            requestMessage,
            Js.<TableServiceClient.SelectMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse select(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.SelectMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.SelectCallbackFn callback) {
        return select(
            requestMessage,
            Js.<TableServiceClient.SelectMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse select(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.SelectMetadata_or_callbackFn metadata_or_callback) {
        return select(
            requestMessage,
            Js.<TableServiceClient.SelectMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse select(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.SelectMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.SelectCallbackFn callback);

    public native UnaryResponse select(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.SelectMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse selectDistinct(
        SelectDistinctRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.SelectDistinctCallbackFn callback) {
        return selectDistinct(
            requestMessage,
            Js.<TableServiceClient.SelectDistinctMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse selectDistinct(
        SelectDistinctRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return selectDistinct(
            requestMessage,
            Js.<TableServiceClient.SelectDistinctMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse selectDistinct(
        SelectDistinctRequest requestMessage,
        TableServiceClient.SelectDistinctMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.SelectDistinctCallbackFn callback) {
        return selectDistinct(
            requestMessage,
            Js.<TableServiceClient.SelectDistinctMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse selectDistinct(
        SelectDistinctRequest requestMessage,
        TableServiceClient.SelectDistinctMetadata_or_callbackFn metadata_or_callback) {
        return selectDistinct(
            requestMessage,
            Js.<TableServiceClient.SelectDistinctMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse selectDistinct(
        SelectDistinctRequest requestMessage,
        TableServiceClient.SelectDistinctMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.SelectDistinctCallbackFn callback);

    public native UnaryResponse selectDistinct(
        SelectDistinctRequest requestMessage,
        TableServiceClient.SelectDistinctMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse snapshot(
        SnapshotTableRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.SnapshotCallbackFn callback) {
        return snapshot(
            requestMessage,
            Js.<TableServiceClient.SnapshotMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse snapshot(
        SnapshotTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return snapshot(
            requestMessage,
            Js.<TableServiceClient.SnapshotMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse snapshot(
        SnapshotTableRequest requestMessage,
        TableServiceClient.SnapshotMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.SnapshotCallbackFn callback) {
        return snapshot(
            requestMessage,
            Js.<TableServiceClient.SnapshotMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse snapshot(
        SnapshotTableRequest requestMessage,
        TableServiceClient.SnapshotMetadata_or_callbackFn metadata_or_callback) {
        return snapshot(
            requestMessage,
            Js.<TableServiceClient.SnapshotMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse snapshot(
        SnapshotTableRequest requestMessage,
        TableServiceClient.SnapshotMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.SnapshotCallbackFn callback);

    public native UnaryResponse snapshot(
        SnapshotTableRequest requestMessage,
        TableServiceClient.SnapshotMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse sort(
        SortTableRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.SortCallbackFn callback) {
        return sort(
            requestMessage,
            Js.<TableServiceClient.SortMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse sort(
        SortTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return sort(
            requestMessage,
            Js.<TableServiceClient.SortMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse sort(
        SortTableRequest requestMessage,
        TableServiceClient.SortMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.SortCallbackFn callback) {
        return sort(
            requestMessage,
            Js.<TableServiceClient.SortMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse sort(
        SortTableRequest requestMessage,
        TableServiceClient.SortMetadata_or_callbackFn metadata_or_callback) {
        return sort(
            requestMessage,
            Js.<TableServiceClient.SortMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse sort(
        SortTableRequest requestMessage,
        TableServiceClient.SortMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.SortCallbackFn callback);

    public native UnaryResponse sort(
        SortTableRequest requestMessage,
        TableServiceClient.SortMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse tail(
        HeadOrTailRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.TailCallbackFn callback) {
        return tail(
            requestMessage,
            Js.<TableServiceClient.TailMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse tail(
        HeadOrTailRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return tail(
            requestMessage,
            Js.<TableServiceClient.TailMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse tail(
        HeadOrTailRequest requestMessage,
        TableServiceClient.TailMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.TailCallbackFn callback) {
        return tail(
            requestMessage,
            Js.<TableServiceClient.TailMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse tail(
        HeadOrTailRequest requestMessage,
        TableServiceClient.TailMetadata_or_callbackFn metadata_or_callback) {
        return tail(
            requestMessage,
            Js.<TableServiceClient.TailMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse tail(
        HeadOrTailRequest requestMessage,
        TableServiceClient.TailMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.TailCallbackFn callback);

    public native UnaryResponse tail(
        HeadOrTailRequest requestMessage,
        TableServiceClient.TailMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse tailBy(
        HeadOrTailByRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.TailByCallbackFn callback) {
        return tailBy(
            requestMessage,
            Js.<TableServiceClient.TailByMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse tailBy(
        HeadOrTailByRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return tailBy(
            requestMessage,
            Js.<TableServiceClient.TailByMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse tailBy(
        HeadOrTailByRequest requestMessage,
        TableServiceClient.TailByMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.TailByCallbackFn callback) {
        return tailBy(
            requestMessage,
            Js.<TableServiceClient.TailByMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse tailBy(
        HeadOrTailByRequest requestMessage,
        TableServiceClient.TailByMetadata_or_callbackFn metadata_or_callback) {
        return tailBy(
            requestMessage,
            Js.<TableServiceClient.TailByMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse tailBy(
        HeadOrTailByRequest requestMessage,
        TableServiceClient.TailByMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.TailByCallbackFn callback);

    public native UnaryResponse tailBy(
        HeadOrTailByRequest requestMessage,
        TableServiceClient.TailByMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse timeTable(
        TimeTableRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.TimeTableCallbackFn callback) {
        return timeTable(
            requestMessage,
            Js.<TableServiceClient.TimeTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse timeTable(
        TimeTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return timeTable(
            requestMessage,
            Js.<TableServiceClient.TimeTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse timeTable(
        TimeTableRequest requestMessage,
        TableServiceClient.TimeTableMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.TimeTableCallbackFn callback) {
        return timeTable(
            requestMessage,
            Js.<TableServiceClient.TimeTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse timeTable(
        TimeTableRequest requestMessage,
        TableServiceClient.TimeTableMetadata_or_callbackFn metadata_or_callback) {
        return timeTable(
            requestMessage,
            Js.<TableServiceClient.TimeTableMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse timeTable(
        TimeTableRequest requestMessage,
        TableServiceClient.TimeTableMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.TimeTableCallbackFn callback);

    public native UnaryResponse timeTable(
        TimeTableRequest requestMessage,
        TableServiceClient.TimeTableMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse ungroup(
        UngroupRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.UngroupCallbackFn callback) {
        return ungroup(
            requestMessage,
            Js.<TableServiceClient.UngroupMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse ungroup(
        UngroupRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return ungroup(
            requestMessage,
            Js.<TableServiceClient.UngroupMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse ungroup(
        UngroupRequest requestMessage,
        TableServiceClient.UngroupMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.UngroupCallbackFn callback) {
        return ungroup(
            requestMessage,
            Js.<TableServiceClient.UngroupMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse ungroup(
        UngroupRequest requestMessage,
        TableServiceClient.UngroupMetadata_or_callbackFn metadata_or_callback) {
        return ungroup(
            requestMessage,
            Js.<TableServiceClient.UngroupMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse ungroup(
        UngroupRequest requestMessage,
        TableServiceClient.UngroupMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.UngroupCallbackFn callback);

    public native UnaryResponse ungroup(
        UngroupRequest requestMessage,
        TableServiceClient.UngroupMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse unstructuredFilter(
        UnstructuredFilterTableRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.UnstructuredFilterCallbackFn callback) {
        return unstructuredFilter(
            requestMessage,
            Js.<TableServiceClient.UnstructuredFilterMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse unstructuredFilter(
        UnstructuredFilterTableRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return unstructuredFilter(
            requestMessage,
            Js.<TableServiceClient.UnstructuredFilterMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse unstructuredFilter(
        UnstructuredFilterTableRequest requestMessage,
        TableServiceClient.UnstructuredFilterMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.UnstructuredFilterCallbackFn callback) {
        return unstructuredFilter(
            requestMessage,
            Js.<TableServiceClient.UnstructuredFilterMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse unstructuredFilter(
        UnstructuredFilterTableRequest requestMessage,
        TableServiceClient.UnstructuredFilterMetadata_or_callbackFn metadata_or_callback) {
        return unstructuredFilter(
            requestMessage,
            Js.<TableServiceClient.UnstructuredFilterMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse unstructuredFilter(
        UnstructuredFilterTableRequest requestMessage,
        TableServiceClient.UnstructuredFilterMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.UnstructuredFilterCallbackFn callback);

    public native UnaryResponse unstructuredFilter(
        UnstructuredFilterTableRequest requestMessage,
        TableServiceClient.UnstructuredFilterMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse update(
        SelectOrUpdateRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.UpdateCallbackFn callback) {
        return update(
            requestMessage,
            Js.<TableServiceClient.UpdateMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse update(
        SelectOrUpdateRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return update(
            requestMessage,
            Js.<TableServiceClient.UpdateMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse update(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.UpdateMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.UpdateCallbackFn callback) {
        return update(
            requestMessage,
            Js.<TableServiceClient.UpdateMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse update(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.UpdateMetadata_or_callbackFn metadata_or_callback) {
        return update(
            requestMessage,
            Js.<TableServiceClient.UpdateMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse update(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.UpdateMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.UpdateCallbackFn callback);

    public native UnaryResponse update(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.UpdateMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse updateView(
        SelectOrUpdateRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.UpdateViewCallbackFn callback) {
        return updateView(
            requestMessage,
            Js.<TableServiceClient.UpdateViewMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse updateView(
        SelectOrUpdateRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return updateView(
            requestMessage,
            Js.<TableServiceClient.UpdateViewMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse updateView(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.UpdateViewMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.UpdateViewCallbackFn callback) {
        return updateView(
            requestMessage,
            Js.<TableServiceClient.UpdateViewMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse updateView(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.UpdateViewMetadata_or_callbackFn metadata_or_callback) {
        return updateView(
            requestMessage,
            Js.<TableServiceClient.UpdateViewMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse updateView(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.UpdateViewMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.UpdateViewCallbackFn callback);

    public native UnaryResponse updateView(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.UpdateViewMetadata_or_callbackUnionType metadata_or_callback);

    @JsOverlay
    public final UnaryResponse view(
        SelectOrUpdateRequest requestMessage,
        BrowserHeaders metadata_or_callback,
        TableServiceClient.ViewCallbackFn callback) {
        return view(
            requestMessage,
            Js.<TableServiceClient.ViewMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse view(
        SelectOrUpdateRequest requestMessage, BrowserHeaders metadata_or_callback) {
        return view(
            requestMessage,
            Js.<TableServiceClient.ViewMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    @JsOverlay
    public final UnaryResponse view(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.ViewMetadata_or_callbackFn metadata_or_callback,
        TableServiceClient.ViewCallbackFn callback) {
        return view(
            requestMessage,
            Js.<TableServiceClient.ViewMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback),
            callback);
    }

    @JsOverlay
    public final UnaryResponse view(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.ViewMetadata_or_callbackFn metadata_or_callback) {
        return view(
            requestMessage,
            Js.<TableServiceClient.ViewMetadata_or_callbackUnionType>uncheckedCast(
                metadata_or_callback));
    }

    public native UnaryResponse view(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.ViewMetadata_or_callbackUnionType metadata_or_callback,
        TableServiceClient.ViewCallbackFn callback);

    public native UnaryResponse view(
        SelectOrUpdateRequest requestMessage,
        TableServiceClient.ViewMetadata_or_callbackUnionType metadata_or_callback);
}
