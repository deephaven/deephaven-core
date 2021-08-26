package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
        isNative = true,
        name = "dhinternal.io.deephaven.proto.table_pb.SearchCondition",
        namespace = JsPackage.GLOBAL)
public class SearchCondition {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OptionalReferencesListFieldType {
            @JsOverlay
            static SearchCondition.ToObjectReturnType.OptionalReferencesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnName();

            @JsProperty
            void setColumnName(String columnName);
        }

        @JsOverlay
        static SearchCondition.ToObjectReturnType create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<SearchCondition.ToObjectReturnType.OptionalReferencesListFieldType> getOptionalReferencesList();

        @JsProperty
        String getSearchString();

        @JsProperty
        void setOptionalReferencesList(
                JsArray<SearchCondition.ToObjectReturnType.OptionalReferencesListFieldType> optionalReferencesList);

        @JsOverlay
        default void setOptionalReferencesList(
                SearchCondition.ToObjectReturnType.OptionalReferencesListFieldType[] optionalReferencesList) {
            setOptionalReferencesList(
                    Js.<JsArray<SearchCondition.ToObjectReturnType.OptionalReferencesListFieldType>>uncheckedCast(
                            optionalReferencesList));
        }

        @JsProperty
        void setSearchString(String searchString);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ToObjectReturnType0 {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface OptionalReferencesListFieldType {
            @JsOverlay
            static SearchCondition.ToObjectReturnType0.OptionalReferencesListFieldType create() {
                return Js.uncheckedCast(JsPropertyMap.of());
            }

            @JsProperty
            String getColumnName();

            @JsProperty
            void setColumnName(String columnName);
        }

        @JsOverlay
        static SearchCondition.ToObjectReturnType0 create() {
            return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        JsArray<SearchCondition.ToObjectReturnType0.OptionalReferencesListFieldType> getOptionalReferencesList();

        @JsProperty
        String getSearchString();

        @JsProperty
        void setOptionalReferencesList(
                JsArray<SearchCondition.ToObjectReturnType0.OptionalReferencesListFieldType> optionalReferencesList);

        @JsOverlay
        default void setOptionalReferencesList(
                SearchCondition.ToObjectReturnType0.OptionalReferencesListFieldType[] optionalReferencesList) {
            setOptionalReferencesList(
                    Js.<JsArray<SearchCondition.ToObjectReturnType0.OptionalReferencesListFieldType>>uncheckedCast(
                            optionalReferencesList));
        }

        @JsProperty
        void setSearchString(String searchString);
    }

    public static native SearchCondition deserializeBinary(Uint8Array bytes);

    public static native SearchCondition deserializeBinaryFromReader(
            SearchCondition message, Object reader);

    public static native void serializeBinaryToWriter(SearchCondition message, Object writer);

    public static native SearchCondition.ToObjectReturnType toObject(
            boolean includeInstance, SearchCondition msg);

    public native Reference addOptionalReferences();

    public native Reference addOptionalReferences(Reference value, double index);

    public native Reference addOptionalReferences(Reference value);

    public native void clearOptionalReferencesList();

    public native JsArray<Reference> getOptionalReferencesList();

    public native String getSearchString();

    public native Uint8Array serializeBinary();

    public native void setOptionalReferencesList(JsArray<Reference> value);

    @JsOverlay
    public final void setOptionalReferencesList(Reference[] value) {
        setOptionalReferencesList(Js.<JsArray<Reference>>uncheckedCast(value));
    }

    public native void setSearchString(String value);

    public native SearchCondition.ToObjectReturnType0 toObject();

    public native SearchCondition.ToObjectReturnType0 toObject(boolean includeInstance);
}
