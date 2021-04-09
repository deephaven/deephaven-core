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
    public interface OptionalreferencesListFieldType {
      @JsOverlay
      static SearchCondition.ToObjectReturnType.OptionalreferencesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getColumnname();

      @JsProperty
      void setColumnname(String columnname);
    }

    @JsOverlay
    static SearchCondition.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<SearchCondition.ToObjectReturnType.OptionalreferencesListFieldType>
        getOptionalreferencesList();

    @JsProperty
    String getSearchstring();

    @JsProperty
    void setOptionalreferencesList(
        JsArray<SearchCondition.ToObjectReturnType.OptionalreferencesListFieldType>
            optionalreferencesList);

    @JsOverlay
    default void setOptionalreferencesList(
        SearchCondition.ToObjectReturnType.OptionalreferencesListFieldType[]
            optionalreferencesList) {
      setOptionalreferencesList(
          Js
              .<JsArray<SearchCondition.ToObjectReturnType.OptionalreferencesListFieldType>>
                  uncheckedCast(optionalreferencesList));
    }

    @JsProperty
    void setSearchstring(String searchstring);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface OptionalreferencesListFieldType {
      @JsOverlay
      static SearchCondition.ToObjectReturnType0.OptionalreferencesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getColumnname();

      @JsProperty
      void setColumnname(String columnname);
    }

    @JsOverlay
    static SearchCondition.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<SearchCondition.ToObjectReturnType0.OptionalreferencesListFieldType>
        getOptionalreferencesList();

    @JsProperty
    String getSearchstring();

    @JsProperty
    void setOptionalreferencesList(
        JsArray<SearchCondition.ToObjectReturnType0.OptionalreferencesListFieldType>
            optionalreferencesList);

    @JsOverlay
    default void setOptionalreferencesList(
        SearchCondition.ToObjectReturnType0.OptionalreferencesListFieldType[]
            optionalreferencesList) {
      setOptionalreferencesList(
          Js
              .<JsArray<SearchCondition.ToObjectReturnType0.OptionalreferencesListFieldType>>
                  uncheckedCast(optionalreferencesList));
    }

    @JsProperty
    void setSearchstring(String searchstring);
  }

  public static native SearchCondition deserializeBinary(Uint8Array bytes);

  public static native SearchCondition deserializeBinaryFromReader(
      SearchCondition message, Object reader);

  public static native void serializeBinaryToWriter(SearchCondition message, Object writer);

  public static native SearchCondition.ToObjectReturnType toObject(
      boolean includeInstance, SearchCondition msg);

  public native Reference addOptionalreferences();

  public native Reference addOptionalreferences(Reference value, double index);

  public native Reference addOptionalreferences(Reference value);

  public native void clearOptionalreferencesList();

  public native JsArray<Reference> getOptionalreferencesList();

  public native String getSearchstring();

  public native Uint8Array serializeBinary();

  public native void setOptionalreferencesList(JsArray<Reference> value);

  @JsOverlay
  public final void setOptionalreferencesList(Reference[] value) {
    setOptionalreferencesList(Js.<JsArray<Reference>>uncheckedCast(value));
  }

  public native void setSearchstring(String value);

  public native SearchCondition.ToObjectReturnType0 toObject();

  public native SearchCondition.ToObjectReturnType0 toObject(boolean includeInstance);
}
