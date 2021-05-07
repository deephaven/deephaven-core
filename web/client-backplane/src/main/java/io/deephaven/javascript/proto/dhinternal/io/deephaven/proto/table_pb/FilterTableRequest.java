package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.Ticket;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.FilterTableRequest",
    namespace = JsPackage.GLOBAL)
public class FilterTableRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FiltersListFieldType {
      @JsOverlay
      static FilterTableRequest.ToObjectReturnType.FiltersListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<Object> getChildrenList();

      @JsProperty
      double getDoublevalue();

      @JsProperty
      double getFilterop();

      @JsProperty
      double getLongvalue();

      @JsProperty
      double getNanotimevalue();

      @JsProperty
      String getStringvalue();

      @JsProperty
      boolean isBoolvalue();

      @JsProperty
      void setBoolvalue(boolean boolvalue);

      @JsProperty
      void setChildrenList(JsArray<Object> childrenList);

      @JsOverlay
      default void setChildrenList(Object[] childrenList) {
        setChildrenList(Js.<JsArray<Object>>uncheckedCast(childrenList));
      }

      @JsProperty
      void setDoublevalue(double doublevalue);

      @JsProperty
      void setFilterop(double filterop);

      @JsProperty
      void setLongvalue(double longvalue);

      @JsProperty
      void setNanotimevalue(double nanotimevalue);

      @JsProperty
      void setStringvalue(String stringvalue);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static FilterTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(Object o) {
          return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
          return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
          return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
          return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
          return (Object) this instanceof Uint8Array;
        }
      }

      @JsOverlay
      static FilterTableRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FilterTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(FilterTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<FilterTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<FilterTableRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static FilterTableRequest.ToObjectReturnType.SourceidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getBatchoffset();

      @JsProperty
      Object getTicket();

      @JsProperty
      void setBatchoffset(double batchoffset);

      @JsProperty
      void setTicket(Object ticket);
    }

    @JsOverlay
    static FilterTableRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<FilterTableRequest.ToObjectReturnType.FiltersListFieldType> getFiltersList();

    @JsProperty
    FilterTableRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    FilterTableRequest.ToObjectReturnType.SourceidFieldType getSourceid();

    @JsOverlay
    default void setFiltersList(
        FilterTableRequest.ToObjectReturnType.FiltersListFieldType[] filtersList) {
      setFiltersList(
          Js.<JsArray<FilterTableRequest.ToObjectReturnType.FiltersListFieldType>>uncheckedCast(
              filtersList));
    }

    @JsProperty
    void setFiltersList(
        JsArray<FilterTableRequest.ToObjectReturnType.FiltersListFieldType> filtersList);

    @JsProperty
    void setResultid(FilterTableRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(FilterTableRequest.ToObjectReturnType.SourceidFieldType sourceid);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface FiltersListFieldType {
      @JsOverlay
      static FilterTableRequest.ToObjectReturnType0.FiltersListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      JsArray<Object> getChildrenList();

      @JsProperty
      double getDoublevalue();

      @JsProperty
      double getFilterop();

      @JsProperty
      double getLongvalue();

      @JsProperty
      double getNanotimevalue();

      @JsProperty
      String getStringvalue();

      @JsProperty
      boolean isBoolvalue();

      @JsProperty
      void setBoolvalue(boolean boolvalue);

      @JsProperty
      void setChildrenList(JsArray<Object> childrenList);

      @JsOverlay
      default void setChildrenList(Object[] childrenList) {
        setChildrenList(Js.<JsArray<Object>>uncheckedCast(childrenList));
      }

      @JsProperty
      void setDoublevalue(double doublevalue);

      @JsProperty
      void setFilterop(double filterop);

      @JsProperty
      void setLongvalue(double longvalue);

      @JsProperty
      void setNanotimevalue(double nanotimevalue);

      @JsProperty
      void setStringvalue(String stringvalue);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static FilterTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(
            Object o) {
          return Js.cast(o);
        }

        @JsOverlay
        default String asString() {
          return Js.asString(this);
        }

        @JsOverlay
        default Uint8Array asUint8Array() {
          return Js.cast(this);
        }

        @JsOverlay
        default boolean isString() {
          return (Object) this instanceof String;
        }

        @JsOverlay
        default boolean isUint8Array() {
          return (Object) this instanceof Uint8Array;
        }
      }

      @JsOverlay
      static FilterTableRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      FilterTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(FilterTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<FilterTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<FilterTableRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static FilterTableRequest.ToObjectReturnType0.SourceidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getBatchoffset();

      @JsProperty
      Object getTicket();

      @JsProperty
      void setBatchoffset(double batchoffset);

      @JsProperty
      void setTicket(Object ticket);
    }

    @JsOverlay
    static FilterTableRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<FilterTableRequest.ToObjectReturnType0.FiltersListFieldType> getFiltersList();

    @JsProperty
    FilterTableRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    FilterTableRequest.ToObjectReturnType0.SourceidFieldType getSourceid();

    @JsOverlay
    default void setFiltersList(
        FilterTableRequest.ToObjectReturnType0.FiltersListFieldType[] filtersList) {
      setFiltersList(
          Js.<JsArray<FilterTableRequest.ToObjectReturnType0.FiltersListFieldType>>uncheckedCast(
              filtersList));
    }

    @JsProperty
    void setFiltersList(
        JsArray<FilterTableRequest.ToObjectReturnType0.FiltersListFieldType> filtersList);

    @JsProperty
    void setResultid(FilterTableRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(FilterTableRequest.ToObjectReturnType0.SourceidFieldType sourceid);
  }

  public static native FilterTableRequest deserializeBinary(Uint8Array bytes);

  public static native FilterTableRequest deserializeBinaryFromReader(
      FilterTableRequest message, Object reader);

  public static native void serializeBinaryToWriter(FilterTableRequest message, Object writer);

  public static native FilterTableRequest.ToObjectReturnType toObject(
      boolean includeInstance, FilterTableRequest msg);

  public native FilterDescription addFilters();

  public native FilterDescription addFilters(FilterDescription value, double index);

  public native FilterDescription addFilters(FilterDescription value);

  public native void clearFiltersList();

  public native void clearResultid();

  public native void clearSourceid();

  public native JsArray<FilterDescription> getFiltersList();

  public native Ticket getResultid();

  public native TableReference getSourceid();

  public native boolean hasResultid();

  public native boolean hasSourceid();

  public native Uint8Array serializeBinary();

  @JsOverlay
  public final void setFiltersList(FilterDescription[] value) {
    setFiltersList(Js.<JsArray<FilterDescription>>uncheckedCast(value));
  }

  public native void setFiltersList(JsArray<FilterDescription> value);

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setSourceid();

  public native void setSourceid(TableReference value);

  public native FilterTableRequest.ToObjectReturnType0 toObject();

  public native FilterTableRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
