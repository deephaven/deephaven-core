package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.JsArray;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.session_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.comboaggregaterequest.AggTypeMap;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.comboaggregaterequest.Aggregate;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.ComboAggregateRequest",
    namespace = JsPackage.GLOBAL)
public class ComboAggregateRequest {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AggregatesListFieldType {
      @JsOverlay
      static ComboAggregateRequest.ToObjectReturnType.AggregatesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getColumnname();

      @JsProperty
      JsArray<String> getMatchpairsList();

      @JsProperty
      double getPercentile();

      @JsProperty
      double getType();

      @JsProperty
      boolean isAvgmedian();

      @JsProperty
      void setAvgmedian(boolean avgmedian);

      @JsProperty
      void setColumnname(String columnname);

      @JsProperty
      void setMatchpairsList(JsArray<String> matchpairsList);

      @JsOverlay
      default void setMatchpairsList(String[] matchpairsList) {
        setMatchpairsList(Js.<JsArray<String>>uncheckedCast(matchpairsList));
      }

      @JsProperty
      void setPercentile(double percentile);

      @JsProperty
      void setType(double type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ComboAggregateRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType of(
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
      static ComboAggregateRequest.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ComboAggregateRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(ComboAggregateRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<ComboAggregateRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<ComboAggregateRequest.ToObjectReturnType.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static ComboAggregateRequest.ToObjectReturnType.SourceidFieldType create() {
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
    static ComboAggregateRequest.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<ComboAggregateRequest.ToObjectReturnType.AggregatesListFieldType> getAggregatesList();

    @JsProperty
    JsArray<String> getGroupbycolumnsList();

    @JsProperty
    ComboAggregateRequest.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    ComboAggregateRequest.ToObjectReturnType.SourceidFieldType getSourceid();

    @JsProperty
    boolean isForcecombo();

    @JsOverlay
    default void setAggregatesList(
        ComboAggregateRequest.ToObjectReturnType.AggregatesListFieldType[] aggregatesList) {
      setAggregatesList(
          Js
              .<JsArray<ComboAggregateRequest.ToObjectReturnType.AggregatesListFieldType>>
                  uncheckedCast(aggregatesList));
    }

    @JsProperty
    void setAggregatesList(
        JsArray<ComboAggregateRequest.ToObjectReturnType.AggregatesListFieldType> aggregatesList);

    @JsProperty
    void setForcecombo(boolean forcecombo);

    @JsProperty
    void setGroupbycolumnsList(JsArray<String> groupbycolumnsList);

    @JsOverlay
    default void setGroupbycolumnsList(String[] groupbycolumnsList) {
      setGroupbycolumnsList(Js.<JsArray<String>>uncheckedCast(groupbycolumnsList));
    }

    @JsProperty
    void setResultid(ComboAggregateRequest.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(ComboAggregateRequest.ToObjectReturnType.SourceidFieldType sourceid);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface AggregatesListFieldType {
      @JsOverlay
      static ComboAggregateRequest.ToObjectReturnType0.AggregatesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getColumnname();

      @JsProperty
      JsArray<String> getMatchpairsList();

      @JsProperty
      double getPercentile();

      @JsProperty
      double getType();

      @JsProperty
      boolean isAvgmedian();

      @JsProperty
      void setAvgmedian(boolean avgmedian);

      @JsProperty
      void setColumnname(String columnname);

      @JsProperty
      void setMatchpairsList(JsArray<String> matchpairsList);

      @JsOverlay
      default void setMatchpairsList(String[] matchpairsList) {
        setMatchpairsList(Js.<JsArray<String>>uncheckedCast(matchpairsList));
      }

      @JsProperty
      void setPercentile(double percentile);

      @JsProperty
      void setType(double type);
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface GetIdUnionType {
        @JsOverlay
        static ComboAggregateRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType of(
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
      static ComboAggregateRequest.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ComboAggregateRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType getId();

      @JsProperty
      void setId(ComboAggregateRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType id);

      @JsOverlay
      default void setId(String id) {
        setId(
            Js
                .<ComboAggregateRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }

      @JsOverlay
      default void setId(Uint8Array id) {
        setId(
            Js
                .<ComboAggregateRequest.ToObjectReturnType0.ResultidFieldType.GetIdUnionType>
                    uncheckedCast(id));
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface SourceidFieldType {
      @JsOverlay
      static ComboAggregateRequest.ToObjectReturnType0.SourceidFieldType create() {
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
    static ComboAggregateRequest.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<ComboAggregateRequest.ToObjectReturnType0.AggregatesListFieldType> getAggregatesList();

    @JsProperty
    JsArray<String> getGroupbycolumnsList();

    @JsProperty
    ComboAggregateRequest.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    ComboAggregateRequest.ToObjectReturnType0.SourceidFieldType getSourceid();

    @JsProperty
    boolean isForcecombo();

    @JsOverlay
    default void setAggregatesList(
        ComboAggregateRequest.ToObjectReturnType0.AggregatesListFieldType[] aggregatesList) {
      setAggregatesList(
          Js
              .<JsArray<ComboAggregateRequest.ToObjectReturnType0.AggregatesListFieldType>>
                  uncheckedCast(aggregatesList));
    }

    @JsProperty
    void setAggregatesList(
        JsArray<ComboAggregateRequest.ToObjectReturnType0.AggregatesListFieldType> aggregatesList);

    @JsProperty
    void setForcecombo(boolean forcecombo);

    @JsProperty
    void setGroupbycolumnsList(JsArray<String> groupbycolumnsList);

    @JsOverlay
    default void setGroupbycolumnsList(String[] groupbycolumnsList) {
      setGroupbycolumnsList(Js.<JsArray<String>>uncheckedCast(groupbycolumnsList));
    }

    @JsProperty
    void setResultid(ComboAggregateRequest.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setSourceid(ComboAggregateRequest.ToObjectReturnType0.SourceidFieldType sourceid);
  }

  public static AggTypeMap AggType;

  public static native ComboAggregateRequest deserializeBinary(Uint8Array bytes);

  public static native ComboAggregateRequest deserializeBinaryFromReader(
      ComboAggregateRequest message, Object reader);

  public static native void serializeBinaryToWriter(ComboAggregateRequest message, Object writer);

  public static native ComboAggregateRequest.ToObjectReturnType toObject(
      boolean includeInstance, ComboAggregateRequest msg);

  public native Aggregate addAggregates();

  public native Aggregate addAggregates(Aggregate value, double index);

  public native Aggregate addAggregates(Aggregate value);

  public native String addGroupbycolumns(String value, double index);

  public native String addGroupbycolumns(String value);

  public native void clearAggregatesList();

  public native void clearGroupbycolumnsList();

  public native void clearResultid();

  public native void clearSourceid();

  public native JsArray<Aggregate> getAggregatesList();

  public native boolean getForcecombo();

  public native JsArray<String> getGroupbycolumnsList();

  public native Ticket getResultid();

  public native TableReference getSourceid();

  public native boolean hasResultid();

  public native boolean hasSourceid();

  public native Uint8Array serializeBinary();

  @JsOverlay
  public final void setAggregatesList(Aggregate[] value) {
    setAggregatesList(Js.<JsArray<Aggregate>>uncheckedCast(value));
  }

  public native void setAggregatesList(JsArray<Aggregate> value);

  public native void setForcecombo(boolean value);

  public native void setGroupbycolumnsList(JsArray<String> value);

  @JsOverlay
  public final void setGroupbycolumnsList(String[] value) {
    setGroupbycolumnsList(Js.<JsArray<String>>uncheckedCast(value));
  }

  public native void setResultid();

  public native void setResultid(Ticket value);

  public native void setSourceid();

  public native void setSourceid(TableReference value);

  public native ComboAggregateRequest.ToObjectReturnType0 toObject();

  public native ComboAggregateRequest.ToObjectReturnType0 toObject(boolean includeInstance);
}
