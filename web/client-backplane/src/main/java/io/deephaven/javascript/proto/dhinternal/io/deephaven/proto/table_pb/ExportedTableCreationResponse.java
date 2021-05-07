package io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb;

import elemental2.core.Uint8Array;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsProperty;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;
import jsinterop.base.JsPropertyMap;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.proto.table_pb.ExportedTableCreationResponse",
    namespace = JsPackage.GLOBAL)
public class ExportedTableCreationResponse {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface GetSchemaHeaderUnionType {
    @JsOverlay
    static ExportedTableCreationResponse.GetSchemaHeaderUnionType of(Object o) {
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

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface SetSchemaHeaderValueUnionType {
    @JsOverlay
    static ExportedTableCreationResponse.SetSchemaHeaderValueUnionType of(Object o) {
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

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetSchemaHeaderUnionType {
      @JsOverlay
      static ExportedTableCreationResponse.ToObjectReturnType.GetSchemaHeaderUnionType of(
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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface TicketFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetIdUnionType {
          @JsOverlay
          static ExportedTableCreationResponse.ToObjectReturnType.ResultidFieldType.TicketFieldType
                  .GetIdUnionType
              of(Object o) {
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
        static ExportedTableCreationResponse.ToObjectReturnType.ResultidFieldType.TicketFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ExportedTableCreationResponse.ToObjectReturnType.ResultidFieldType.TicketFieldType
                .GetIdUnionType
            getId();

        @JsProperty
        void setId(
            ExportedTableCreationResponse.ToObjectReturnType.ResultidFieldType.TicketFieldType
                    .GetIdUnionType
                id);

        @JsOverlay
        default void setId(String id) {
          setId(
              Js
                  .<ExportedTableCreationResponse.ToObjectReturnType.ResultidFieldType
                          .TicketFieldType.GetIdUnionType>
                      uncheckedCast(id));
        }

        @JsOverlay
        default void setId(Uint8Array id) {
          setId(
              Js
                  .<ExportedTableCreationResponse.ToObjectReturnType.ResultidFieldType
                          .TicketFieldType.GetIdUnionType>
                      uncheckedCast(id));
        }
      }

      @JsOverlay
      static ExportedTableCreationResponse.ToObjectReturnType.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getBatchoffset();

      @JsProperty
      ExportedTableCreationResponse.ToObjectReturnType.ResultidFieldType.TicketFieldType
          getTicket();

      @JsProperty
      void setBatchoffset(double batchoffset);

      @JsProperty
      void setTicket(
          ExportedTableCreationResponse.ToObjectReturnType.ResultidFieldType.TicketFieldType
              ticket);
    }

    @JsOverlay
    static ExportedTableCreationResponse.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getErrorinfo();

    @JsProperty
    ExportedTableCreationResponse.ToObjectReturnType.ResultidFieldType getResultid();

    @JsProperty
    ExportedTableCreationResponse.ToObjectReturnType.GetSchemaHeaderUnionType getSchemaHeader();

    @JsProperty
    String getSize();

    @JsProperty
    boolean isIsstatic();

    @JsProperty
    boolean isSuccess();

    @JsProperty
    void setErrorinfo(String errorinfo);

    @JsProperty
    void setIsstatic(boolean isstatic);

    @JsProperty
    void setResultid(ExportedTableCreationResponse.ToObjectReturnType.ResultidFieldType resultid);

    @JsProperty
    void setSchemaHeader(
        ExportedTableCreationResponse.ToObjectReturnType.GetSchemaHeaderUnionType schemaHeader);

    @JsOverlay
    default void setSchemaHeader(String schemaHeader) {
      setSchemaHeader(
          Js
              .<ExportedTableCreationResponse.ToObjectReturnType.GetSchemaHeaderUnionType>
                  uncheckedCast(schemaHeader));
    }

    @JsOverlay
    default void setSchemaHeader(Uint8Array schemaHeader) {
      setSchemaHeader(
          Js
              .<ExportedTableCreationResponse.ToObjectReturnType.GetSchemaHeaderUnionType>
                  uncheckedCast(schemaHeader));
    }

    @JsProperty
    void setSize(String size);

    @JsProperty
    void setSuccess(boolean success);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface GetSchemaHeaderUnionType {
      @JsOverlay
      static ExportedTableCreationResponse.ToObjectReturnType0.GetSchemaHeaderUnionType of(
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

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ResultidFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface TicketFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetIdUnionType {
          @JsOverlay
          static ExportedTableCreationResponse.ToObjectReturnType0.ResultidFieldType.TicketFieldType
                  .GetIdUnionType
              of(Object o) {
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
        static ExportedTableCreationResponse.ToObjectReturnType0.ResultidFieldType.TicketFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ExportedTableCreationResponse.ToObjectReturnType0.ResultidFieldType.TicketFieldType
                .GetIdUnionType
            getId();

        @JsProperty
        void setId(
            ExportedTableCreationResponse.ToObjectReturnType0.ResultidFieldType.TicketFieldType
                    .GetIdUnionType
                id);

        @JsOverlay
        default void setId(String id) {
          setId(
              Js
                  .<ExportedTableCreationResponse.ToObjectReturnType0.ResultidFieldType
                          .TicketFieldType.GetIdUnionType>
                      uncheckedCast(id));
        }

        @JsOverlay
        default void setId(Uint8Array id) {
          setId(
              Js
                  .<ExportedTableCreationResponse.ToObjectReturnType0.ResultidFieldType
                          .TicketFieldType.GetIdUnionType>
                      uncheckedCast(id));
        }
      }

      @JsOverlay
      static ExportedTableCreationResponse.ToObjectReturnType0.ResultidFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      double getBatchoffset();

      @JsProperty
      ExportedTableCreationResponse.ToObjectReturnType0.ResultidFieldType.TicketFieldType
          getTicket();

      @JsProperty
      void setBatchoffset(double batchoffset);

      @JsProperty
      void setTicket(
          ExportedTableCreationResponse.ToObjectReturnType0.ResultidFieldType.TicketFieldType
              ticket);
    }

    @JsOverlay
    static ExportedTableCreationResponse.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    String getErrorinfo();

    @JsProperty
    ExportedTableCreationResponse.ToObjectReturnType0.ResultidFieldType getResultid();

    @JsProperty
    ExportedTableCreationResponse.ToObjectReturnType0.GetSchemaHeaderUnionType getSchemaHeader();

    @JsProperty
    String getSize();

    @JsProperty
    boolean isIsstatic();

    @JsProperty
    boolean isSuccess();

    @JsProperty
    void setErrorinfo(String errorinfo);

    @JsProperty
    void setIsstatic(boolean isstatic);

    @JsProperty
    void setResultid(ExportedTableCreationResponse.ToObjectReturnType0.ResultidFieldType resultid);

    @JsProperty
    void setSchemaHeader(
        ExportedTableCreationResponse.ToObjectReturnType0.GetSchemaHeaderUnionType schemaHeader);

    @JsOverlay
    default void setSchemaHeader(String schemaHeader) {
      setSchemaHeader(
          Js
              .<ExportedTableCreationResponse.ToObjectReturnType0.GetSchemaHeaderUnionType>
                  uncheckedCast(schemaHeader));
    }

    @JsOverlay
    default void setSchemaHeader(Uint8Array schemaHeader) {
      setSchemaHeader(
          Js
              .<ExportedTableCreationResponse.ToObjectReturnType0.GetSchemaHeaderUnionType>
                  uncheckedCast(schemaHeader));
    }

    @JsProperty
    void setSize(String size);

    @JsProperty
    void setSuccess(boolean success);
  }

  public static native ExportedTableCreationResponse deserializeBinary(Uint8Array bytes);

  public static native ExportedTableCreationResponse deserializeBinaryFromReader(
      ExportedTableCreationResponse message, Object reader);

  public static native void serializeBinaryToWriter(
      ExportedTableCreationResponse message, Object writer);

  public static native ExportedTableCreationResponse.ToObjectReturnType toObject(
      boolean includeInstance, ExportedTableCreationResponse msg);

  public native void clearResultid();

  public native String getErrorinfo();

  public native boolean getIsstatic();

  public native TableReference getResultid();

  public native ExportedTableCreationResponse.GetSchemaHeaderUnionType getSchemaHeader();

  public native String getSchemaHeader_asB64();

  public native Uint8Array getSchemaHeader_asU8();

  public native String getSize();

  public native boolean getSuccess();

  public native boolean hasResultid();

  public native Uint8Array serializeBinary();

  public native void setErrorinfo(String value);

  public native void setIsstatic(boolean value);

  public native void setResultid();

  public native void setResultid(TableReference value);

  public native void setSchemaHeader(
      ExportedTableCreationResponse.SetSchemaHeaderValueUnionType value);

  @JsOverlay
  public final void setSchemaHeader(String value) {
    setSchemaHeader(
        Js.<ExportedTableCreationResponse.SetSchemaHeaderValueUnionType>uncheckedCast(value));
  }

  @JsOverlay
  public final void setSchemaHeader(Uint8Array value) {
    setSchemaHeader(
        Js.<ExportedTableCreationResponse.SetSchemaHeaderValueUnionType>uncheckedCast(value));
  }

  public native void setSize(String value);

  public native void setSuccess(boolean value);

  public native ExportedTableCreationResponse.ToObjectReturnType0 toObject();

  public native ExportedTableCreationResponse.ToObjectReturnType0 toObject(boolean includeInstance);
}
