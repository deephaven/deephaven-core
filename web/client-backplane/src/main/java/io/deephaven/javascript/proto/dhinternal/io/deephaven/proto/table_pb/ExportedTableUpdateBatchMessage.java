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
    name = "dhinternal.io.deephaven.proto.table_pb.ExportedTableUpdateBatchMessage",
    namespace = JsPackage.GLOBAL)
public class ExportedTableUpdateBatchMessage {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UpdatesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ExportidFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetIdUnionType {
          @JsOverlay
          static ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType
                  .ExportidFieldType.GetIdUnionType
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
        static ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType
                .ExportidFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType.ExportidFieldType
                .GetIdUnionType
            getId();

        @JsProperty
        void setId(
            ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType
                    .ExportidFieldType.GetIdUnionType
                id);

        @JsOverlay
        default void setId(String id) {
          setId(
              Js
                  .<ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType
                          .ExportidFieldType.GetIdUnionType>
                      uncheckedCast(id));
        }

        @JsOverlay
        default void setId(Uint8Array id) {
          setId(
              Js
                  .<ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType
                          .ExportidFieldType.GetIdUnionType>
                      uncheckedCast(id));
        }
      }

      @JsOverlay
      static ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType.ExportidFieldType
          getExportid();

      @JsProperty
      String getSize();

      @JsProperty
      String getUpdatefailuremessage();

      @JsProperty
      void setExportid(
          ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType.ExportidFieldType
              exportid);

      @JsProperty
      void setSize(String size);

      @JsProperty
      void setUpdatefailuremessage(String updatefailuremessage);
    }

    @JsOverlay
    static ExportedTableUpdateBatchMessage.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType>
        getUpdatesList();

    @JsProperty
    void setUpdatesList(
        JsArray<ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType>
            updatesList);

    @JsOverlay
    default void setUpdatesList(
        ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType[] updatesList) {
      setUpdatesList(
          Js
              .<JsArray<ExportedTableUpdateBatchMessage.ToObjectReturnType.UpdatesListFieldType>>
                  uncheckedCast(updatesList));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UpdatesListFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ExportidFieldType {
        @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
        public interface GetIdUnionType {
          @JsOverlay
          static ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType
                  .ExportidFieldType.GetIdUnionType
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
        static ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType
                .ExportidFieldType
            create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType.ExportidFieldType
                .GetIdUnionType
            getId();

        @JsProperty
        void setId(
            ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType
                    .ExportidFieldType.GetIdUnionType
                id);

        @JsOverlay
        default void setId(String id) {
          setId(
              Js
                  .<ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType
                          .ExportidFieldType.GetIdUnionType>
                      uncheckedCast(id));
        }

        @JsOverlay
        default void setId(Uint8Array id) {
          setId(
              Js
                  .<ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType
                          .ExportidFieldType.GetIdUnionType>
                      uncheckedCast(id));
        }
      }

      @JsOverlay
      static ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType.ExportidFieldType
          getExportid();

      @JsProperty
      String getSize();

      @JsProperty
      String getUpdatefailuremessage();

      @JsProperty
      void setExportid(
          ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType.ExportidFieldType
              exportid);

      @JsProperty
      void setSize(String size);

      @JsProperty
      void setUpdatefailuremessage(String updatefailuremessage);
    }

    @JsOverlay
    static ExportedTableUpdateBatchMessage.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType>
        getUpdatesList();

    @JsProperty
    void setUpdatesList(
        JsArray<ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType>
            updatesList);

    @JsOverlay
    default void setUpdatesList(
        ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType[] updatesList) {
      setUpdatesList(
          Js
              .<JsArray<ExportedTableUpdateBatchMessage.ToObjectReturnType0.UpdatesListFieldType>>
                  uncheckedCast(updatesList));
    }
  }

  public static native ExportedTableUpdateBatchMessage deserializeBinary(Uint8Array bytes);

  public static native ExportedTableUpdateBatchMessage deserializeBinaryFromReader(
      ExportedTableUpdateBatchMessage message, Object reader);

  public static native void serializeBinaryToWriter(
      ExportedTableUpdateBatchMessage message, Object writer);

  public static native ExportedTableUpdateBatchMessage.ToObjectReturnType toObject(
      boolean includeInstance, ExportedTableUpdateBatchMessage msg);

  public native ExportedTableUpdateMessage addUpdates();

  public native ExportedTableUpdateMessage addUpdates(
      ExportedTableUpdateMessage value, double index);

  public native ExportedTableUpdateMessage addUpdates(ExportedTableUpdateMessage value);

  public native void clearUpdatesList();

  public native JsArray<ExportedTableUpdateMessage> getUpdatesList();

  public native Uint8Array serializeBinary();

  @JsOverlay
  public final void setUpdatesList(ExportedTableUpdateMessage[] value) {
    setUpdatesList(Js.<JsArray<ExportedTableUpdateMessage>>uncheckedCast(value));
  }

  public native void setUpdatesList(JsArray<ExportedTableUpdateMessage> value);

  public native ExportedTableUpdateBatchMessage.ToObjectReturnType0 toObject();

  public native ExportedTableUpdateBatchMessage.ToObjectReturnType0 toObject(
      boolean includeInstance);
}
