package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.message_generated.io.deephaven.barrage;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.barrage_generated.io.deephaven.barrage.flatbuf.BarrageRecordBatch;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.message_generated.io.deephaven.barrage.flatbuf.DictionaryBatch;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.message_generated.io.deephaven.barrage.flatbuf.RecordBatch;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.schema_generated.io.deephaven.barrage.flatbuf.Schema;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.sparsetensor_generated.io.deephaven.barrage.flatbuf.SparseTensor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.tensor_generated.io.deephaven.barrage.flatbuf.Tensor;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name = "dhinternal.io.deephaven.barrage.flatbuf.Message_generated.io.deephaven.barrage.flatbuf",
    namespace = JsPackage.GLOBAL)
public class Flatbuf {
  @JsFunction
  public interface UnionListToMessageHeaderAccessorFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P1UnionType {
      @JsOverlay
      static Flatbuf.UnionListToMessageHeaderAccessorFn.P1UnionType of(Object o) {
        return Js.cast(o);
      }

      @JsOverlay
      default BarrageRecordBatch asBarrageRecordBatch() {
        return Js.cast(this);
      }

      @JsOverlay
      default DictionaryBatch asDictionaryBatch() {
        return Js.cast(this);
      }

      @JsOverlay
      default RecordBatch asRecordBatch() {
        return Js.cast(this);
      }

      @JsOverlay
      default Schema asSchema() {
        return Js.cast(this);
      }

      @JsOverlay
      default SparseTensor asSparseTensor() {
        return Js.cast(this);
      }

      @JsOverlay
      default Tensor asTensor() {
        return Js.cast(this);
      }

      @JsOverlay
      default boolean isBarrageRecordBatch() {
        return (Object) this instanceof BarrageRecordBatch;
      }

      @JsOverlay
      default boolean isDictionaryBatch() {
        return (Object) this instanceof DictionaryBatch;
      }

      @JsOverlay
      default boolean isRecordBatch() {
        return (Object) this instanceof RecordBatch;
      }

      @JsOverlay
      default boolean isSchema() {
        return (Object) this instanceof Schema;
      }

      @JsOverlay
      default boolean isSparseTensor() {
        return (Object) this instanceof SparseTensor;
      }

      @JsOverlay
      default boolean isTensor() {
        return (Object) this instanceof Tensor;
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UnionType {
      @JsOverlay
      static Flatbuf.UnionListToMessageHeaderAccessorFn.UnionType of(Object o) {
        return Js.cast(o);
      }

      @JsOverlay
      default BarrageRecordBatch asBarrageRecordBatch() {
        return Js.cast(this);
      }

      @JsOverlay
      default DictionaryBatch asDictionaryBatch() {
        return Js.cast(this);
      }

      @JsOverlay
      default RecordBatch asRecordBatch() {
        return Js.cast(this);
      }

      @JsOverlay
      default Schema asSchema() {
        return Js.cast(this);
      }

      @JsOverlay
      default SparseTensor asSparseTensor() {
        return Js.cast(this);
      }

      @JsOverlay
      default Tensor asTensor() {
        return Js.cast(this);
      }

      @JsOverlay
      default boolean isBarrageRecordBatch() {
        return (Object) this instanceof BarrageRecordBatch;
      }

      @JsOverlay
      default boolean isDictionaryBatch() {
        return (Object) this instanceof DictionaryBatch;
      }

      @JsOverlay
      default boolean isRecordBatch() {
        return (Object) this instanceof RecordBatch;
      }

      @JsOverlay
      default boolean isSchema() {
        return (Object) this instanceof Schema;
      }

      @JsOverlay
      default boolean isSparseTensor() {
        return (Object) this instanceof SparseTensor;
      }

      @JsOverlay
      default boolean isTensor() {
        return (Object) this instanceof Tensor;
      }
    }

    @JsOverlay
    default Flatbuf.UnionListToMessageHeaderAccessorFn.UnionType onInvoke(
        double p0, BarrageRecordBatch p1) {
      return onInvoke(
          p0, Js.<Flatbuf.UnionListToMessageHeaderAccessorFn.P1UnionType>uncheckedCast(p1));
    }

    @JsOverlay
    default Flatbuf.UnionListToMessageHeaderAccessorFn.UnionType onInvoke(
        double p0, DictionaryBatch p1) {
      return onInvoke(
          p0, Js.<Flatbuf.UnionListToMessageHeaderAccessorFn.P1UnionType>uncheckedCast(p1));
    }

    Flatbuf.UnionListToMessageHeaderAccessorFn.UnionType onInvoke(
        double p0, Flatbuf.UnionListToMessageHeaderAccessorFn.P1UnionType p1);

    @JsOverlay
    default Flatbuf.UnionListToMessageHeaderAccessorFn.UnionType onInvoke(
        double p0, RecordBatch p1) {
      return onInvoke(
          p0, Js.<Flatbuf.UnionListToMessageHeaderAccessorFn.P1UnionType>uncheckedCast(p1));
    }

    @JsOverlay
    default Flatbuf.UnionListToMessageHeaderAccessorFn.UnionType onInvoke(double p0, Schema p1) {
      return onInvoke(
          p0, Js.<Flatbuf.UnionListToMessageHeaderAccessorFn.P1UnionType>uncheckedCast(p1));
    }

    @JsOverlay
    default Flatbuf.UnionListToMessageHeaderAccessorFn.UnionType onInvoke(
        double p0, SparseTensor p1) {
      return onInvoke(
          p0, Js.<Flatbuf.UnionListToMessageHeaderAccessorFn.P1UnionType>uncheckedCast(p1));
    }

    @JsOverlay
    default Flatbuf.UnionListToMessageHeaderAccessorFn.UnionType onInvoke(double p0, Tensor p1) {
      return onInvoke(
          p0, Js.<Flatbuf.UnionListToMessageHeaderAccessorFn.P1UnionType>uncheckedCast(p1));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface UnionListToMessageHeaderUnionType {
    @JsOverlay
    static Flatbuf.UnionListToMessageHeaderUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default BarrageRecordBatch asBarrageRecordBatch() {
      return Js.cast(this);
    }

    @JsOverlay
    default DictionaryBatch asDictionaryBatch() {
      return Js.cast(this);
    }

    @JsOverlay
    default RecordBatch asRecordBatch() {
      return Js.cast(this);
    }

    @JsOverlay
    default Schema asSchema() {
      return Js.cast(this);
    }

    @JsOverlay
    default SparseTensor asSparseTensor() {
      return Js.cast(this);
    }

    @JsOverlay
    default Tensor asTensor() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isBarrageRecordBatch() {
      return (Object) this instanceof BarrageRecordBatch;
    }

    @JsOverlay
    default boolean isDictionaryBatch() {
      return (Object) this instanceof DictionaryBatch;
    }

    @JsOverlay
    default boolean isRecordBatch() {
      return (Object) this instanceof RecordBatch;
    }

    @JsOverlay
    default boolean isSchema() {
      return (Object) this instanceof Schema;
    }

    @JsOverlay
    default boolean isSparseTensor() {
      return (Object) this instanceof SparseTensor;
    }

    @JsOverlay
    default boolean isTensor() {
      return (Object) this instanceof Tensor;
    }
  }

  @JsFunction
  public interface UnionToMessageHeaderAccessorFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0UnionType {
      @JsOverlay
      static Flatbuf.UnionToMessageHeaderAccessorFn.P0UnionType of(Object o) {
        return Js.cast(o);
      }

      @JsOverlay
      default BarrageRecordBatch asBarrageRecordBatch() {
        return Js.cast(this);
      }

      @JsOverlay
      default DictionaryBatch asDictionaryBatch() {
        return Js.cast(this);
      }

      @JsOverlay
      default RecordBatch asRecordBatch() {
        return Js.cast(this);
      }

      @JsOverlay
      default Schema asSchema() {
        return Js.cast(this);
      }

      @JsOverlay
      default SparseTensor asSparseTensor() {
        return Js.cast(this);
      }

      @JsOverlay
      default Tensor asTensor() {
        return Js.cast(this);
      }

      @JsOverlay
      default boolean isBarrageRecordBatch() {
        return (Object) this instanceof BarrageRecordBatch;
      }

      @JsOverlay
      default boolean isDictionaryBatch() {
        return (Object) this instanceof DictionaryBatch;
      }

      @JsOverlay
      default boolean isRecordBatch() {
        return (Object) this instanceof RecordBatch;
      }

      @JsOverlay
      default boolean isSchema() {
        return (Object) this instanceof Schema;
      }

      @JsOverlay
      default boolean isSparseTensor() {
        return (Object) this instanceof SparseTensor;
      }

      @JsOverlay
      default boolean isTensor() {
        return (Object) this instanceof Tensor;
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UnionType {
      @JsOverlay
      static Flatbuf.UnionToMessageHeaderAccessorFn.UnionType of(Object o) {
        return Js.cast(o);
      }

      @JsOverlay
      default BarrageRecordBatch asBarrageRecordBatch() {
        return Js.cast(this);
      }

      @JsOverlay
      default DictionaryBatch asDictionaryBatch() {
        return Js.cast(this);
      }

      @JsOverlay
      default RecordBatch asRecordBatch() {
        return Js.cast(this);
      }

      @JsOverlay
      default Schema asSchema() {
        return Js.cast(this);
      }

      @JsOverlay
      default SparseTensor asSparseTensor() {
        return Js.cast(this);
      }

      @JsOverlay
      default Tensor asTensor() {
        return Js.cast(this);
      }

      @JsOverlay
      default boolean isBarrageRecordBatch() {
        return (Object) this instanceof BarrageRecordBatch;
      }

      @JsOverlay
      default boolean isDictionaryBatch() {
        return (Object) this instanceof DictionaryBatch;
      }

      @JsOverlay
      default boolean isRecordBatch() {
        return (Object) this instanceof RecordBatch;
      }

      @JsOverlay
      default boolean isSchema() {
        return (Object) this instanceof Schema;
      }

      @JsOverlay
      default boolean isSparseTensor() {
        return (Object) this instanceof SparseTensor;
      }

      @JsOverlay
      default boolean isTensor() {
        return (Object) this instanceof Tensor;
      }
    }

    @JsOverlay
    default Flatbuf.UnionToMessageHeaderAccessorFn.UnionType onInvoke(BarrageRecordBatch p0) {
      return onInvoke(Js.<Flatbuf.UnionToMessageHeaderAccessorFn.P0UnionType>uncheckedCast(p0));
    }

    @JsOverlay
    default Flatbuf.UnionToMessageHeaderAccessorFn.UnionType onInvoke(DictionaryBatch p0) {
      return onInvoke(Js.<Flatbuf.UnionToMessageHeaderAccessorFn.P0UnionType>uncheckedCast(p0));
    }

    Flatbuf.UnionToMessageHeaderAccessorFn.UnionType onInvoke(
        Flatbuf.UnionToMessageHeaderAccessorFn.P0UnionType p0);

    @JsOverlay
    default Flatbuf.UnionToMessageHeaderAccessorFn.UnionType onInvoke(RecordBatch p0) {
      return onInvoke(Js.<Flatbuf.UnionToMessageHeaderAccessorFn.P0UnionType>uncheckedCast(p0));
    }

    @JsOverlay
    default Flatbuf.UnionToMessageHeaderAccessorFn.UnionType onInvoke(Schema p0) {
      return onInvoke(Js.<Flatbuf.UnionToMessageHeaderAccessorFn.P0UnionType>uncheckedCast(p0));
    }

    @JsOverlay
    default Flatbuf.UnionToMessageHeaderAccessorFn.UnionType onInvoke(SparseTensor p0) {
      return onInvoke(Js.<Flatbuf.UnionToMessageHeaderAccessorFn.P0UnionType>uncheckedCast(p0));
    }

    @JsOverlay
    default Flatbuf.UnionToMessageHeaderAccessorFn.UnionType onInvoke(Tensor p0) {
      return onInvoke(Js.<Flatbuf.UnionToMessageHeaderAccessorFn.P0UnionType>uncheckedCast(p0));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface UnionToMessageHeaderUnionType {
    @JsOverlay
    static Flatbuf.UnionToMessageHeaderUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default BarrageRecordBatch asBarrageRecordBatch() {
      return Js.cast(this);
    }

    @JsOverlay
    default DictionaryBatch asDictionaryBatch() {
      return Js.cast(this);
    }

    @JsOverlay
    default RecordBatch asRecordBatch() {
      return Js.cast(this);
    }

    @JsOverlay
    default Schema asSchema() {
      return Js.cast(this);
    }

    @JsOverlay
    default SparseTensor asSparseTensor() {
      return Js.cast(this);
    }

    @JsOverlay
    default Tensor asTensor() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isBarrageRecordBatch() {
      return (Object) this instanceof BarrageRecordBatch;
    }

    @JsOverlay
    default boolean isDictionaryBatch() {
      return (Object) this instanceof DictionaryBatch;
    }

    @JsOverlay
    default boolean isRecordBatch() {
      return (Object) this instanceof RecordBatch;
    }

    @JsOverlay
    default boolean isSchema() {
      return (Object) this instanceof Schema;
    }

    @JsOverlay
    default boolean isSparseTensor() {
      return (Object) this instanceof SparseTensor;
    }

    @JsOverlay
    default boolean isTensor() {
      return (Object) this instanceof Tensor;
    }
  }

  public static native Flatbuf.UnionListToMessageHeaderUnionType unionListToMessageHeader(
      int type, Flatbuf.UnionListToMessageHeaderAccessorFn accessor, double index);

  public static native Flatbuf.UnionToMessageHeaderUnionType unionToMessageHeader(
      int type, Flatbuf.UnionToMessageHeaderAccessorFn accessor);
}
