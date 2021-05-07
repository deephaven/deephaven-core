package io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.sparsetensor_generated.io.deephaven.barrage;

import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.sparsetensor_generated.io.deephaven.barrage.flatbuf.SparseMatrixIndexCSX;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.sparsetensor_generated.io.deephaven.barrage.flatbuf.SparseTensorIndexCOO;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.barrage.flatbuf.sparsetensor_generated.io.deephaven.barrage.flatbuf.SparseTensorIndexCSF;
import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsOverlay;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.Js;

@JsType(
    isNative = true,
    name =
        "dhinternal.io.deephaven.barrage.flatbuf.SparseTensor_generated.io.deephaven.barrage.flatbuf",
    namespace = JsPackage.GLOBAL)
public class Flatbuf {
  @JsFunction
  public interface UnionListToSparseTensorIndexAccessorFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P1UnionType {
      @JsOverlay
      static Flatbuf.UnionListToSparseTensorIndexAccessorFn.P1UnionType of(Object o) {
        return Js.cast(o);
      }

      @JsOverlay
      default SparseMatrixIndexCSX asSparseMatrixIndexCSX() {
        return Js.cast(this);
      }

      @JsOverlay
      default SparseTensorIndexCOO asSparseTensorIndexCOO() {
        return Js.cast(this);
      }

      @JsOverlay
      default SparseTensorIndexCSF asSparseTensorIndexCSF() {
        return Js.cast(this);
      }

      @JsOverlay
      default boolean isSparseMatrixIndexCSX() {
        return (Object) this instanceof SparseMatrixIndexCSX;
      }

      @JsOverlay
      default boolean isSparseTensorIndexCOO() {
        return (Object) this instanceof SparseTensorIndexCOO;
      }

      @JsOverlay
      default boolean isSparseTensorIndexCSF() {
        return (Object) this instanceof SparseTensorIndexCSF;
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UnionType {
      @JsOverlay
      static Flatbuf.UnionListToSparseTensorIndexAccessorFn.UnionType of(Object o) {
        return Js.cast(o);
      }

      @JsOverlay
      default SparseMatrixIndexCSX asSparseMatrixIndexCSX() {
        return Js.cast(this);
      }

      @JsOverlay
      default SparseTensorIndexCOO asSparseTensorIndexCOO() {
        return Js.cast(this);
      }

      @JsOverlay
      default SparseTensorIndexCSF asSparseTensorIndexCSF() {
        return Js.cast(this);
      }

      @JsOverlay
      default boolean isSparseMatrixIndexCSX() {
        return (Object) this instanceof SparseMatrixIndexCSX;
      }

      @JsOverlay
      default boolean isSparseTensorIndexCOO() {
        return (Object) this instanceof SparseTensorIndexCOO;
      }

      @JsOverlay
      default boolean isSparseTensorIndexCSF() {
        return (Object) this instanceof SparseTensorIndexCSF;
      }
    }

    Flatbuf.UnionListToSparseTensorIndexAccessorFn.UnionType onInvoke(
        double p0, Flatbuf.UnionListToSparseTensorIndexAccessorFn.P1UnionType p1);

    @JsOverlay
    default Flatbuf.UnionListToSparseTensorIndexAccessorFn.UnionType onInvoke(
        double p0, SparseMatrixIndexCSX p1) {
      return onInvoke(
          p0, Js.<Flatbuf.UnionListToSparseTensorIndexAccessorFn.P1UnionType>uncheckedCast(p1));
    }

    @JsOverlay
    default Flatbuf.UnionListToSparseTensorIndexAccessorFn.UnionType onInvoke(
        double p0, SparseTensorIndexCOO p1) {
      return onInvoke(
          p0, Js.<Flatbuf.UnionListToSparseTensorIndexAccessorFn.P1UnionType>uncheckedCast(p1));
    }

    @JsOverlay
    default Flatbuf.UnionListToSparseTensorIndexAccessorFn.UnionType onInvoke(
        double p0, SparseTensorIndexCSF p1) {
      return onInvoke(
          p0, Js.<Flatbuf.UnionListToSparseTensorIndexAccessorFn.P1UnionType>uncheckedCast(p1));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface UnionListToSparseTensorIndexUnionType {
    @JsOverlay
    static Flatbuf.UnionListToSparseTensorIndexUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default SparseMatrixIndexCSX asSparseMatrixIndexCSX() {
      return Js.cast(this);
    }

    @JsOverlay
    default SparseTensorIndexCOO asSparseTensorIndexCOO() {
      return Js.cast(this);
    }

    @JsOverlay
    default SparseTensorIndexCSF asSparseTensorIndexCSF() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isSparseMatrixIndexCSX() {
      return (Object) this instanceof SparseMatrixIndexCSX;
    }

    @JsOverlay
    default boolean isSparseTensorIndexCOO() {
      return (Object) this instanceof SparseTensorIndexCOO;
    }

    @JsOverlay
    default boolean isSparseTensorIndexCSF() {
      return (Object) this instanceof SparseTensorIndexCSF;
    }
  }

  @JsFunction
  public interface UnionToSparseTensorIndexAccessorFn {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface P0UnionType {
      @JsOverlay
      static Flatbuf.UnionToSparseTensorIndexAccessorFn.P0UnionType of(Object o) {
        return Js.cast(o);
      }

      @JsOverlay
      default SparseMatrixIndexCSX asSparseMatrixIndexCSX() {
        return Js.cast(this);
      }

      @JsOverlay
      default SparseTensorIndexCOO asSparseTensorIndexCOO() {
        return Js.cast(this);
      }

      @JsOverlay
      default SparseTensorIndexCSF asSparseTensorIndexCSF() {
        return Js.cast(this);
      }

      @JsOverlay
      default boolean isSparseMatrixIndexCSX() {
        return (Object) this instanceof SparseMatrixIndexCSX;
      }

      @JsOverlay
      default boolean isSparseTensorIndexCOO() {
        return (Object) this instanceof SparseTensorIndexCOO;
      }

      @JsOverlay
      default boolean isSparseTensorIndexCSF() {
        return (Object) this instanceof SparseTensorIndexCSF;
      }
    }

    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface UnionType {
      @JsOverlay
      static Flatbuf.UnionToSparseTensorIndexAccessorFn.UnionType of(Object o) {
        return Js.cast(o);
      }

      @JsOverlay
      default SparseMatrixIndexCSX asSparseMatrixIndexCSX() {
        return Js.cast(this);
      }

      @JsOverlay
      default SparseTensorIndexCOO asSparseTensorIndexCOO() {
        return Js.cast(this);
      }

      @JsOverlay
      default SparseTensorIndexCSF asSparseTensorIndexCSF() {
        return Js.cast(this);
      }

      @JsOverlay
      default boolean isSparseMatrixIndexCSX() {
        return (Object) this instanceof SparseMatrixIndexCSX;
      }

      @JsOverlay
      default boolean isSparseTensorIndexCOO() {
        return (Object) this instanceof SparseTensorIndexCOO;
      }

      @JsOverlay
      default boolean isSparseTensorIndexCSF() {
        return (Object) this instanceof SparseTensorIndexCSF;
      }
    }

    Flatbuf.UnionToSparseTensorIndexAccessorFn.UnionType onInvoke(
        Flatbuf.UnionToSparseTensorIndexAccessorFn.P0UnionType p0);

    @JsOverlay
    default Flatbuf.UnionToSparseTensorIndexAccessorFn.UnionType onInvoke(SparseMatrixIndexCSX p0) {
      return onInvoke(Js.<Flatbuf.UnionToSparseTensorIndexAccessorFn.P0UnionType>uncheckedCast(p0));
    }

    @JsOverlay
    default Flatbuf.UnionToSparseTensorIndexAccessorFn.UnionType onInvoke(SparseTensorIndexCOO p0) {
      return onInvoke(Js.<Flatbuf.UnionToSparseTensorIndexAccessorFn.P0UnionType>uncheckedCast(p0));
    }

    @JsOverlay
    default Flatbuf.UnionToSparseTensorIndexAccessorFn.UnionType onInvoke(SparseTensorIndexCSF p0) {
      return onInvoke(Js.<Flatbuf.UnionToSparseTensorIndexAccessorFn.P0UnionType>uncheckedCast(p0));
    }
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface UnionToSparseTensorIndexUnionType {
    @JsOverlay
    static Flatbuf.UnionToSparseTensorIndexUnionType of(Object o) {
      return Js.cast(o);
    }

    @JsOverlay
    default SparseMatrixIndexCSX asSparseMatrixIndexCSX() {
      return Js.cast(this);
    }

    @JsOverlay
    default SparseTensorIndexCOO asSparseTensorIndexCOO() {
      return Js.cast(this);
    }

    @JsOverlay
    default SparseTensorIndexCSF asSparseTensorIndexCSF() {
      return Js.cast(this);
    }

    @JsOverlay
    default boolean isSparseMatrixIndexCSX() {
      return (Object) this instanceof SparseMatrixIndexCSX;
    }

    @JsOverlay
    default boolean isSparseTensorIndexCOO() {
      return (Object) this instanceof SparseTensorIndexCOO;
    }

    @JsOverlay
    default boolean isSparseTensorIndexCSF() {
      return (Object) this instanceof SparseTensorIndexCSF;
    }
  }

  public static native Flatbuf.UnionListToSparseTensorIndexUnionType unionListToSparseTensorIndex(
      int type,
      Flatbuf.UnionListToSparseTensorIndexAccessorFn accessor,
      double index);

  public static native Flatbuf.UnionToSparseTensorIndexUnionType unionToSparseTensorIndex(
      int type, Flatbuf.UnionToSparseTensorIndexAccessorFn accessor);
}
