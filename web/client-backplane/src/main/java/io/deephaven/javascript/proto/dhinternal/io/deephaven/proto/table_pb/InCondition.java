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
    name = "dhinternal.io.deephaven.proto.table_pb.InCondition",
    namespace = JsPackage.GLOBAL)
public class InCondition {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TargetFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface LiteralFieldType {
        @JsOverlay
        static InCondition.ToObjectReturnType.TargetFieldType.LiteralFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getDoublevalue();

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
        void setDoublevalue(double doublevalue);

        @JsProperty
        void setLongvalue(double longvalue);

        @JsProperty
        void setNanotimevalue(double nanotimevalue);

        @JsProperty
        void setStringvalue(String stringvalue);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ReferenceFieldType {
        @JsOverlay
        static InCondition.ToObjectReturnType.TargetFieldType.ReferenceFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnname();

        @JsProperty
        void setColumnname(String columnname);
      }

      @JsOverlay
      static InCondition.ToObjectReturnType.TargetFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      InCondition.ToObjectReturnType.TargetFieldType.LiteralFieldType getLiteral();

      @JsProperty
      InCondition.ToObjectReturnType.TargetFieldType.ReferenceFieldType getReference();

      @JsProperty
      void setLiteral(InCondition.ToObjectReturnType.TargetFieldType.LiteralFieldType literal);

      @JsProperty
      void setReference(
          InCondition.ToObjectReturnType.TargetFieldType.ReferenceFieldType reference);
    }

    @JsOverlay
    static InCondition.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<Object> getCandidatesList();

    @JsProperty
    double getCasesensitivity();

    @JsProperty
    double getMatchtype();

    @JsProperty
    InCondition.ToObjectReturnType.TargetFieldType getTarget();

    @JsProperty
    void setCandidatesList(JsArray<Object> candidatesList);

    @JsOverlay
    default void setCandidatesList(Object[] candidatesList) {
      setCandidatesList(Js.<JsArray<Object>>uncheckedCast(candidatesList));
    }

    @JsProperty
    void setCasesensitivity(double casesensitivity);

    @JsProperty
    void setMatchtype(double matchtype);

    @JsProperty
    void setTarget(InCondition.ToObjectReturnType.TargetFieldType target);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface TargetFieldType {
      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface LiteralFieldType {
        @JsOverlay
        static InCondition.ToObjectReturnType0.TargetFieldType.LiteralFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        double getDoublevalue();

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
        void setDoublevalue(double doublevalue);

        @JsProperty
        void setLongvalue(double longvalue);

        @JsProperty
        void setNanotimevalue(double nanotimevalue);

        @JsProperty
        void setStringvalue(String stringvalue);
      }

      @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
      public interface ReferenceFieldType {
        @JsOverlay
        static InCondition.ToObjectReturnType0.TargetFieldType.ReferenceFieldType create() {
          return Js.uncheckedCast(JsPropertyMap.of());
        }

        @JsProperty
        String getColumnname();

        @JsProperty
        void setColumnname(String columnname);
      }

      @JsOverlay
      static InCondition.ToObjectReturnType0.TargetFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      InCondition.ToObjectReturnType0.TargetFieldType.LiteralFieldType getLiteral();

      @JsProperty
      InCondition.ToObjectReturnType0.TargetFieldType.ReferenceFieldType getReference();

      @JsProperty
      void setLiteral(InCondition.ToObjectReturnType0.TargetFieldType.LiteralFieldType literal);

      @JsProperty
      void setReference(
          InCondition.ToObjectReturnType0.TargetFieldType.ReferenceFieldType reference);
    }

    @JsOverlay
    static InCondition.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    JsArray<Object> getCandidatesList();

    @JsProperty
    double getCasesensitivity();

    @JsProperty
    double getMatchtype();

    @JsProperty
    InCondition.ToObjectReturnType0.TargetFieldType getTarget();

    @JsProperty
    void setCandidatesList(JsArray<Object> candidatesList);

    @JsOverlay
    default void setCandidatesList(Object[] candidatesList) {
      setCandidatesList(Js.<JsArray<Object>>uncheckedCast(candidatesList));
    }

    @JsProperty
    void setCasesensitivity(double casesensitivity);

    @JsProperty
    void setMatchtype(double matchtype);

    @JsProperty
    void setTarget(InCondition.ToObjectReturnType0.TargetFieldType target);
  }

  public static native InCondition deserializeBinary(Uint8Array bytes);

  public static native InCondition deserializeBinaryFromReader(InCondition message, Object reader);

  public static native void serializeBinaryToWriter(InCondition message, Object writer);

  public static native InCondition.ToObjectReturnType toObject(
      boolean includeInstance, InCondition msg);

  public native Value addCandidates();

  public native Value addCandidates(Value value, double index);

  public native Value addCandidates(Value value);

  public native void clearCandidatesList();

  public native void clearTarget();

  public native JsArray<Value> getCandidatesList();

  public native double getCasesensitivity();

  public native double getMatchtype();

  public native Value getTarget();

  public native boolean hasTarget();

  public native Uint8Array serializeBinary();

  public native void setCandidatesList(JsArray<Value> value);

  @JsOverlay
  public final void setCandidatesList(Value[] value) {
    setCandidatesList(Js.<JsArray<Value>>uncheckedCast(value));
  }

  public native void setCasesensitivity(double value);

  public native void setMatchtype(double value);

  public native void setTarget();

  public native void setTarget(Value value);

  public native InCondition.ToObjectReturnType0 toObject();

  public native InCondition.ToObjectReturnType0 toObject(boolean includeInstance);
}
