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
    name = "dhinternal.io.deephaven.proto.table_pb.MatchesCondition",
    namespace = JsPackage.GLOBAL)
public class MatchesCondition {
  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ReferenceFieldType {
      @JsOverlay
      static MatchesCondition.ToObjectReturnType.ReferenceFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getColumnname();

      @JsProperty
      void setColumnname(String columnname);
    }

    @JsOverlay
    static MatchesCondition.ToObjectReturnType create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    double getCasesensitivity();

    @JsProperty
    double getMatchtype();

    @JsProperty
    MatchesCondition.ToObjectReturnType.ReferenceFieldType getReference();

    @JsProperty
    String getRegex();

    @JsProperty
    void setCasesensitivity(double casesensitivity);

    @JsProperty
    void setMatchtype(double matchtype);

    @JsProperty
    void setReference(MatchesCondition.ToObjectReturnType.ReferenceFieldType reference);

    @JsProperty
    void setRegex(String regex);
  }

  @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
  public interface ToObjectReturnType0 {
    @JsType(isNative = true, name = "?", namespace = JsPackage.GLOBAL)
    public interface ReferenceFieldType {
      @JsOverlay
      static MatchesCondition.ToObjectReturnType0.ReferenceFieldType create() {
        return Js.uncheckedCast(JsPropertyMap.of());
      }

      @JsProperty
      String getColumnname();

      @JsProperty
      void setColumnname(String columnname);
    }

    @JsOverlay
    static MatchesCondition.ToObjectReturnType0 create() {
      return Js.uncheckedCast(JsPropertyMap.of());
    }

    @JsProperty
    double getCasesensitivity();

    @JsProperty
    double getMatchtype();

    @JsProperty
    MatchesCondition.ToObjectReturnType0.ReferenceFieldType getReference();

    @JsProperty
    String getRegex();

    @JsProperty
    void setCasesensitivity(double casesensitivity);

    @JsProperty
    void setMatchtype(double matchtype);

    @JsProperty
    void setReference(MatchesCondition.ToObjectReturnType0.ReferenceFieldType reference);

    @JsProperty
    void setRegex(String regex);
  }

  public static native MatchesCondition deserializeBinary(Uint8Array bytes);

  public static native MatchesCondition deserializeBinaryFromReader(
      MatchesCondition message, Object reader);

  public static native void serializeBinaryToWriter(MatchesCondition message, Object writer);

  public static native MatchesCondition.ToObjectReturnType toObject(
      boolean includeInstance, MatchesCondition msg);

  public native void clearReference();

  public native double getCasesensitivity();

  public native double getMatchtype();

  public native Reference getReference();

  public native String getRegex();

  public native boolean hasReference();

  public native Uint8Array serializeBinary();

  public native void setCasesensitivity(double value);

  public native void setMatchtype(double value);

  public native void setReference();

  public native void setReference(Reference value);

  public native void setRegex(String value);

  public native MatchesCondition.ToObjectReturnType0 toObject();

  public native MatchesCondition.ToObjectReturnType0 toObject(boolean includeInstance);
}
