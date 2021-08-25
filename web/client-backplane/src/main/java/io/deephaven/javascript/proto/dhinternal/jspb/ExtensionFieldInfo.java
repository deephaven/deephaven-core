package io.deephaven.javascript.proto.dhinternal.jspb;

import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;
import jsinterop.base.JsPropertyMap;

@JsType(isNative = true, name = "dhinternal.jspb.ExtensionFieldInfo", namespace = JsPackage.GLOBAL)
public class ExtensionFieldInfo<T> {
    @JsFunction
    public interface ExtensionFieldInfoToObjectFn {
        Object onInvoke(boolean p0, Message p1);
    }

    @JsFunction
    public interface ToObjectFn {
        Object onInvoke(boolean p0, Message p1);
    }

    public Object ctor;
    public double fieldIndex;
    public double fieldName;
    public double isRepeated;
    public ExtensionFieldInfo.ToObjectFn toObjectFn;

    public ExtensionFieldInfo(
            double fieldIndex,
            JsPropertyMap<Double> fieldName,
            Object ctor,
            ExtensionFieldInfo.ExtensionFieldInfoToObjectFn toObjectFn,
            double isRepeated) {}

    public native boolean isMessageType();
}
