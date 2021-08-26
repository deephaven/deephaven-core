package io.deephaven.javascript.proto.dhinternal.jspb;

import jsinterop.annotations.JsFunction;
import jsinterop.annotations.JsPackage;
import jsinterop.annotations.JsType;

@JsType(
        isNative = true,
        name = "dhinternal.jspb.ExtensionFieldBinaryInfo",
        namespace = JsPackage.GLOBAL)
public class ExtensionFieldBinaryInfo<T> {
    @JsFunction
    public interface BinaryReaderFn {
        @JsFunction
        public interface P1Fn {
            void onInvoke(Object p0, BinaryReader p1);
        }

        void onInvoke(Object p0, ExtensionFieldBinaryInfo.BinaryReaderFn.P1Fn p1);
    }

    @JsFunction
    public interface BinaryWriterFn {
        @JsFunction
        public interface P2Fn {
            void onInvoke(Object p0, BinaryWriter p1);
        }

        void onInvoke(double p0, Object p1, ExtensionFieldBinaryInfo.BinaryWriterFn.P2Fn p2);
    }

    @JsFunction
    public interface ExtensionFieldBinaryInfoBinaryMessageDeserializeFn {
        Message onInvoke(Message p0, BinaryReader p1);
    }

    @JsFunction
    public interface ExtensionFieldBinaryInfoBinaryMessageSerializeFn {
        void onInvoke(Message p0, BinaryWriter p1);
    }

    @JsFunction
    public interface ExtensionFieldBinaryInfoBinaryReaderFn {
        @JsFunction
        public interface P1Fn {
            void onInvoke(Object p0, BinaryReader p1);
        }

        void onInvoke(
                Object p0, ExtensionFieldBinaryInfo.ExtensionFieldBinaryInfoBinaryReaderFn.P1Fn p1);
    }

    @JsFunction
    public interface ExtensionFieldBinaryInfoBinaryWriterFn {
        @JsFunction
        public interface P2Fn {
            void onInvoke(Object p0, BinaryWriter p1);
        }

        void onInvoke(
                double p0,
                Object p1,
                ExtensionFieldBinaryInfo.ExtensionFieldBinaryInfoBinaryWriterFn.P2Fn p2);
    }

    @JsFunction
    public interface Opt_binaryMessageDeserializeFn {
        Message onInvoke(Message p0, BinaryReader p1);
    }

    @JsFunction
    public interface Opt_binaryMessageSerializeFn {
        void onInvoke(Message p0, BinaryWriter p1);
    }

    public ExtensionFieldBinaryInfo.BinaryReaderFn binaryReaderFn;
    public ExtensionFieldBinaryInfo.BinaryWriterFn binaryWriterFn;
    public ExtensionFieldInfo<T> fieldInfo;
    public ExtensionFieldBinaryInfo.Opt_binaryMessageDeserializeFn opt_binaryMessageDeserializeFn;
    public ExtensionFieldBinaryInfo.Opt_binaryMessageSerializeFn opt_binaryMessageSerializeFn;
    public boolean opt_isPacked;

    public ExtensionFieldBinaryInfo(
            ExtensionFieldInfo<T> fieldInfo,
            ExtensionFieldBinaryInfo.ExtensionFieldBinaryInfoBinaryReaderFn binaryReaderFn,
            ExtensionFieldBinaryInfo.ExtensionFieldBinaryInfoBinaryWriterFn binaryWriterFn,
            ExtensionFieldBinaryInfo.ExtensionFieldBinaryInfoBinaryMessageSerializeFn binaryMessageSerializeFn,
            ExtensionFieldBinaryInfo.ExtensionFieldBinaryInfoBinaryMessageDeserializeFn binaryMessageDeserializeFn,
            boolean isPacked) {}
}
